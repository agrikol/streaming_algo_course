//	[Секция данных]   — записи key/opType/value идут подряд
//	[Секция индекса]  — sparse index: ключ → byte offset в секции данных
//	[Footer]          — 16 байт: indexOffset (8) + magic (8)
//
// Формат одной записи данных:

// [4 байта uint32 LE] keyLen
// [keyLen байт]       key
// [1 байт]            opType  (1=Put, 2=Delete/tombstone)
// [4 байта uint32 LE] valLen  (0 для tombstone)
// [valLen байт]       value
package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
)

// ErrNotImplemented оставлен для совместимости с заготовкой.
var ErrNotImplemented = errors.New("sstable: функция не реализована")

// и количеством записей, которые нужно пропустить при поиске.
const indexStride = 16

const magic uint64 = 0x53535441424c4531

const (
	opPut    byte = 1 // ключ существует
	opDelete byte = 2 // tombstone
)

// indexEntry — одна запись в sparse index.
// Хранит первый ключ блока и его byte offset в секции данных.
type indexEntry struct {
	key    []byte // первый ключ этого блока записей
	offset int64  // byte offset начала блока в секции данных
}

// Writer записывает отсортированные пары key/value в SSTable файл.
type Writer struct {
	w           *bufio.Writer // буферизованный writer
	curOffset   int64         // текущий логический byte offset в файле (отслеживаем вручную)
	index       []indexEntry  // накапливаемый sparse index
	recordCount int           // сколько записей уже добавлено
}

// NewWriter создаёт Writer, который пишет SSTable в переданный io.Writer.
// В качестве w обычно передаётся созданный os.File.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w)}
}

// Add добавляет пару ключ/значение (операция Put).
func (w *Writer) Add(key, value []byte) error {
	return w.addRecord(key, value, opPut)
}

// маркер удаления
func (w *Writer) AddTombstone(key []byte) error {
	return w.addRecord(key, nil, opDelete)
}

// addRecord — внутренний метод, записывает одну запись в секцию данных.
// Если это начало нового блока (каждые indexStride записей) — добавляет
// запись в sparse index с текущим byte offset.
func (w *Writer) addRecord(key, value []byte, op byte) error {
	// Каждые indexStride записей добавляем точку в sparse index.
	// recordCount % indexStride == 0 означает: это первая запись нового блока.
	if w.recordCount%indexStride == 0 {

		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		w.index = append(w.index, indexEntry{key: keyCopy, offset: w.curOffset})
	}

	keyLen := uint32(len(key))
	valLen := uint32(len(value))

	// Вспомогательный буфер для 4-байтных чисел
	var buf [4]byte

	// Пишем keyLen (4 байта, little-endian)
	binary.LittleEndian.PutUint32(buf[:], keyLen)
	if _, err := w.w.Write(buf[:]); err != nil {
		return fmt.Errorf("sstable: запись keyLen: %w", err)
	}

	// Пишем key
	if len(key) > 0 {
		if _, err := w.w.Write(key); err != nil {
			return fmt.Errorf("sstable: запись key: %w", err)
		}
	}

	// Пишем тип операции (1 байт): Put=1 или Delete=2
	if err := w.w.WriteByte(op); err != nil {
		return fmt.Errorf("sstable: запись opType: %w", err)
	}

	// Пишем valLen (4 байта). Для tombstone valLen=0.
	binary.LittleEndian.PutUint32(buf[:], valLen)
	if _, err := w.w.Write(buf[:]); err != nil {
		return fmt.Errorf("sstable: запись valLen: %w", err)
	}

	// Пишем value. Для tombstone пропускаем (valLen=0).
	if len(value) > 0 {
		if _, err := w.w.Write(value); err != nil {
			return fmt.Errorf("sstable: запись value: %w", err)
		}
	}

	// Обновляем логический offset: 4(keyLen) + keyLen + 1(opType) + 4(valLen) + valLen
	w.curOffset += 4 + int64(keyLen) + 1 + 4 + int64(valLen)
	w.recordCount++
	return nil
}

// Close завершает запись SSTable:
//  1. Запоминает текущий offset — это начало секции индекса.
//  2. Записывает все накопленные записи sparse index.
//  3. Записывает footer (indexOffset + magic).
//  4. Сбрасывает буфер в файл.
func (w *Writer) Close() error {
	// Запоминаем, с какого байта начинается секция индекса.
	// Всё что записано ДО этой точки — это секция данных.
	indexOffset := w.curOffset

	var buf8 [8]byte
	var buf4 [4]byte

	// Записываем все записи sparse index: (keyLen, key, offset)
	for _, entry := range w.index {
		// keyLen (4 байта) + key
		binary.LittleEndian.PutUint32(buf4[:], uint32(len(entry.key)))
		if _, err := w.w.Write(buf4[:]); err != nil {
			return fmt.Errorf("sstable: запись index keyLen: %w", err)
		}
		if _, err := w.w.Write(entry.key); err != nil {
			return fmt.Errorf("sstable: запись index key: %w", err)
		}
		// offset в секции данных (8 байт, little-endian)
		binary.LittleEndian.PutUint64(buf8[:], uint64(entry.offset))
		if _, err := w.w.Write(buf8[:]); err != nil {
			return fmt.Errorf("sstable: запись index offset: %w", err)
		}
	}

	// Записываем footer — последние 16 байт файла:
	// [8 байт: indexOffset] [8 байт: magic]
	binary.LittleEndian.PutUint64(buf8[:], uint64(indexOffset))
	if _, err := w.w.Write(buf8[:]); err != nil {
		return fmt.Errorf("sstable: запись footer indexOffset: %w", err)
	}
	binary.LittleEndian.PutUint64(buf8[:], magic)
	if _, err := w.w.Write(buf8[:]); err != nil {
		return fmt.Errorf("sstable: запись footer magic: %w", err)
	}

	// Сбрасываем буфер — всё попадает в файл
	return w.w.Flush()
}

// Reader читает SSTable файл с произвольным доступом (io.ReaderAt).
// При создании загружает footer и sparse index в память — они маленькие,
// поэтому это дёшево.
type Reader struct {
	r       io.ReaderAt  // файл с поддержкой произвольного доступа
	size    int64        // размер файла в байтах
	dataEnd int64        // byte offset конца секции данных (= indexOffset)
	index   []indexEntry // загруженный sparse index
	loadErr error        // ошибка при загрузке (если файл повреждён)
}

// NewReader открывает SSTable для чтения.
// Сразу загружает footer и sparse index в память.
//
//   - r   — файл с поддержкой произвольного доступа (например, *os.File)
//   - size — размер файла в байтах (получается через os.File.Stat().Size())
func NewReader(r io.ReaderAt, size int64) *Reader {
	reader := &Reader{r: r, size: size}
	reader.loadErr = reader.loadIndex()
	return reader
}

// loadIndex читает footer и sparse index в память.
// Алгоритм:
//  1. Читаем последние 16 байт (footer): indexOffset и magic.
//  2. Проверяем magic — защита от повреждённых файлов.
//  3. Читаем секцию индекса целиком (от indexOffset до footer).
//  4. Разбираем записи индекса и сохраняем в r.index. /?
func (r *Reader) loadIndex() error {
	// Файл должен быть не меньше 16 байт (минимум: footer без данных и индекса)
	if r.size < 16 {
		return fmt.Errorf("sstable: файл слишком мал (%d байт, минимум 16)", r.size)
	}

	// Читаем footer из последних 16 байт файла
	var footerBuf [16]byte
	if _, err := r.r.ReadAt(footerBuf[:], r.size-16); err != nil {
		return fmt.Errorf("sstable: чтение footer: %w", err)
	}

	indexOffset := int64(binary.LittleEndian.Uint64(footerBuf[:8]))
	fileMagic := binary.LittleEndian.Uint64(footerBuf[8:])

	if fileMagic != magic {
		return fmt.Errorf("sstable: неверная magic-константа: %016x (ожидалось %016x)", fileMagic, magic)
	}

	// граница между секцией данных и секцией индекса
	r.dataEnd = indexOffset

	// размер секции индекса: от indexOffset до начала footer
	indexSize := r.size - 16 - indexOffset
	if indexSize < 0 {
		return fmt.Errorf("sstable: некорректный indexOffset=%d при размере файла=%d", indexOffset, r.size)
	}
	if indexSize == 0 {
		return nil // пустой SSTable — нет записей, нет индекса
	}

	// Читаем всю секцию индекса одним куском
	indexBuf := make([]byte, indexSize)
	if _, err := r.r.ReadAt(indexBuf, indexOffset); err != nil {
		return fmt.Errorf("sstable: чтение секции индекса: %w", err)
	}

	// Разбираем записи индекса: каждая запись = keyLen(4) + key(keyLen) + offset(8) /?
	pos := 0
	for pos < len(indexBuf) {
		if pos+4 > len(indexBuf) {
			break // неполная запись — останавливаемся
		}
		keyLen := int(binary.LittleEndian.Uint32(indexBuf[pos:]))
		pos += 4

		if pos+keyLen+8 > len(indexBuf) {
			break // неполная запись
		}
		key := make([]byte, keyLen)
		copy(key, indexBuf[pos:])
		pos += keyLen

		offset := int64(binary.LittleEndian.Uint64(indexBuf[pos:]))
		pos += 8

		r.index = append(r.index, indexEntry{key: key, offset: offset})
	}

	return nil
}

// Iterator создаёт итератор по ключам в диапазоне [start, end).
//
// Алгоритм поиска стартовой позиции (используем sparse index):
//  1. Ищем в индексе последнюю запись с key ≤ start.
//  2. Начинаем читать с offset этой записи.
//  3. Первые несколько записей с key < start итератор пропустит сам.
//
// Если start == nil — начинаем с начала файла.
// Если end == nil — читаем до конца файла.
func (r *Reader) Iterator(start, end []byte) (*Iter, error) {
	if r.loadErr != nil {
		return nil, fmt.Errorf("sstable: Reader не загружен: %w", r.loadErr)
	}

	// По умолчанию начинаем с начала секции данных
	startOffset := int64(0)

	if start != nil && len(r.index) > 0 { // /?
		// sort.Search(n, f) возвращает наименьший i в [0,n), для которого f(i)=true.
		// Мы ищем первый i, где index[i].key > start.
		// Значит нам нужен блок i-1 (последний блок, чей первый ключ ≤ start).
		i := sort.Search(len(r.index), func(i int) bool {
			return bytes.Compare(r.index[i].key, start) > 0
		})
		if i > 0 {
			// Блок i-1 начинается с ключа ≤ start, поэтому start точно
			// находится в этом блоке или в следующем — начинаем отсюда.
			startOffset = r.index[i-1].offset
		}
		// Если i == 0: все индексные ключи > start → начинаем с самого начала (offset=0)
	}

	return &Iter{
		r:       r.r,
		offset:  startOffset,
		dataEnd: r.dataEnd,
		start:   start,
		end:     end,
	}, nil
}

// Iter последовательно читает записи из SSTable начиная с заданного offset.
// Поддерживает диапазонные запросы [start, end).
type Iter struct {
	r       io.ReaderAt // файл с произвольным доступом
	offset  int64       // текущий byte offset чтения в файле
	dataEnd int64       // граница секции данных — дальше читать нельзя
	start   []byte      // нижняя граница диапазона (включительно)
	end     []byte      // верхняя граница диапазона (исключительно)
	seeked  bool        // флаг: уже нашли первый ключ >= start?
	done    bool        // флаг: итерация завершена?
}

// Next возвращает следующую пару ключ/значение из SSTable.
//
// Особые случаи:
//   - tombstone запись: возвращает (key, nil, true, nil) — nil сигнализирует LSM слою об удалении
//   - конец диапазона/файла: возвращает (nil, nil, false, nil)
func (it *Iter) Next() (key, value []byte, ok bool, err error) {
	for {
		// Проверяем, не вышли ли за границы секции данных
		if it.done || it.offset >= it.dataEnd {
			return nil, nil, false, nil
		}

		// Читаем одну запись из файла, получаем (key, value, opType, размер записи в байтах)
		k, v, op, n, readErr := it.readRecord()
		if readErr != nil {
			return nil, nil, false, fmt.Errorf("sstable iter: чтение записи: %w", readErr)
		}
		if n == 0 {
			// Достигли конца данных (ReadAt вернул EOF или insufficient data)
			it.done = true
			return nil, nil, false, nil
		}
		it.offset += int64(n) // продвигаем позицию на размер прочитанной записи

		// Фаза поиска: пропускаем записи с ключом < start.
		// Это нужно потому что sparse index указывает на начало блока,
		// который может содержать ключи, меньшие чем start.
		if !it.seeked {
			if it.start != nil && bytes.Compare(k, it.start) < 0 {
				continue // ключ меньше start — пропускаем, ищем дальше
			}
			it.seeked = true // нашли первый ключ >= start
		}

		// Проверяем верхнюю границу диапазона [start, end)
		// Если ключ >= end — останавливаемся (end — исключительная граница)
		if it.end != nil && bytes.Compare(k, it.end) >= 0 {
			it.done = true
			return nil, nil, false, nil
		}

		// Возвращаем запись.
		// Для tombstone (opDelete) возвращаем value=nil — это сигнал для LSM слоя,
		// что ключ был удалён. LSM слой скроет его от пользователя.
		if op == opDelete {
			return k, nil, true, nil
		}
		return k, v, true, nil
	}
}

// readRecord читает одну запись из io.ReaderAt начиная с it.offset.
// Использует ReadAt (произвольный доступ) — не изменяет позицию файла.
//
// Возвращает (key, value, opType, totalBytesRead, error).
// Если totalBytesRead == 0 — данных больше нет.
func (it *Iter) readRecord() (key, value []byte, op byte, n int, err error) {
	// Защита: если не хватает байт даже для keyLen — данные закончились
	if it.offset+4 > it.dataEnd {
		return nil, nil, 0, 0, nil
	}

	// Читаем keyLen (4 байта)
	var buf4 [4]byte
	m, readErr := it.r.ReadAt(buf4[:], it.offset)
	if readErr != nil || m < 4 {
		if readErr == io.EOF {
			return nil, nil, 0, 0, nil // нормальный конец файла
		}
		if readErr != nil {
			return nil, nil, 0, 0, readErr
		}
		return nil, nil, 0, 0, nil
	}
	keyLen := int(binary.LittleEndian.Uint32(buf4[:]))
	pos := it.offset + 4

	// Читаем key
	key = make([]byte, keyLen)
	if keyLen > 0 {
		if _, readErr = it.r.ReadAt(key, pos); readErr != nil {
			return nil, nil, 0, 0, fmt.Errorf("чтение key: %w", readErr)
		}
	}
	pos += int64(keyLen)

	// Читаем opType (1 байт)
	var opBuf [1]byte
	if _, readErr = it.r.ReadAt(opBuf[:], pos); readErr != nil {
		return nil, nil, 0, 0, fmt.Errorf("чтение opType: %w", readErr)
	}
	op = opBuf[0]
	pos++

	// Читаем valLen (4 байта)
	if _, readErr = it.r.ReadAt(buf4[:], pos); readErr != nil {
		return nil, nil, 0, 0, fmt.Errorf("чтение valLen: %w", readErr)
	}
	valLen := int(binary.LittleEndian.Uint32(buf4[:]))
	pos += 4

	// Читаем value
	value = make([]byte, valLen)
	if valLen > 0 {
		if _, readErr = it.r.ReadAt(value, pos); readErr != nil {
			return nil, nil, 0, 0, fmt.Errorf("чтение value: %w", readErr)
		}
	}

	// Суммарный размер записи: 4(keyLen) + keyLen + 1(opType) + 4(valLen) + valLen
	totalSize := 4 + keyLen + 1 + 4 + valLen
	return key, value, op, totalSize, nil
}

// Close завершает итерацию. Повторный вызов безопасен.
func (it *Iter) Close() error {
	it.done = true
	return nil
}
