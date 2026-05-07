// lsm - tree
// чтение:
//	Put(key, val) → WAL.Append → Memtable.Put → [если переполнена: flush → SSTable]
//
// запись:
//	Get(key) → Memtable → SSTable[новый] → SSTable[старый] → ErrNotFound

package lsm

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"kvschool/internal/skiplist"
	"kvschool/internal/sstable"
	"kvschool/internal/wal"
)

var ErrNotFound = errors.New("lsm: ключ не найден")

type Iterator interface {
	Next() (key, value []byte, ok bool, err error)
	Close() error
}

// Тэги для хранения значений в Memtable.
const (
	tagPut    byte = 1 // запись типа Put: тэг + значение
	tagDelete byte = 2 // tombstone: только тэг, значения нет
)

const (
	walFileName = "wal.log"      // файл Write-Ahead Log
	sstableFmt  = "sst-%06d.sst" // шаблон имени SSTable
	sstableGlob = "sst-*.sst"    // glob-паттерн для поиска
)

const (
	defaultFlushThreshold = 4 * 1024 * 1024 // порог flush

	compactionThreshold = 4 // порог SSTable файлов
)

// sstableHandle объединяет путь к файлу, его порядковый номер и открытый Reader.
// Чем больше seq — тем новее файл (новый файл всегда "важнее" старого).
type sstableHandle struct {
	path   string          // полный путь к файлу на диске
	seq    int             // порядковый номер (больше = новее)
	file   *os.File        // открытый файловый дескриптор
	reader *sstable.Reader // Reader для этого файла
}

// Options задаёт параметры LSM движка.
type Options struct {
	Dir string // директория для хранения WAL и SSTable файлов

	// MemtableFlushThreshold — максимальный приблизительный размер Memtable в байтах
	// перед сбросом на диск. 0 = использовать дефолтное значение (4MB).
	MemtableFlushThreshold int
}

// Engine — основной движок CDR Storage.
// Координирует работу Memtable, WAL и SSTables, обеспечивает
// надёжное хранение данных с crash recovery.
type Engine struct {
	opts Options

	// Memtable — отсортированная структура в памяти (SkipList из Дня 1).
	// Хранит самые свежие данные. Значения в Memtable хранятся с тэгом (tagPut/tagDelete).
	memtable *skiplist.SkipList
	memBytes int // приблизительный суммарный размер данных в Memtable в байтах

	// WAL файл и writer для записи операций перед применением к Memtable
	walFile *os.File
	wal     *wal.Writer

	// SSTables на диске: отсортированы от старейшего (индекс 0) к новейшему (последний).
	// При поиске просматриваем с конца (от нового к старому) — новое имеет приоритет.
	sstables []*sstableHandle

	nextSeq int // порядковый номер следующего создаваемого SSTable файла
}

// Open открывает или создаёт LSM движок в указанной директории.
//
// При наличии WAL файла — воспроизводит его (Crash Recovery):
// читает все записанные операции и применяет их к Memtable.
// Это восстанавливает состояние, которое было в момент краша.
func Open(opts Options) (*Engine, error) {
	if opts.MemtableFlushThreshold == 0 {
		opts.MemtableFlushThreshold = defaultFlushThreshold
	}

	// Создаём директорию для файлов, если не существует.
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil { // /?
		return nil, fmt.Errorf("lsm: создание директории %q: %w", opts.Dir, err)
	}

	e := &Engine{
		opts:     opts,
		memtable: skiplist.New(1), // seed=1 для воспроизводимости
		nextSeq:  1,
	}

	// Загружаем существующие SSTable файлы с диска
	if err := e.loadSSTables(); err != nil {
		return nil, err
	}

	// Воспроизводим WAL (если существует) — восстанавливаем Memtable после краша
	if err := e.replayWAL(); err != nil {
		return nil, err
	}

	// Открываем WAL для записи новых операций.
	// O_APPEND — все записи добавляются в конец (ключевое для sequential write).
	walPath := filepath.Join(opts.Dir, walFileName)
	walFile, err := os.OpenFile(walPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644) // /?
	if err != nil {
		return nil, fmt.Errorf("lsm: открытие WAL для записи: %w", err)
	}
	e.walFile = walFile
	e.wal = wal.NewWriter(walFile)

	return e, nil
}

// loadSSTables находит все SSTable файлы в директории, сортирует их
// по имени (= по возрасту), открывает каждый и создаёт Reader.
func (e *Engine) loadSSTables() error {
	// Ищем файлы по паттерну "sst-*.sst" в директории движка
	pattern := filepath.Join(e.opts.Dir, sstableGlob)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("lsm: поиск SSTable файлов: %w", err)
	}

	// Сортируем по имени
	sort.Strings(matches)

	for _, path := range matches {
		// порядковый номер
		seq, parseErr := parseSSTSeq(filepath.Base(path))
		if parseErr != nil {
			continue // пропуск файлы с неправильным именем
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("lsm: открытие SSTable %q: %w", path, err)
		}

		// Размер файла нужен чтобы знать, где footer
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return fmt.Errorf("lsm: stat SSTable %q: %w", path, err)
		}

		reader := sstable.NewReader(f, info.Size())
		e.sstables = append(e.sstables, &sstableHandle{
			path:   path,
			seq:    seq,
			file:   f,
			reader: reader,
		})

		// Следующий SSTable должен иметь номер больше максимального существующего
		if seq >= e.nextSeq {
			e.nextSeq = seq + 1
		}
	}
	return nil
}

// parseSSTSeq извлекает порядковый номер из имени файла.
func parseSSTSeq(name string) (int, error) {
	name = strings.TrimPrefix(name, "sst-")
	name = strings.TrimSuffix(name, ".sst")
	return strconv.Atoi(name)
}

// replayWAL читает WAL файл и воспроизводит все записи в Memtable.
// Вызывается при старте, до открытия WAL на запись.
func (e *Engine) replayWAL() error {
	walPath := filepath.Join(e.opts.Dir, walFileName) // /?

	f, err := os.Open(walPath)
	if os.IsNotExist(err) {
		return nil // WAL не существует — первый старт
	}
	if err != nil {
		return fmt.Errorf("lsm: открытие WAL для воспроизведения: %w", err)
	}
	defer f.Close()

	r := wal.NewReader(f)
	for {
		rec, ok, err := r.Next()
		if err != nil {
			return fmt.Errorf("lsm: чтение WAL при воспроизведении: %w", err)
		}
		if !ok {
			break // достигли конца WAL (или обрезанной хвостовой записи при краше)
		}

		// Воспроизводим операцию в Memtable
		switch rec.Type {
		case wal.OpPut:
			if err := e.memPut(rec.Key, rec.Value); err != nil {
				return fmt.Errorf("lsm: replay Put(%q): %w", rec.Key, err)
			}
		case wal.OpDelete:
			if err := e.memDelete(rec.Key); err != nil {
				return fmt.Errorf("lsm: replay Delete(%q): %w", rec.Key, err)
			}
		}
	}
	return nil
}

// Close завершает работу движка.
// Если в Memtable есть данные — сбрасывает их в SSTable (flush).
// Закрывает WAL и все SSTable файлы.
func (e *Engine) Close() error {
	// Если Memtable непустая — сбрасываем на диск.
	if e.memBytes > 0 {
		if err := e.flush(); err != nil {
			return fmt.Errorf("lsm: flush при Close: %w", err)
		}
	}

	// Закрываем WAL writer и файл.
	e.wal.Close()
	e.walFile.Close()
	walPath := filepath.Join(e.opts.Dir, walFileName)
	os.Remove(walPath) // удаляем пустой WAL
	// Закрываем все открытые SSTable файловые дескрипторы
	for _, h := range e.sstables {
		h.file.Close()
	}
	return nil
}

// memPut сохраняет значение в Memtable с тэгом Put.
func (e *Engine) memPut(key, value []byte) error {
	tagged := make([]byte, 1+len(value))
	tagged[0] = tagPut
	copy(tagged[1:], value)

	if err := e.memtable.Put(key, tagged); err != nil {
		return err
	}
	// Обновляем счётчик размера: ключ + значение + 1 байт тэга
	e.memBytes += len(key) + len(value) + 1
	return nil
}

// memDelete записывает tombstone в Memtable.
func (e *Engine) memDelete(key []byte) error {
	return e.memtable.Put(key, []byte{tagDelete})
}

func (e *Engine) memGet(key []byte) (value []byte, found bool, isTombstone bool, err error) {
	tagged, err := e.memtable.Get(key)
	if err == skiplist.ErrNotFound {
		return nil, false, false, nil // ключ не найден
	}
	if err != nil {
		return nil, false, false, err // ошибка
	}

	if len(tagged) == 0 {
		return nil, true, false, nil // тэг отсутствует
	}

	if tagged[0] == tagDelete {
		return nil, true, true, nil // tombstone
	}

	return tagged[1:], true, false, nil // значение
}

// CRUDs

func (e *Engine) Put(key, value []byte) error {
	// записываем в WAL ПЕРВЫМ
	if err := e.wal.Append(wal.Record{Type: wal.OpPut, Key: key, Value: value}); err != nil {
		return fmt.Errorf("lsm: WAL Put: %w", err)
	}

	// применяем к Memtable
	if err := e.memPut(key, value); err != nil {
		return fmt.Errorf("lsm: Memtable Put: %w", err)
	}

	// если превысила порог — сбрасываем на диск
	if e.memBytes >= e.opts.MemtableFlushThreshold {
		return e.flush()
	}
	return nil
}

// Delete помечает ключ как tombstone
func (e *Engine) Delete(key []byte) error {
	// Записываем tombstone в WAL (Delete — без значения)
	if err := e.wal.Append(wal.Record{Type: wal.OpDelete, Key: key}); err != nil {
		return fmt.Errorf("lsm: WAL Delete: %w", err)
	}
	// Записываем tombstone в Memtable
	return e.memDelete(key)
}

// Порядок поиска:
//  1. Memtable — самые свежие данные
//  2. SSTables от новейшего к старейшему (новейший имеет приоритет)
//
// Если где-то встречается tombstone — возвращаем ErrNotFound.
func (e *Engine) Get(key []byte) ([]byte, error) {
	// ищем в Memtable
	val, found, isTombstone, err := e.memGet(key)
	if err != nil {
		return nil, err
	}
	if found {
		if isTombstone {
			return nil, ErrNotFound // ключ был удалён
		}
		return val, nil
	}

	// Просматриваем e.sstables с конца (последний = самый новый).
	for i := len(e.sstables) - 1; i >= 0; i-- {
		// Создаём итератор, начиная с нашего ключа.
		// end=nil означает "без верхней границы" — итератор выдаст всё с key и дальше,
		// но мы берём только первый результат и проверяем совпадение ключа.
		it, err := e.sstables[i].reader.Iterator(key, nil)
		if err != nil {
			return nil, fmt.Errorf("lsm: Iterator для SSTable[%d]: %w", i, err)
		}

		k, v, ok, err := it.Next()
		it.Close()

		if err != nil {
			return nil, fmt.Errorf("lsm: чтение из SSTable[%d]: %w", i, err)
		}
		if !ok || !bytes.Equal(k, key) {
			continue // ключ не найден в этом SSTable
		}
		if v == nil {
			return nil, ErrNotFound // tombstone в SSTable
		}
		return v, nil
	}

	return nil, ErrNotFound
}

// - при одинаковых ключах побеждает наиболее приоритетный источник
// - tombstone не возвращаются
func (e *Engine) Scan(start, end []byte) (Iterator, error) {
	var sources []iterator

	// Источник 0: Memtable
	slIt, err := e.memtable.Scan(start, end)
	if err != nil {
		return nil, fmt.Errorf("lsm: Scan Memtable: %w", err)
	}
	sources = append(sources, &memIterWrapper{it: slIt})

	// Источники 1, 2, ...: SSTables от новейшего к старейшему
	for i := len(e.sstables) - 1; i >= 0; i-- {
		sstIt, err := e.sstables[i].reader.Iterator(start, end)
		if err != nil {
			return nil, fmt.Errorf("lsm: Scan SSTable[%d]: %w", i, err)
		}
		sources = append(sources, sstIt)
	}

	return newMergeIter(sources, end), nil
}

// Порядок действий:
//  1. Создаём новый SSTable файл и пишем в него всё из Memtable.
//  2. Открываем созданный SSTable для чтения (добавляем в e.sstables).
//  3. Сбрасываем Memtable (создаём новый пустой SkipList).
//  4. WAL: удаляем старый, создаём новый пустой.
//  5. Если файлов SSTable стало >= compactionThreshold — Compaction.
func (e *Engine) flush() error {
	// Выделяем порядковый номер для нового SSTable
	seq := e.nextSeq
	e.nextSeq++
	sstPath := filepath.Join(e.opts.Dir, fmt.Sprintf(sstableFmt, seq))

	sstFile, err := os.Create(sstPath)
	if err != nil {
		return fmt.Errorf("lsm: создание файла SSTable %q: %w", sstPath, err)
	}

	w := sstable.NewWriter(sstFile)

	// Читаем весь Memtable
	it, err := e.memtable.Scan(nil, nil)
	if err != nil {
		sstFile.Close()
		return fmt.Errorf("lsm: Scan Memtable при flush: %w", err)
	}

	for {
		k, taggedV, ok, err := it.Next()
		if err != nil {
			it.Close()
			sstFile.Close()
			return fmt.Errorf("lsm: итерация Memtable при flush: %w", err)
		}
		if !ok {
			break // все записи обработаны
		}

		// Интерпретируем тэг и записываем соответствующий тип в SSTable
		if len(taggedV) > 0 && taggedV[0] == tagDelete {
			// записываем tombstone в SSTable
			if err := w.AddTombstone(k); err != nil {
				it.Close()
				sstFile.Close()
				return fmt.Errorf("lsm: AddTombstone при flush: %w", err)
			}
		} else {
			// снимаем тэг-байт и записываем чистое значение
			actualVal := taggedV[1:]
			if err := w.Add(k, actualVal); err != nil {
				it.Close()
				sstFile.Close()
				return fmt.Errorf("lsm: Add при flush: %w", err)
			}
		}
	}
	it.Close()

	// Закрываем Writer SSTable (записывает index + footer, сбрасывает буфер)
	if err := w.Close(); err != nil {
		sstFile.Close()
		return fmt.Errorf("lsm: закрытие Writer SSTable: %w", err)
	}

	// Получаем размер файла
	info, err := sstFile.Stat()
	if err != nil {
		sstFile.Close()
		return fmt.Errorf("lsm: stat нового SSTable: %w", err)
	}
	sstFile.Close()

	// lобавляем в список SSTables
	sstReadFile, err := os.Open(sstPath)
	if err != nil {
		return fmt.Errorf("lsm: открытие нового SSTable для чтения: %w", err)
	}
	reader := sstable.NewReader(sstReadFile, info.Size())
	e.sstables = append(e.sstables, &sstableHandle{
		path:   sstPath,
		seq:    seq,
		file:   sstReadFile,
		reader: reader,
	})

	// Сбрасываем Memtable
	e.memtable = skiplist.New(int64(e.nextSeq))
	e.memBytes = 0

	// Удаляем wal и создаём новый пустой WAL.
	e.wal.Close()
	e.walFile.Close()
	walPath := filepath.Join(e.opts.Dir, walFileName)
	os.Remove(walPath)

	newWALFile, err := os.OpenFile(walPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("lsm: создание нового WAL после flush: %w", err)
	}
	e.walFile = newWALFile
	e.wal = wal.NewWriter(newWALFile)

	// компановка
	if len(e.sstables) >= compactionThreshold {
		return e.compact()
	}
	return nil
}

// Удяляем tombstone и объединяем все SSTable файлы в один новый.
func (e *Engine) compact() error {
	if len(e.sstables) < 2 {
		return nil // нечего компановать
	}

	// итераторы sst - самый новый - наивысший приоритет
	var sources []iterator
	for i := len(e.sstables) - 1; i >= 0; i-- {
		it, err := e.sstables[i].reader.Iterator(nil, nil) // nil,nil = весь файл
		if err != nil {
			return fmt.Errorf("lsm: compact Iterator[%d]: %w", i, err)
		}
		sources = append(sources, it)
	}

	// Создаём merge-итератор для слияния всех источников
	mi := newMergeIter(sources, nil)

	// Создаём новый SSTable для скомпанованых данных
	seq := e.nextSeq
	e.nextSeq++
	compactPath := filepath.Join(e.opts.Dir, fmt.Sprintf(sstableFmt, seq))

	compactFile, err := os.Create(compactPath)
	if err != nil {
		mi.Close()
		return fmt.Errorf("lsm: создание файла для Compaction: %w", err)
	}
	w := sstable.NewWriter(compactFile)

	// Записываем объединённые данные в новый SSTable. без ts
	for {
		k, v, ok, err := mi.Next()
		if err != nil {
			compactFile.Close()
			mi.Close()
			return fmt.Errorf("lsm: compact merge Next: %w", err)
		}
		if !ok {
			break
		}
		// nil - tombstone
		if v == nil {
			continue
		}
		if err := w.Add(k, v); err != nil {
			compactFile.Close()
			mi.Close()
			return fmt.Errorf("lsm: compact Add: %w", err)
		}
	}
	mi.Close()

	if err := w.Close(); err != nil {
		compactFile.Close()
		return fmt.Errorf("lsm: compact Writer Close: %w", err)
	}

	info, err := compactFile.Stat()
	if err != nil {
		compactFile.Close()
		return fmt.Errorf("lsm: compact stat: %w", err)
	}
	compactFile.Close()

	// удаляем все старые SSTable файлы
	for _, h := range e.sstables {
		h.file.Close()
		os.Remove(h.path)
	}

	// Открываем скомпанованный файл как новый SSTable
	newFile, err := os.Open(compactPath)
	if err != nil {
		return fmt.Errorf("lsm: открытие скомпанованного SSTable: %w", err)
	}

	reader := sstable.NewReader(newFile, info.Size())
	e.sstables = []*sstableHandle{{
		path:   compactPath,
		seq:    seq,
		file:   newFile,
		reader: reader,
	}}

	return nil
}

// iterator — внутренний интерфейс для источников данных в merge-итераторе.
// Реализуется и sstable.Iter, и memIterWrapper.
type iterator interface {
	Next() (key, value []byte, ok bool, err error)
	Close() error
}

// heapEntry — элемент кучи (min-heap) для k-way merge.
// Heap упорядочен по ключу (меньший ключ = выше в куче).
// При равных ключах: меньший srcIdx = более высокий приоритет (более новый источник).
type heapEntry struct {
	key    []byte
	value  []byte // nil означает tombstone
	srcIdx int    // индекс источника в mergeIter.sources (0 = наиболее приоритетный)
}

// mergeHeap реализует heap.Interface для min-heap на heapEntry.
// container/heap в Go реализует кучу через интерфейс: нужно определить
// Len, Less, Swap, Push, Pop — и мы получаем эффективную кучу O(log n).
type mergeHeap []heapEntry

func (h mergeHeap) Len() int { return len(h) }

// Less определяет порядок: меньший ключ имеет приоритет.
// При равных ключах — меньший srcIdx (= более новый источник) имеет приоритет.
func (h mergeHeap) Less(i, j int) bool {
	c := bytes.Compare(h[i].key, h[j].key)
	if c != 0 {
		return c < 0 // более маленький ключ — выше в куче
	}
	return h[i].srcIdx < h[j].srcIdx // при равных ключах — более свежий источник
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(heapEntry))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// mergeIter реализует k-way merge нескольких отсортированных источников.
//
// Алгоритм (аналогично фазе Merge в Merge Sort):
//  1. Инициализация: берём первый элемент из каждого источника, кладём в min-heap.
//  2. Next(): извлекаем минимальный элемент из кучи (это наш "победитель").
//  3. Продвигаем источник победителя: берём следующий элемент, кладём в кучу.
//  4. Пропускаем все элементы с тем же ключом из других источников (они устарели).
//  5. Если победитель — tombstone, повторяем с шага 2 (не возвращаем пользователю).
type mergeIter struct {
	sources []iterator // все источники данных (0 = наиболее приоритетный)
	h       mergeHeap  // min-heap для эффективного нахождения минимального ключа
	end     []byte     // верхняя граница диапазона (exclusive)
}

// newMergeIter создаёт merge-итератор.
// sources[0] — наиболее приоритетный (обычно Memtable).
// end — верхняя граница диапазона (nil = без ограничения).
func newMergeIter(sources []iterator, end []byte) *mergeIter {
	mi := &mergeIter{sources: sources, end: end}
	mi.h = make(mergeHeap, 0, len(sources))
	heap.Init(&mi.h)

	// Инициализируем кучу: берём первый элемент из каждого источника
	for idx, src := range sources {
		k, v, ok, _ := src.Next()
		if ok {
			heap.Push(&mi.h, heapEntry{key: k, value: v, srcIdx: idx})
		}
	}
	return mi
}

// Next возвращает следующий уникальный ключ в порядке возрастания.
// Tombstone-записи пропускаются — пользователь их не видит.
func (mi *mergeIter) Next() (key, value []byte, ok bool, err error) {
	for {
		// Если куча пуста — все источники исчерпаны
		if mi.h.Len() == 0 {
			return nil, nil, false, nil
		}

		// Извлекаем запись с минимальным ключом из кучи.
		// При равных ключах — из наиболее приоритетного источника (меньший srcIdx).
		winner := heap.Pop(&mi.h).(heapEntry)

		// Проверяем верхнюю границу диапазона
		if mi.end != nil && bytes.Compare(winner.key, mi.end) >= 0 {
			return nil, nil, false, nil
		}

		// Продвигаем источник победителя: берём его следующий элемент
		if k2, v2, ok2, _ := mi.sources[winner.srcIdx].Next(); ok2 {
			heap.Push(&mi.h, heapEntry{key: k2, value: v2, srcIdx: winner.srcIdx})
		}

		// Пропускаем все записи с тем же ключом из других источников.
		// Они старее победителя, поэтому не должны возвращаться пользователю.
		for mi.h.Len() > 0 && bytes.Equal(mi.h[0].key, winner.key) {
			dup := heap.Pop(&mi.h).(heapEntry)
			// Продвигаем источник дубликата, чтобы не потерять его следующие элементы
			if k2, v2, ok2, _ := mi.sources[dup.srcIdx].Next(); ok2 {
				heap.Push(&mi.h, heapEntry{key: k2, value: v2, srcIdx: dup.srcIdx})
			}
		}

		// Если победитель — tombstone (value=nil), не возвращаем его пользователю.
		// Ключ "удалён" — пропускаем и берём следующий.
		if winner.value == nil {
			continue
		}

		return winner.key, winner.value, true, nil
	}
}

// Close закрывает все источники итератора.
func (mi *mergeIter) Close() error {
	for _, src := range mi.sources {
		src.Close()
	}
	return nil
}

// memIterWrapper адаптирует skiplist.Iterator к внутреннему интерфейсу iterator.
// Снимает тэг-байт с каждого значения и преобразует tombstone в nil.
type memIterWrapper struct {
	it skiplist.Iterator
}

// Next читает следующую запись из Memtable и снимает тэг.
// Tombstone (tagDelete) преобразуется в value=nil — сигнал для merge-итератора.
func (m *memIterWrapper) Next() (key, value []byte, ok bool, err error) {
	k, taggedV, ok, err := m.it.Next()
	if !ok || err != nil {
		return nil, nil, ok, err
	}

	if len(taggedV) > 0 && taggedV[0] == tagDelete {
		// Tombstone: value=nil сигнализирует merge-итератору об удалении ключа
		return k, nil, true, nil
	}
	// Обычная запись: снимаем первый байт (тэг) и возвращаем чистое значение
	return k, taggedV[1:], true, nil
}

func (m *memIterWrapper) Close() error {
	return m.it.Close()
}

// Проверка на этапе компиляции, что mergeIter реализует Iterator.
var _ Iterator = (*mergeIter)(nil)
