// Формат каждой записи в WAL (length-prefixed):
//
//	[4 байта: uint32 LE] totalPayloadLen  — размер полезной нагрузки (всего что идёт ниже)
//	[1 байт]             opType           — тип операции: 1=Put, 2=Delete
//	[4 байта: uint32 LE] keyLen           — длина ключа
//	[keyLen байт]        key              — сам ключ
//	[4 байта: uint32 LE] valLen           — длина значения (0 для Delete)
//	[valLen байт]        value            — значение (отсутствует для Delete)
//
// length-prefixed

package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var ErrNotImplemented = errors.New("wal: функция не реализована")

// OpType — тип операции в WAL.
type OpType byte

const (
	OpPut    OpType = 1 // запись нового или обновление существующего значения
	OpDelete OpType = 2 // tombstone

)

// Record — одна запись в журнале.
// При восстановлении движок LSM читает все записи
// и воспроизводит их в Memtable в том же порядке.
type Record struct {
	Type  OpType
	Key   []byte
	Value []byte // для OpDelete — пустой срез
}

// Writer пишет записи в append-only лог.
// Использует bufio.Writer для буферизации, но флашит буфер после каждой
// записи — это гарантирует, что данные попадут в файл до того, как
// мы сообщим пользователю об успехе операции.
type Writer struct {
	w *bufio.Writer // буферизованный writer поверх файла
}

// NewWriter создаёт Writer, который будет писать в переданный io.Writer.
// Обычно в качестве w передаётся открытый файл (os.File).
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w)}
}

// Append записывает одну запись в WAL.
func (w *Writer) Append(r Record) error {
	keyLen := uint32(len(r.Key))
	valLen := uint32(len(r.Value))

	// totalPayloadLen — это размер всего что мы запишем после первых 4 байт:
	// 1 байт opType + 4 байта keyLen + keyLen байт key + 4 байта valLen + valLen байт value
	totalPayloadLen := uint32(1 + 4 + int(keyLen) + 4 + int(valLen))

	// Вспомогательный буфер для записи 4-байтных чисел
	var buf [4]byte

	// Записываем размер записи (4 байта, little-endian).
	// Зная этот размер, читатель сможет за одно чтение получить всю запись.
	binary.LittleEndian.PutUint32(buf[:], totalPayloadLen)
	if _, err := w.w.Write(buf[:]); err != nil {
		return fmt.Errorf("wal: запись totalPayloadLen: %w", err)
	}

	// Записываем тип операции (1 байт): Put=1 или Delete=2
	if err := w.w.WriteByte(byte(r.Type)); err != nil {
		return fmt.Errorf("wal: запись opType: %w", err)
	}

	// Записываем длину ключа (4 байта), затем сам ключ
	binary.LittleEndian.PutUint32(buf[:], keyLen)
	if _, err := w.w.Write(buf[:]); err != nil {
		return fmt.Errorf("wal: запись keyLen: %w", err)
	}
	if len(r.Key) > 0 {
		if _, err := w.w.Write(r.Key); err != nil {
			return fmt.Errorf("wal: запись key: %w", err)
		}
	}

	// Записываем длину значения (4 байта), затем само значение.
	// Для Delete: valLen=0 и value не записывается (valLen записывается как 0).
	binary.LittleEndian.PutUint32(buf[:], valLen)
	if _, err := w.w.Write(buf[:]); err != nil {
		return fmt.Errorf("wal: запись valLen: %w", err)
	}
	if len(r.Value) > 0 {
		if _, err := w.w.Write(r.Value); err != nil {
			return fmt.Errorf("wal: запись value: %w", err)
		}
	}

	// Сбрасываем буфер — запись попадает в файл прямо сейчас.
	// Без этого данные могут "застрять" в буфере и потеряться при краше.
	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("wal: flush после записи: %w", err)
	}
	return nil
}

// Close сбрасывает остаток буфера. Вызывается при закрытии WAL.
func (w *Writer) Close() error {
	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("wal: flush при закрытии: %w", err)
	}
	return nil
}

// Reader читает записи из WAL последовательно.
// Используется один раз при старте системы для восстановления Memtable.
type Reader struct {
	r *bufio.Reader // буферизованный reader
}

// NewReader создаёт Reader, который будет читать из переданного io.Reader.
// Обычно в качестве r передаётся открытый WAL файл (os.File).
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// Next читает следующую запись из WAL.
//
//   - (запись, true, nil)  — запись прочитана успешно
//   - (Record{}, false, nil) — достигнут конец лога (нормальное завершение)
//   - (Record{}, false, err) — ошибка чтения
func (r *Reader) Next() (Record, bool, error) {
	// Читаем заголовок: размер всей записи (4 байта)
	var lenBuf [4]byte
	_, err := io.ReadFull(r.r, lenBuf[:])
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return Record{}, false, nil
		}
		return Record{}, false, fmt.Errorf("wal: чтение заголовка записи: %w", err)
	}
	totalPayloadLen := binary.LittleEndian.Uint32(lenBuf[:])

	// Читаем весь payload одним куском для простоты разбора.
	payload := make([]byte, totalPayloadLen)
	_, err = io.ReadFull(r.r, payload)
	if err != nil {
		// Payload не дочитан до конца — запись была обрезана при краше.
		// Просто останавливаем чтение WAL, не возвращая ошибку.
		if err == io.ErrUnexpectedEOF {
			return Record{}, false, nil
		}
		return Record{}, false, fmt.Errorf("wal: чтение тела записи: %w", err)
	}

	// Разбираем payload из буфера
	pos := 0

	// Байт 0: тип операции
	opType := OpType(payload[pos])
	pos++

	// Байты 1-4: длина ключа
	keyLen := binary.LittleEndian.Uint32(payload[pos:])
	pos += 4

	// keyLen байт: сам ключ
	key := make([]byte, keyLen)
	copy(key, payload[pos:])
	pos += int(keyLen)

	// Байты pos..pos+3: длина значения
	valLen := binary.LittleEndian.Uint32(payload[pos:])
	pos += 4

	// valLen байт: само значение (пусто для Delete)
	val := make([]byte, valLen)
	copy(val, payload[pos:])

	return Record{Type: opType, Key: key, Value: val}, true, nil
}
