// kv.Store поверх LSM движка.
package lsmstore

import (
	"context"
	"errors"
	"fmt"

	"kvschool/internal/kv"
	"kvschool/internal/lsm"
)

var ErrNotImplemented = errors.New("lsmstore: функция не реализована")

// Store — реализация kv.Store на основе LSM движка.
type Store struct {
	engine *lsm.Engine // LSM движок, который реально хранит данные
}

// параметры для открытия LSM хранилища.
type Options struct {
	Dir string // путь к директории, где будут храниться файлы WAL и SSTable
}

// Open открывает (или создаёт) LSM хранилище в указанной директории.
// Если директория уже содержит файлы — данные будут восстановлены.
func Open(opts Options) (*Store, error) {
	engine, err := lsm.Open(lsm.Options{
		Dir: opts.Dir,
	})
	if err != nil {
		return nil, fmt.Errorf("lsmstore: открытие LSM движка: %w", err)
	}
	return &Store{engine: engine}, nil
}

func (s *Store) Put(_ context.Context, key, value []byte) error {
	return s.engine.Put(key, value)
}

func (s *Store) Get(_ context.Context, key []byte) ([]byte, error) {
	v, err := s.engine.Get(key)
	if err == lsm.ErrNotFound {
		return nil, kv.ErrNotFound
	}
	return v, err
}

// Delete помечает ключ как удалённый.
func (s *Store) Delete(_ context.Context, key []byte) error {
	return s.engine.Delete(key)
}

// Scan возвращает итератор по ключам в диапазоне [start, end).
func (s *Store) Scan(_ context.Context, start, end []byte) (kv.Iterator, error) {
	it, err := s.engine.Scan(start, end)
	if err != nil {
		return nil, err
	}
	// Оборачиваем lsm.Iterator в lsmIterAdapter, который реализует kv.Iterator
	return &lsmIterAdapter{it: it}, nil
}

// сбрасывает данные на диск
func (s *Store) Close() error {
	return s.engine.Close()
}

// адаптация lsm.Iterator к интерфейсу kv.Iterator.
//
// Разница между интерфейсами:
//   - lsm.Iterator.Next() возвращает (key, value []byte, ok bool, err error)
//   - kv.Iterator.Next() возвращает (kv.Pair, ok bool, err error)
type lsmIterAdapter struct {
	it lsm.Iterator
}

// Next возвращает следующую пару ключ/значение в формате kv.Pair.
func (a *lsmIterAdapter) Next() (kv.Pair, bool, error) {
	k, v, ok, err := a.it.Next()
	if !ok || err != nil {
		return kv.Pair{}, ok, err
	}
	return kv.Pair{Key: k, Value: v}, true, nil
}

func (a *lsmIterAdapter) Close() error {
	return a.it.Close()
}

// приверка на соотв.
var _ kv.Store = (*Store)(nil)
