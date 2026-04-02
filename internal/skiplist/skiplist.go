package skiplist

import (
	"bytes"
	"errors"
	"math/rand"
)

// ErrNotFound означает отсутствие ключа (IMSI).
var ErrNotFound = errors.New("skiplist: ключ не найден")

// ErrNotImplemented используется в заготовке практики первого дня.
var ErrNotImplemented = errors.New("skiplist: функция не реализована")

// Iterator — упорядоченная итерация по диапазону ключей (Range Scan).
// В HLR используется для выгрузки абонентов по префиксу IMSI.
type Iterator interface {
	Next() (key, value []byte, ok bool, err error)
	Close() error
}

const (
	maxLevel = 32
	p        = 0.5
)

type node struct {
	key     []byte
	value   []byte
	forward []*node // forward[i] - следующий узел на уровне i;
}

func newNode(key, value []byte, level int) *node {
	return &node{
		key:     key,
		value:   value,
		forward: make([]*node, level),
	}
}

type SkipList struct {
	head  *node // не хранит данных, только указатели; ключ = nil
	level int   // текущий максимальный уровень в списке,изначально 0
	rng   *rand.Rand
	size  int // количество элементов (не считая sentinel head)
}

func New(seed int64) *SkipList {
	return &SkipList{
		head: newNode(nil, nil, maxLevel),

		// level = 0 означает: список пустой, активен только уровень 0.
		level: 0,

		rng: rand.New(rand.NewSource(seed)),

		size: 0,
	}
}

func (s *SkipList) randomLevel() int {
	lvl := 1
	for s.rng.Float64() < p && lvl < maxLevel {
		lvl++
	}
	return lvl
}

func copyBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	return append([]byte(nil), src...)
}

func (s *SkipList) Put(key, value []byte) error {

	// После поиска мы обновим update[i].forward[i] чтобы вставить новый узел.
	var update [maxLevel]*node // узлы-предшественники на уровне i.

	curr := s.head

	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && bytes.Compare(curr.forward[i].key, key) < 0 {
			curr = curr.forward[i]
		}
		// curr — последний узел на уровне i, чей ключ < key.
		// Значит, новый узел должен встать после него.
		update[i] = curr
	}

	// После цикла curr.forward[0] — первый узел с ключом >= key (или nil).
	candidate := curr.forward[0]
	if candidate != nil && bytes.Equal(candidate.key, key) {
		candidate.value = copyBytes(value) // если ключ совпадает - обновляем
		return nil
	}

	lvl := s.randomLevel()

	if lvl > s.level { // инит доп.уровней
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	n := newNode(copyBytes(key), copyBytes(value), lvl)

	for i := 0; i < lvl; i++ {
		n.forward[i] = update[i].forward[i] // вставка узла на каждом уровне
		update[i].forward[i] = n
	}

	s.size++
	return nil
}

func (s *SkipList) Get(key []byte) ([]byte, error) {
	curr := s.head
	for i := s.level - 1; i >= 0; i-- { // сверху вниз
		// вправо по уровню
		for curr.forward[i] != nil && bytes.Compare(curr.forward[i].key, key) < 0 {
			curr = curr.forward[i]
		}
	}
	candidate := curr.forward[0] // первый узел с ключом >= key/nil.
	if candidate == nil || !bytes.Equal(candidate.key, key) {
		return nil, ErrNotFound
	}
	return copyBytes(candidate.value), nil
}

func (s *SkipList) Delete(key []byte) error {
	var update [maxLevel]*node
	curr := s.head

	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && bytes.Compare(curr.forward[i].key, key) < 0 {
			curr = curr.forward[i] // предшественник
		}
		update[i] = curr
	}

	target := curr.forward[0]
	if target == nil || !bytes.Equal(target.key, key) { // существует ли ключ
		return ErrNotFound
	}

	for i := 0; i < s.level; i++ { // Удаляем узел с каждого уровня, на котором он присутствует.
		if update[i].forward[i] != target { // target на этом уровне не присутствует, выше тоже не будет
			break
		}
		update[i].forward[i] = target.forward[i]
	}

	for s.level > 0 && s.head.forward[s.level-1] == nil { // Если верхний уровень пустой, понижаем уровень списка.
		s.level--
	}

	s.size--
	return nil
}

func (s *SkipList) Scan(start, end []byte) (Iterator, error) {
	curr := s.head

	if start != nil {
		for i := s.level - 1; i >= 0; i-- { // Ищем первый узел с ключом >= start
			for curr.forward[i] != nil && bytes.Compare(curr.forward[i].key, start) < 0 {
				curr = curr.forward[i]
			}
		}
	}
	return &listIter{
		curr: curr.forward[0],
		end:  end,
	}, nil
}

// Уровень 0 содержит все узлы в отсортированном порядке.
type listIter struct {
	curr *node  // следующий узел, который вернёт Next(); nil = итерация закончена
	end  []byte // верхняя граница диапазона (exclusive); nil = нет ограничения
}

func (it *listIter) Next() (key, value []byte, ok bool, err error) {
	// Проверяем: достигли ли конца списка или границы диапазона.
	if it.curr == nil {
		return nil, nil, false, nil
	}

	if it.end != nil && bytes.Compare(it.curr.key, it.end) >= 0 { // конец
		return nil, nil, false, nil
	}

	n := it.curr
	it.curr = it.curr.forward[0] // переходим к следующему узлу на уровне 0

	return copyBytes(n.key), copyBytes(n.value), true, nil
}

func (it *listIter) Close() error {
	it.curr = nil // помечаем итератор как исчерпанный
	return nil
}

var _ Iterator = (*listIter)(nil)
