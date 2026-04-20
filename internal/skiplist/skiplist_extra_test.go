//go:build day1

package skiplist

import (
	"bytes"
	"fmt"
	"testing"
)

type listIterNoCopy struct {
	curr *node
	end  []byte
}

func (it *listIterNoCopy) Next() (key, value []byte, ok bool, err error) {
	if it.curr == nil {
		return nil, nil, false, nil
	}
	if it.end != nil && bytes.Compare(it.curr.key, it.end) >= 0 {
		return nil, nil, false, nil
	}
	n := it.curr
	it.curr = it.curr.forward[0]
	return n.key, n.value, true, nil
}

func (it *listIterNoCopy) Close() error {
	it.curr = nil
	return nil
}

func scanNoCopy(s *SkipList, start, end []byte) Iterator {
	curr := s.head
	if start != nil {
		for i := s.level - 1; i >= 0; i-- {
			for curr.forward[i] != nil && bytes.Compare(curr.forward[i].key, start) < 0 {
				curr = curr.forward[i]
			}
		}
	}
	return &listIterNoCopy{curr: curr.forward[0], end: end}
}

func TestPut_UpdateExistingKey(t *testing.T) {
	sl := New(42)

	_ = sl.Put([]byte("key"), []byte("v1"))
	_ = sl.Put([]byte("key"), []byte("v2")) // перезапись

	v, err := sl.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "v2" {
		t.Fatalf("ожидалось v2, получили %q", string(v))
	}
}

func TestDelete_NonExistentKey(t *testing.T) {
	sl := New(42)
	err := sl.Delete([]byte("ghost"))
	if err != ErrNotFound {
		t.Fatalf("ожидали ErrNotFound, получили %v", err)
	}
}

func TestPut_EmptyKey(t *testing.T) {
	sl := New(42)
	if err := sl.Put([]byte{}, []byte("empty key value")); err != nil {
		t.Fatal(err)
	}
	v, err := sl.Get([]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "empty key value" {
		t.Fatalf("неверное значение: %q", v)
	}
}

func TestScan_ExclusiveEndBoundary(t *testing.T) {
	sl := New(42)
	_ = sl.Put([]byte("a"), []byte("1"))
	_ = sl.Put([]byte("b"), []byte("2"))
	_ = sl.Put([]byte("c"), []byte("3"))

	// Scan [a, c) — должен вернуть a и b, но не c.
	it, err := sl.Scan([]byte("a"), []byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	var got []string
	for {
		k, _, ok, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got = append(got, string(k))
	}

	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("неожиданный результат: %v", got)
	}
}

func BenchmarkScan(b *testing.B) {
	for _, n := range []int{10_000} {
		n := n
		sl := New(1)
		for i := 0; i < n; i++ {
			_ = sl.Put([]byte(fmt.Sprintf("key-%08d", i)), []byte("value"))
		}

		b.Run(fmt.Sprintf("copies/N=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				it, _ := sl.Scan(nil, nil)
				for {
					_, _, ok, _ := it.Next()
					if !ok {
						break
					}
				}
				_ = it.Close()
			}
		})

		b.Run(fmt.Sprintf("no_copy/N=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				it := scanNoCopy(sl, nil, nil)
				for {
					_, _, ok, _ := it.Next()
					if !ok {
						break
					}
				}
				_ = it.Close()
			}
		})
	}
}

func BenchmarkGet(b *testing.B) {
	for _, n := range []int{100, 1_000, 100_000} {
		n := n // захватываем значение для замыкания
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			sl := New(1)
			for i := 0; i < n; i++ {
				_ = sl.Put([]byte(fmt.Sprintf("key-%08d", i)), []byte("value"))
			}

			b.ResetTimer() // не считаем время на подготовку

			for i := 0; i < b.N; i++ {
				// % n — ищем ключ из середины диапазона (типичный случай)
				_, _ = sl.Get([]byte(fmt.Sprintf("key-%08d", i%n)))
			}
		})
	}
}
