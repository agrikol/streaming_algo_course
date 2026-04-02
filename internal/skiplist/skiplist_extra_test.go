//go:build day1

package skiplist

import (
	"fmt"
	"testing"
)

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
