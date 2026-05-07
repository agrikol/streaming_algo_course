//go:build day2

package lsmstore

// Запуск тестов:  make test-day2
// Запуск бенчей:  go test -tags=day2 -bench=. -benchtime=3s ./internal/kv/lsmstore/

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"kvschool/internal/kv"
)

// openTempStore открывает хранилище во временной директории.
// TempDir() создаётся и очищается фреймворком тестирования автоматически.
func openTempStore(tb testing.TB) *Store {
	tb.Helper()
	s, err := Open(Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	// Close вызывается до удаления TempDir (порядок Cleanup — LIFO)
	tb.Cleanup(func() { s.Close() })
	return s
}

// scanKeys — вспомогательная функция: собирает все ключи из итератора в срез строк.
func scanKeys(tb testing.TB, it kv.Iterator) []string {
	tb.Helper()
	defer it.Close()
	var keys []string
	for {
		pair, ok, err := it.Next()
		if err != nil {
			tb.Fatalf("Iterator.Next: %v", err)
		}
		if !ok {
			break
		}
		keys = append(keys, string(pair.Key))
	}
	return keys
}

// Пустое значение (нулевой []byte) должно храниться и возвращаться корректно.
// В Memtable оно кодируется как [tagPut] (1 байт), значения нет — это нормально.
func TestLSMStore_EmptyValue(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)

	if err := s.Put(ctx, []byte("k"), []byte{}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Get after Put(empty value): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("ожидалось пустое значение, получили %q", got)
	}
}

// Пустой ключ — допустимая граница: keyLen=0 в WAL/SSTable/SkipList.
// Проверяем, что Put/Get с пустым ключом работает и переживает рестарт (через Close+Open).
func TestLSMStore_EmptyKey(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, []byte{}, []byte("empty-key-val")); err != nil {
		t.Fatalf("Put(empty key): %v", err)
	}
	got, err := s.Get(ctx, []byte{})
	if err != nil {
		t.Fatalf("Get(empty key): %v", err)
	}
	if string(got) != "empty-key-val" {
		t.Fatalf("хотели empty-key-val, получили %q", got)
	}

	// Рестарт через Close+Open: пустой ключ должен пройти flush в SSTable
	// и корректно прочитаться обратно (проверка keyLen=0 в формате SSTable).
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	got, err = s2.Get(ctx, []byte{})
	if err != nil {
		t.Fatalf("Get(empty key) после рестарта: %v", err)
	}
	if string(got) != "empty-key-val" {
		t.Fatalf("после рестарта хотели empty-key-val, получили %q", got)
	}
}

// Повторный Put по тому же ключу должен перезаписать значение.
// SkipList хранит последнюю запись; при Scan/Get — возвращает её.
func TestLSMStore_UpdateKey(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)

	for _, v := range []string{"v1", "v2", "v3"} {
		if err := s.Put(ctx, []byte("key"), []byte(v)); err != nil {
			t.Fatalf("Put(%q): %v", v, err)
		}
	}
	got, err := s.Get(ctx, []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v3" {
		t.Fatalf("хотели v3, получили %q", got)
	}
}

// Delete записывает tombstone; Get должен вернуть ErrNotFound.
func TestLSMStore_Delete(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)

	if err := s.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, []byte("k")); err != nil {
		t.Fatal(err)
	}
	_, err := s.Get(ctx, []byte("k"))
	if !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("после Delete хотели ErrNotFound, получили %v", err)
	}
}

// Get по несуществующему ключу должен возвращать ErrNotFound.
func TestLSMStore_GetMissing(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)

	_, err := s.Get(ctx, []byte("ghost"))
	if !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("хотели ErrNotFound, получили %v", err)
	}
}

// Scan([b, d)) должен вернуть b, c — но не a и не d (граница exclusive).
func TestLSMStore_Scan_RangeExclusion(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)

	for _, pair := range []struct{ k, v string }{
		{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"},
	} {
		if err := s.Put(ctx, []byte(pair.k), []byte(pair.v)); err != nil {
			t.Fatal(err)
		}
	}

	it, err := s.Scan(ctx, []byte("b"), []byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	keys := scanKeys(t, it)

	if len(keys) != 2 || keys[0] != "b" || keys[1] != "c" {
		t.Fatalf("хотели [b c], получили %v", keys)
	}
}

// Удалённый ключ не должен появляться в результатах Scan.
func TestLSMStore_Scan_DeletedKeyHidden(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)

	if err := s.Put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, []byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, []byte("a")); err != nil {
		t.Fatal(err)
	}

	it, err := s.Scan(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	keys := scanKeys(t, it)

	if len(keys) != 1 || keys[0] != "b" {
		t.Fatalf("хотели [b], получили %v", keys)
	}
}

// Scan по пустому диапазону (start == end) не должен возвращать записей.
func TestLSMStore_Scan_EmptyRange(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)

	if err := s.Put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	it, err := s.Scan(ctx, []byte("a"), []byte("a")) // [a, a) — пустой диапазон
	if err != nil {
		t.Fatal(err)
	}
	keys := scanKeys(t, it)

	if len(keys) != 0 {
		t.Fatalf("ожидали пустой результат, получили %v", keys)
	}
}

// WAL crash recovery: данные записаны в WAL, но Close() не вызывался —
func TestLSMStore_WALCrashRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Открываем хранилище и пишем данные.
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, []byte("x"), []byte("crash-val")); err != nil {
		t.Fatal(err)
	}
	// Имитируем краш: НЕ вызываем s.Close().
	// SSTable не создан, WAL не удалён — как kill -9.

	// Повторный Open должен найти WAL, воспроизвести запись и вернуть данные.
	s2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open после краша: %v", err)
	}
	defer s2.Close()

	got, err := s2.Get(ctx, []byte("x"))
	if err != nil {
		t.Fatalf("Get после WAL recovery: %v", err)
	}
	if string(got) != "crash-val" {
		t.Fatalf("хотели crash-val, получили %q", got)
	}
}

// BenchmarkLSMStore_Put измеряет скорость записи при разном числе уникальных ключей.
//
// Критическая операция: Put = WAL.Append (sync write) + Memtable.Put.
// При переполнении Memtable происходит flush на диск — это видно как outlier.
// Тренд: ns/op растёт незначительно с N (SkipList O(log N)),
// доминирует задержка WAL sync.
func BenchmarkLSMStore_Put(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{100, 1_000, 10_000} {
		n := n
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			s, err := Open(Options{Dir: b.TempDir()})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()

			// Предварительно генерируем ключи, чтобы не измерять fmt.Sprintf
			keys := make([][]byte, n)
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key-%08d", i))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keys[i%n]
				if err := s.Put(ctx, k, []byte("value-bench")); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkLSMStore_Get измеряет скорость чтения при разном размере хранилища.
//
// При N ≤ порога flush (~4MB) данные живут в Memtable (SkipList O(log N)).
// При большем N часть данных уходит в SSTable — появляется дисковый I/O.
// Тренд: ns/op растёт логарифмически с N пока данные в памяти,
// затем плато или рост из-за SSTable seek.
func BenchmarkLSMStore_Get(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{100, 1_000, 10_000} {
		n := n
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			s, err := Open(Options{Dir: b.TempDir()})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()

			keys := make([][]byte, n)
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key-%08d", i))
				if err := s.Put(ctx, keys[i], []byte("value-bench")); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = s.Get(ctx, keys[i%n])
			}
		})
	}
}
