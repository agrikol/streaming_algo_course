//go:build day2

package sstable

// Бенчмарк уровня C: сравнение двух версий readRecord — legacy (5 ReadAt)
// против оптимизированной (3 ReadAt). Один прогон выводит обе строки,
// дельта видна сразу.
//
// Запуск: go test -tags=day2 -bench=. -benchtime=3s -benchmem ./internal/sstable/

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// buildTestSSTable создаёт во временной директории SSTable с n записями.
// Возвращает открытый Reader, размер файла и функцию очистки.
func buildTestSSTable(tb testing.TB, n int) (*Reader, *os.File) {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), "bench.sst")
	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create: %v", err)
	}

	w := NewWriter(f)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		val := []byte(fmt.Sprintf("value-%08d", i))
		if err := w.Add(key, val); err != nil {
			tb.Fatalf("Add: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		tb.Fatalf("Writer.Close: %v", err)
	}
	if err := f.Close(); err != nil {
		tb.Fatalf("file.Close: %v", err)
	}

	rf, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	tb.Cleanup(func() { rf.Close() })

	st, err := rf.Stat()
	if err != nil {
		tb.Fatalf("stat: %v", err)
	}
	r := NewReader(rf, st.Size())
	if r.loadErr != nil {
		tb.Fatalf("NewReader: %v", r.loadErr)
	}
	return r, rf
}

// BenchmarkSSTable_ReadRecord прогоняет полный последовательный обход SSTable
// двумя версиями readRecord. Это эмулирует горячий путь Scan.
//
// Чтобы избежать смешения сетапа и измерения, b.N означает повтор полного обхода:
// сетап (создание SSTable) делается один раз до b.ResetTimer().
func BenchmarkSSTable_ReadRecord(b *testing.B) {
	const n = 10_000

	b.Run("legacy_5_readat", func(b *testing.B) {
		r, _ := buildTestSSTable(b, n)
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			it, err := r.Iterator(nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			it.mode = readModeLegacy

			cnt := 0
			for {
				_, _, ok, err := it.Next()
				if err != nil {
					b.Fatal(err)
				}
				if !ok {
					break
				}
				cnt++
			}
			it.Close()
			if cnt != n {
				b.Fatalf("прочитано %d, ожидалось %d", cnt, n)
			}
		}
	})

	b.Run("optimized_3_readat", func(b *testing.B) {
		r, _ := buildTestSSTable(b, n)
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			it, err := r.Iterator(nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			// mode = readModeOptimized по умолчанию

			cnt := 0
			for {
				_, _, ok, err := it.Next()
				if err != nil {
					b.Fatal(err)
				}
				if !ok {
					break
				}
				cnt++
			}
			it.Close()
			if cnt != n {
				b.Fatalf("прочитано %d, ожидалось %d", cnt, n)
			}
		}
	})
}

