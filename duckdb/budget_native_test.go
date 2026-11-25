package duckdb_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bububa/duckdbspike/duckdb"
)

func dirBytesAndFiles(dir string) (int64, int) {
	var total int64
	var files int
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if sz, ok := fileLiveSize(p, d); ok {
			total += sz
			files++
		}
		return nil
	})
	return total, files
}

func TestNativeSpillResourceBudgetAndExhaustion(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	bc, err := duckdb.NewBudgetController(duckdb.GlobalBudget{
		MaxConcurrent:    2,
		MaxWaitQueue:     2,
		TotalMemoryBytes: 512 << 20,
		TotalThreads:     4,
		TotalSpillBytes:  2 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()

	// 1) Controlled spill under 128 MiB memory_limit with exclusive spill dir.
	spillDir := filepath.Join(t.TempDir(), "spill")
	if err := os.MkdirAll(spillDir, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := bc.Open(ctx, lib, ":memory:", duckdb.InstanceConfig{
		NativeMemoryBytes: 128 << 20,
		Threads:           2,
		SpillDir:          spillDir,
		SpillBytes:        1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Connect(ctx)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}

	var peakSpill int64
	stopPoll := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			if b, _ := dirBytesAndFiles(spillDir); b > peakSpill {
				peakSpill = b
			}
			select {
			case <-stopPoll:
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	})

	rows, err := conn.Query(ctx, "SELECT i, repeat('x', 240) FROM range(1500000) t(i) ORDER BY hash(i)")
	if err != nil {
		close(stopPoll)
		wg.Wait()
		_ = db.Close()
		t.Fatalf("spilling sort query failed: %v", err)
	}
	var total int
	var batch [][]duckdb.Value
	for {
		batch, err = rows.ReadBatch(512, batch)
		if err != nil {
			break
		}
		if len(batch) == 0 {
			break
		}
		total += len(batch)
	}
	_ = rows.Close()
	close(stopPoll)
	wg.Wait()
	if err != nil {
		_ = db.Close()
		t.Fatalf("ReadBatch error: %v", err)
	}
	if total != 1500000 {
		_ = db.Close()
		t.Fatalf("total rows = %d, want 1500000", total)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	afterBytes, afterFiles := dirBytesAndFiles(spillDir)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("spill baseline: peak_spill=%.1fMiB spill_after_close=%dB/%dfiles maxrss=%.1fMiB go_heap_inuse=%.1fMiB fds=%d",
		float64(peakSpill)/(1<<20), afterBytes, afterFiles,
		float64(maxRSSBytes())/(1<<20), float64(ms.HeapInuse)/(1<<20), openFDCount())
	if peakSpill == 0 {
		t.Fatalf("expected real disk spill under 128MiB limit, got peakSpill=0")
	}
	if afterBytes != 0 || afterFiles != 0 {
		t.Fatalf("spill files leaked after Close: %dB / %d files", afterBytes, afterFiles)
	}

	// 2) Tight max_temp_directory_size must fail cleanly without partial success.
	tightSpill := filepath.Join(t.TempDir(), "tight-spill")
	if err := os.MkdirAll(tightSpill, 0o700); err != nil {
		t.Fatal(err)
	}
	dbTight, err := bc.Open(ctx, lib, ":memory:", duckdb.InstanceConfig{
		NativeMemoryBytes: 64 << 20,
		Threads:           2,
		SpillDir:          tightSpill,
		SpillBytes:        1 << 20, // 1 MiB spill cap
	})
	if err != nil {
		t.Fatal(err)
	}
	cTight, _ := dbTight.Connect(ctx)
	rTight, err := cTight.Query(ctx, "SELECT i, repeat('y', 120) FROM range(3000000) t(i) ORDER BY hash(i)")
	if err == nil {
		for rTight.Next() {
		}
		err = rTight.Err()
		_ = rTight.Close()
	}
	if err == nil {
		_ = dbTight.Close()
		t.Fatal("expected spill exhaustion error under 1MiB cap")
	}
	_ = dbTight.Close()
	if b, f := dirBytesAndFiles(tightSpill); b != 0 || f != 0 {
		t.Fatalf("tight spill leaked after error+close: %dB / %d files", b, f)
	}

	// 3) Per-query MaxRows and MaxBytes return ErrBudgetExceeded (never EOF).
	dbLim, err := bc.Open(ctx, lib, ":memory:", duckdb.InstanceConfig{
		NativeMemoryBytes: 64 << 20,
		Threads:           2,
		ResultLimits:      duckdb.ResultLimits{MaxRows: 1000, MaxBytes: 64 << 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer dbLim.Close()
	cLim, _ := dbLim.Connect(ctx)

	// Wide rows hit MaxBytes before MaxRows.
	rWide, err := cLim.Query(ctx, "SELECT repeat('w', 4096) FROM range(100) t(i)")
	if err != nil {
		t.Fatal(err)
	}
	var wideErr error
	for rWide.Next() {
		if _, err := rWide.DecodeRow(nil); err != nil {
			wideErr = err
			break
		}
	}
	if wideErr == nil {
		wideErr = rWide.Err()
	}
	_ = rWide.Close()
	if !errors.Is(wideErr, duckdb.ErrBudgetExceeded) {
		t.Fatalf("wide row err = %v, want ErrBudgetExceeded", wideErr)
	}

	// Narrow rows hit MaxRows.
	if err := cLim.SetResultLimits(duckdb.ResultLimits{MaxRows: 1000}); err != nil {
		t.Fatal(err)
	}
	rMany, err := cLim.Query(ctx, "SELECT i FROM range(5000) t(i)")
	if err != nil {
		t.Fatal(err)
	}
	var read int
	for rMany.Next() {
		read++
	}
	manyErr := rMany.Err()
	_ = rMany.Close()
	if read != 1000 || !errors.Is(manyErr, duckdb.ErrBudgetExceeded) {
		t.Fatalf("MaxRows read=%d err=%v, want 1000 + ErrBudgetExceeded", read, manyErr)
	}
}

func TestNativeRepeatedQueryResourcePlateau(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	spillDir := filepath.Join(t.TempDir(), "spill")
	if err := os.MkdirAll(spillDir, 0o700); err != nil {
		t.Fatal(err)
	}
	bc, err := duckdb.NewBudgetController(duckdb.GlobalBudget{
		MaxConcurrent:    1,
		MaxWaitQueue:     1,
		TotalMemoryBytes: 128 << 20,
		TotalThreads:     2,
		TotalSpillBytes:  256 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()

	fdsBefore := openFDCount()
	for iter := range 40 {
		db, err := bc.Open(ctx, lib, ":memory:", duckdb.InstanceConfig{
			NativeMemoryBytes: 64 << 20,
			Threads:           2,
			SpillDir:          spillDir,
			SpillBytes:        128 << 20,
		})
		if err != nil {
			t.Fatalf("iter %d open: %v", iter, err)
		}
		conn, err := db.Connect(ctx)
		if err != nil {
			_ = db.Close()
			t.Fatalf("iter %d connect: %v", iter, err)
		}
		rows, err := conn.Query(ctx, "SELECT i % 256 AS g, count(*) AS c FROM range(20000) t(i) GROUP BY g")
		if err != nil {
			_ = db.Close()
			t.Fatalf("iter %d query: %v", iter, err)
		}
		var n int
		var buf []duckdb.Value
		for rows.Next() {
			buf, err = rows.DecodeRow(buf)
			if err != nil {
				t.Fatalf("iter %d decode: %v", iter, err)
			}
			n++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iter %d err: %v", iter, err)
		}
		_ = rows.Close()
		_ = db.Close()
		if n != 256 {
			t.Fatalf("iter %d groups = %d, want 256", iter, n)
		}
	}
	runtime.GC()
	fdsAfter := openFDCount()
	spillAfter, filesAfter := dirBytesAndFiles(spillDir)
	snap := bc.Snapshot()
	t.Logf("plateau after 40 DB cycles: fds_before=%d fds_after=%d spill=%dB/%dfiles budget=%+v",
		fdsBefore, fdsAfter, spillAfter, filesAfter, snap)
	if snap.ActiveInstances != 0 || snap.UsedMemoryBytes != 0 {
		t.Fatalf("budget leaked: %+v", snap)
	}
	if spillAfter != 0 || filesAfter != 0 {
		t.Fatalf("spill leaked: %dB/%dfiles", spillAfter, filesAfter)
	}
	if fdsBefore > 0 && fdsAfter > fdsBefore+4 {
		t.Fatalf("possible FD leak: before=%d after=%d", fdsBefore, fdsAfter)
	}
}
