package duckdb_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

func TestNativeExternalThreadingTaskState(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	db, err := lib.Open(ctx, ":memory:",
		duckdb.Option{Name: "threads", Value: "3"},
		duckdb.Option{Name: "external_threads", Value: "2"},
	)
	if err != nil {
		t.Fatalf("Open with external_threads: %v", err)
	}
	defer db.Close()

	// Exercise one-shot Database.ExecuteTasks.
	if err := db.ExecuteTasks(4); err != nil {
		t.Fatalf("ExecuteTasks: %v", err)
	}

	ts, err := db.NewTaskState()
	if err != nil {
		t.Fatalf("NewTaskState: %v", err)
	}
	if ts.IsFinished() {
		t.Fatal("expected IsFinished == false before Finish")
	}

	// Launch two external worker goroutines sharing the same TaskState while
	// executing a parallel aggregation query.
	var workerWG sync.WaitGroup
	for range 2 {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			if err := ts.Execute(); err != nil {
				t.Errorf("ts.Execute: %v", err)
			}
		}()
	}

	conn, err := db.Connect(ctx)
	if err != nil {
		_ = ts.Close()
		workerWG.Wait()
		t.Fatal(err)
	}

	_, row := querySingleRow(t, conn, "SELECT sum(i)::BIGINT FROM range(100000) t(i)")
	if sum, _ := row[0].Int64(); sum != 4999950000 {
		t.Fatalf("sum = %d, want 4999950000", sum)
	}

	// Also exercise ExecuteN on a separate TaskState.
	tsN, err := db.NewTaskState()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tsN.ExecuteN(8); err != nil {
		t.Fatalf("tsN.ExecuteN: %v", err)
	}
	tsN.Finish()
	if !tsN.IsFinished() {
		t.Fatal("expected tsN.IsFinished == true after Finish")
	}
	if err := tsN.Close(); err != nil {
		t.Fatalf("tsN.Close: %v", err)
	}

	// Finish and close the shared TaskState; both workers must return cleanly.
	ts.Finish()
	if !ts.IsFinished() {
		t.Fatal("expected ts.IsFinished == true after Finish")
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("ts.Close: %v", err)
	}
	workerWG.Wait()

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	// Leave an open TaskState with an active worker and let db.Close() wake & join it.
	tsAuto, err := db.NewTaskState()
	if err != nil {
		t.Fatal(err)
	}
	autoDone := make(chan error, 1)
	go func() {
		autoDone <- tsAuto.Execute()
	}()
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
	if err := <-autoDone; err != nil && !errors.Is(err, duckdb.ErrClosed) {
		t.Fatalf("tsAuto.Execute after db.Close: %v", err)
	}
}
