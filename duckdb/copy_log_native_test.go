package duckdb_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/internal/abi"
)

func TestNativeCopyFunctionCopyToAndCopyFromRoundTrip(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	baseHooks := abi.HookHandleCount()

	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Connect(ctx)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}

	type tsvBind struct {
		delim string
	}
	type tsvWriteState struct {
		mu   sync.Mutex
		file *os.File
	}
	type tsvReadBind struct {
		path       string
		delim      string
		targetCols []duckdb.Column
	}
	type tsvReadState struct {
		lines [][]string
		idx   int
	}

	err = conn.RegisterCopyFunction(duckdb.CopyFunctionSpec{
		Name: "go_tsv",
		Bind: func(in duckdb.CopyFunctionBindInput) (any, error) {
			delim := "\t"
			if v, ok := in.Options["DELIM"]; ok && !v.IsNull() {
				s, err := v.Varchar()
				if err != nil {
					return nil, err
				}
				if s == "" {
					return nil, errors.New("DELIM must not be empty")
				}
				delim = s
			}
			return &tsvBind{delim: delim}, nil
		},
		GlobalInit: func(in duckdb.CopyFunctionGlobalInitInput) (any, error) {
			f, err := os.Create(in.FilePath)
			if err != nil {
				return nil, err
			}
			return &tsvWriteState{file: f}, nil
		},
		Sink: func(bd, gs any, chunk *duckdb.CopyFunctionChunk) error {
			b := bd.(*tsvBind)
			st := gs.(*tsvWriteState)
			st.mu.Lock()
			defer st.mu.Unlock()
			var rowBuf []duckdb.Value
			for r := range chunk.Len() {
				var err error
				rowBuf, err = chunk.Row(r, rowBuf)
				if err != nil {
					return err
				}
				parts := make([]string, len(rowBuf))
				for c, v := range rowBuf {
					if v.IsNull() {
						parts[c] = "\\N"
						continue
					}
					switch v.Kind() {
					case duckdb.TypeBigint:
						n, _ := v.Int64()
						parts[c] = strconv.FormatInt(n, 10)
					case duckdb.TypeVarchar:
						s, _ := v.Varchar()
						parts[c] = s
					default:
						return fmt.Errorf("unsupported go_tsv column kind %s", v.Kind())
					}
				}
				if _, err := fmt.Fprintln(st.file, strings.Join(parts, b.delim)); err != nil {
					return err
				}
			}
			return nil
		},
		Finalize: func(_, gs any) error {
			st := gs.(*tsvWriteState)
			st.mu.Lock()
			defer st.mu.Unlock()
			if st.file != nil {
				err := st.file.Close()
				st.file = nil
				return err
			}
			return nil
		},
		CopyFrom: &duckdb.TableFunctionSpec{
			NamedParameters: map[string]duckdb.LogicalType{
				"delim": duckdb.ScalarType(duckdb.TypeVarchar),
			},
			Bind: func(in duckdb.TableFunctionBindInput) (duckdb.TableFunctionBindResult, error) {
				path, err := in.Args[0].Varchar()
				if err != nil {
					return duckdb.TableFunctionBindResult{}, err
				}
				delim := "\t"
				if v, ok := in.NamedArgs["delim"]; ok && !v.IsNull() {
					delim, _ = v.Varchar()
				}
				if len(in.TargetColumns) != 2 {
					return duckdb.TableFunctionBindResult{}, fmt.Errorf("expected 2 target columns, got %d", len(in.TargetColumns))
				}
				return duckdb.TableFunctionBindResult{
					BindData: &tsvReadBind{
						path:       path,
						delim:      delim,
						targetCols: in.TargetColumns,
					},
				}, nil
			},
			Init: func(in duckdb.TableFunctionInitInput) (duckdb.TableFunctionInitResult, error) {
				b := in.BindData.(*tsvReadBind)
				f, err := os.Open(b.path)
				if err != nil {
					return duckdb.TableFunctionInitResult{}, err
				}
				defer f.Close()
				var lines [][]string
				sc := bufio.NewScanner(f)
				for sc.Scan() {
					lines = append(lines, strings.Split(sc.Text(), b.delim))
				}
				if err := sc.Err(); err != nil {
					return duckdb.TableFunctionInitResult{}, err
				}
				return duckdb.TableFunctionInitResult{InitData: &tsvReadState{lines: lines}}, nil
			},
			Execute: func(_, initData, _ any, chunk *duckdb.TableFunctionChunk) error {
				st := initData.(*tsvReadState)
				for st.idx < len(st.lines) && chunk.Len() < chunk.Cap() {
					parts := st.lines[st.idx]
					st.idx++
					idVal := duckdb.NullValue()
					if parts[0] != "\\N" {
						n, err := strconv.ParseInt(parts[0], 10, 64)
						if err != nil {
							return err
						}
						idVal = duckdb.Int64Value(n)
					}
					nameVal := duckdb.NullValue()
					if parts[1] != "\\N" {
						nameVal = duckdb.VarcharValue(parts[1])
					}
					if err := chunk.AppendRow(idVal, nameVal); err != nil {
						return err
					}
				}
				return nil
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterCopyFunction(go_tsv): %v", err)
	}

	outPath := filepath.Join(t.TempDir(), "export.tsv")
	escapedPath := strings.ReplaceAll(outPath, "'", "''")

	// 1. COPY ... TO using custom format and DELIM option.
	copyToSQL := fmt.Sprintf(
		"COPY (SELECT 1::BIGINT AS id, 'alice'::VARCHAR AS name UNION ALL SELECT 2::BIGINT, NULL::VARCHAR UNION ALL SELECT 3::BIGINT, 'charlie'::VARCHAR) TO '%s' (FORMAT go_tsv, DELIM '|')",
		escapedPath,
	)
	if _, err := conn.Exec(ctx, copyToSQL); err != nil {
		t.Fatalf("COPY TO go_tsv: %v", err)
	}

	rawBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read exported file: %v", err)
	}
	if !strings.Contains(string(rawBytes), "1|alice") || !strings.Contains(string(rawBytes), "2|\\N") {
		t.Fatalf("unexpected exported contents:\n%s", string(rawBytes))
	}

	// 2. COPY ... FROM round-trip back into a DuckDB table!
	if _, err := conn.Exec(ctx, `CREATE TABLE imported (id BIGINT, name VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	copyFromSQL := fmt.Sprintf("COPY imported FROM '%s' (FORMAT go_tsv, DELIM '|')", escapedPath)
	if _, err := conn.Exec(ctx, copyFromSQL); err != nil {
		t.Fatalf("COPY FROM go_tsv: %v", err)
	}

	_, verifyRow := querySingleRow(t, conn, `SELECT count(*)::BIGINT, sum(id)::BIGINT, count(name)::BIGINT FROM imported`)
	if cnt, _ := verifyRow[0].Int64(); cnt != 3 {
		t.Fatalf("imported count = %d, want 3", cnt)
	}
	if sum, _ := verifyRow[1].Int64(); sum != 6 {
		t.Fatalf("imported sum(id) = %d, want 6", sum)
	}
	if nonNullNames, _ := verifyRow[2].Int64(); nonNullNames != 2 {
		t.Fatalf("imported count(name) = %d, want 2 (1 NULL preserved)", nonNullNames)
	}

	// 3. Verify Bind error surfaces cleanly.
	badSQL := fmt.Sprintf("COPY (SELECT 1::BIGINT) TO '%s' (FORMAT go_tsv, DELIM '')", escapedPath)
	if _, err := conn.Exec(ctx, badSQL); err == nil || !strings.Contains(err.Error(), "DELIM must not be empty") {
		t.Fatalf("expected Bind error for empty DELIM, got %v", err)
	}

	_ = conn.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after db.Close = %d, want %d", got, baseHooks)
	}
}

func TestNativeCustomLogStorageCaptureAndCleanup(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	baseHooks := abi.HookHandleCount()

	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	var (
		mu      sync.Mutex
		entries []duckdb.LogEntry
	)
	if err := db.RegisterLogStorage("go_mem_log", func(e duckdb.LogEntry) {
		mu.Lock()
		entries = append(entries, e)
		mu.Unlock()
	}); err != nil {
		_ = db.Close()
		t.Fatalf("RegisterLogStorage: %v", err)
	}

	// Duplicate registration with the same name must fail without leaking or crashing.
	if err := db.RegisterLogStorage("go_mem_log", func(duckdb.LogEntry) {}); err == nil {
		_ = db.Close()
		t.Fatal("expected duplicate RegisterLogStorage to fail")
	}

	conn, err := db.Connect(ctx)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}

	// Enable logging routed to our custom Go log storage and run queries that emit log entries.
	if _, err := conn.Exec(ctx, `CALL enable_logging(storage = 'go_mem_log', level = 'trace')`); err != nil {
		_ = conn.Close()
		_ = db.Close()
		t.Fatalf("enable_logging: %v", err)
	}
	if _, err := conn.Exec(ctx, `SELECT 42`); err != nil {
		_ = conn.Close()
		_ = db.Close()
		t.Fatalf("SELECT 42: %v", err)
	}
	if _, err := conn.Exec(ctx, `CALL disable_logging()`); err != nil {
		_ = conn.Close()
		_ = db.Close()
		t.Fatalf("disable_logging: %v", err)
	}

	mu.Lock()
	captured := append([]duckdb.LogEntry(nil), entries...)
	mu.Unlock()

	if len(captured) == 0 {
		_ = conn.Close()
		_ = db.Close()
		t.Fatal("expected at least one log entry captured by go_mem_log")
	}
	foundQuery := false
	for _, e := range captured {
		if e.Timestamp.Value == 0 || e.Level == "" {
			t.Fatalf("unexpected log entry metadata: %+v", e)
		}
		if strings.Contains(e.Message, "SELECT 42") {
			foundQuery = true
		}
	}
	if !foundQuery {
		t.Logf("captured %d log entries (first=%+v)", len(captured), captured[0])
	}

	_ = conn.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if got := abi.HookHandleCount(); got != baseHooks {
		t.Fatalf("HookHandleCount after db.Close = %d, want %d", got, baseHooks)
	}
}
