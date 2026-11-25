package duckdbarrow_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/bububa/duckdbspike/duckdb"
	"github.com/bububa/duckdbspike/duckdbarrow"
)

func TestNativeArrowTxStmtAndScriptHelpers(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()
	db, err := lib.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	// 1. StmtSchema + QueryStmt + QueryStmtNamed.
	if _, err := conn.Exec(ctx, `CREATE TABLE arrow_tx_t (id BIGINT, name VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO arrow_tx_t VALUES (1, 'a'), (2, 'b'), (3, 'c')`); err != nil {
		t.Fatal(err)
	}

	stmt, err := conn.Prepare(ctx, `SELECT id, name FROM arrow_tx_t WHERE id >= $min ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	sc, err := duckdbarrow.StmtSchema(stmt)
	if err != nil {
		t.Fatalf("StmtSchema: %v", err)
	}
	if sc.NumFields() != 2 || sc.Field(0).Name != "id" || sc.Field(0).Type.ID() != arrow.INT64 || sc.Field(1).Name != "name" {
		t.Fatalf("StmtSchema mismatch: %v", sc)
	}

	rr, err := duckdbarrow.QueryStmtNamed(ctx, stmt, alloc, duckdb.Named("min", duckdb.Int64Value(2)))
	if err != nil {
		t.Fatalf("QueryStmtNamed: %v", err)
	}
	var gotRows int64
	for rr.Next() {
		gotRows += rr.RecordBatch().NumRows()
	}
	if err := rr.Err(); err != nil {
		rr.Release()
		t.Fatal(err)
	}
	rr.Release()
	if gotRows != 2 {
		t.Fatalf("QueryStmtNamed rows = %d, want 2", gotRows)
	}

	// 2. Tx-scoped RegisterRecordBatch, RegisterRecordReader, Query, QueryScript.
	tx, err := conn.BeginTx(ctx, duckdb.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	inSchema := arrow.NewSchema([]arrow.Field{
		{Name: "k", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "v", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	b := array.NewRecordBuilder(alloc, inSchema)
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{10, 20}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"x", "y"}, nil)
	rec := b.NewRecordBatch()
	b.Release()
	defer rec.Release()

	view, err := duckdbarrow.RegisterRecordBatch(ctx, tx, "tx_arrow_v", rec)
	if err != nil {
		t.Fatalf("RegisterRecordBatch on Tx: %v", err)
	}
	txRR, err := duckdbarrow.Query(ctx, tx, alloc, `SELECT k, v FROM tx_arrow_v ORDER BY k`)
	if err != nil {
		_ = view.Close()
		t.Fatalf("duckdbarrow.Query on Tx: %v", err)
	}
	var txRows int64
	for txRR.Next() {
		txRows += txRR.RecordBatch().NumRows()
	}
	if err := txRR.Err(); err != nil {
		txRR.Release()
		_ = view.Close()
		t.Fatal(err)
	}
	txRR.Release()
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if txRows != 2 {
		t.Fatalf("tx arrow view rows = %d, want 2", txRows)
	}

	// RegisterRecordReader on Tx + QueryScript on Tx.
	memReader, err := array.NewRecordReader(inSchema, []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatal(err)
	}
	streamView, err := duckdbarrow.RegisterRecordReader(ctx, tx, "tx_stream_v", memReader)
	memReader.Release()
	if err != nil {
		t.Fatalf("RegisterRecordReader on Tx: %v", err)
	}
	scrRR, err := duckdbarrow.QueryScript(ctx, tx, alloc, `
		CREATE TEMP TABLE tx_copy AS SELECT k, v FROM tx_stream_v;
		SELECT k, v FROM tx_copy WHERE k >= ? ORDER BY k;
	`, duckdb.Int32Value(15))
	if err != nil {
		_ = streamView.Close()
		t.Fatalf("QueryScript on Tx: %v", err)
	}
	var scrRows int64
	for scrRR.Next() {
		scrRows += scrRR.RecordBatch().NumRows()
	}
	if err := scrRR.Err(); err != nil {
		scrRR.Release()
		_ = streamView.Close()
		t.Fatal(err)
	}
	scrRR.Release()
	if err := streamView.Close(); err != nil {
		t.Fatal(err)
	}
	if scrRows != 1 {
		t.Fatalf("tx QueryScript rows = %d, want 1", scrRows)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// 3. Multi-result-set ScriptRecordReader via QueryScript & QueryScriptNamed.
	multiRR, err := duckdbarrow.QueryScript(ctx, conn, alloc, `
		CREATE TEMP TABLE arrow_mrs (id INT, label VARCHAR);
		INSERT INTO arrow_mrs VALUES (1, 'one'), (2, 'two');
		SELECT id FROM arrow_mrs WHERE id >= ? ORDER BY id;
		SELECT label FROM arrow_mrs WHERE id = ?;
		INSERT INTO arrow_mrs VALUES (3, 'three');
	`, duckdb.Int32Value(1), duckdb.Int32Value(2))
	if err != nil {
		t.Fatalf("QueryScript multi: %v", err)
	}
	if multiRR.Schema().NumFields() != 1 || multiRR.Schema().Field(0).Name != "id" || multiRR.Schema().Field(0).Type.ID() != arrow.INT32 {
		multiRR.Release()
		t.Fatalf("RS1 schema = %v, want [id:int32]", multiRR.Schema())
	}
	var gotIDs []int32
	for multiRR.Next() {
		col := multiRR.RecordBatch().Column(0).(*array.Int32)
		for i := range col.Len() {
			gotIDs = append(gotIDs, col.Value(i))
		}
	}
	if err := multiRR.Err(); err != nil {
		multiRR.Release()
		t.Fatalf("RS1 err: %v", err)
	}
	if len(gotIDs) != 2 || gotIDs[0] != 1 || gotIDs[1] != 2 {
		multiRR.Release()
		t.Fatalf("RS1 ids = %v, want [1 2]", gotIDs)
	}
	if !multiRR.HasNextResultSet() {
		multiRR.Release()
		t.Fatal("expected HasNextResultSet == true after RS1")
	}
	if !multiRR.NextResultSet() {
		err := multiRR.Err()
		multiRR.Release()
		t.Fatalf("NextResultSet to RS2 returned false (err=%v)", err)
	}
	if multiRR.Schema().NumFields() != 1 || multiRR.Schema().Field(0).Name != "label" || multiRR.Schema().Field(0).Type.ID() != arrow.STRING {
		multiRR.Release()
		t.Fatalf("RS2 schema = %v, want [label:utf8]", multiRR.Schema())
	}
	var gotLabels []string
	for multiRR.Next() {
		col := multiRR.RecordBatch().Column(0).(*array.String)
		for i := range col.Len() {
			gotLabels = append(gotLabels, col.Value(i))
		}
	}
	if err := multiRR.Err(); err != nil {
		multiRR.Release()
		t.Fatalf("RS2 err: %v", err)
	}
	if len(gotLabels) != 1 || gotLabels[0] != "two" {
		multiRR.Release()
		t.Fatalf("RS2 labels = %v, want [two]", gotLabels)
	}
	// Advancing past RS2 runs trailing INSERT and returns false cleanly.
	if multiRR.NextResultSet() {
		multiRR.Release()
		t.Fatal("expected NextResultSet == false after trailing INSERT")
	}
	if err := multiRR.Err(); err != nil {
		multiRR.Release()
		t.Fatalf("trailing INSERT err: %v", err)
	}
	multiRR.Release()

	// Verify trailing INSERT executed via QueryScriptNamed multi-result-set.
	namedRR, err := duckdbarrow.QueryScriptNamed(ctx, conn, alloc, `
		SELECT count(*)::BIGINT AS cnt FROM arrow_mrs;
		SELECT label FROM arrow_mrs WHERE id = $target;
	`, duckdb.Named("target", duckdb.Int32Value(3)))
	if err != nil {
		t.Fatalf("QueryScriptNamed multi: %v", err)
	}
	defer namedRR.Release()
	if !namedRR.Next() || namedRR.RecordBatch().Column(0).(*array.Int64).Value(0) != 3 {
		t.Fatalf("expected count=3 after trailing INSERT (err=%v)", namedRR.Err())
	}
	if !namedRR.NextResultSet() || !namedRR.Next() || namedRR.RecordBatch().Column(0).(*array.String).Value(0) != "three" {
		t.Fatalf("expected RS2 label='three' (err=%v)", namedRR.Err())
	}
}
