package duckdb_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

func TestNativeMetadataAPIs(t *testing.T) {
	lib := loadNativeLibrary(t)
	ctx := context.Background()

	// 1. Library.ConfigFlags
	flags, err := lib.ConfigFlags()
	if err != nil {
		t.Fatalf("ConfigFlags: %v", err)
	}
	if len(flags) == 0 {
		t.Fatal("expected non-empty ConfigFlags")
	}
	seen := make(map[string]string, len(flags))
	for _, f := range flags {
		seen[f.Name] = f.Description
	}
	if _, ok := seen["memory_limit"]; !ok {
		t.Fatal("ConfigFlags missing memory_limit")
	}
	if _, ok := seen["threads"]; !ok {
		t.Fatal("ConfigFlags missing threads")
	}

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

	// 2. Stmt.StatementType & Stmt.ParamTypes
	if _, err := conn.Exec(ctx, `CREATE TABLE accounts (
		id BIGINT PRIMARY KEY,
		balance DECIMAL(12, 2) NOT NULL DEFAULT 0.00,
		label VARCHAR DEFAULT 'standard'
	)`); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		sql  string
		want duckdb.StatementType
	}{
		{"SELECT $1::DECIMAL(12, 2), $2::INTEGER, ?", duckdb.StatementTypeSelect},
		{"INSERT INTO accounts (id, balance) VALUES (?, ?)", duckdb.StatementTypeInsert},
		{"UPDATE accounts SET label = ? WHERE id = ?", duckdb.StatementTypeUpdate},
		{"DELETE FROM accounts WHERE id = ?", duckdb.StatementTypeDelete},
	}
	for _, tc := range cases {
		stmt, err := conn.Prepare(ctx, tc.sql)
		if err != nil {
			t.Fatalf("Prepare(%q): %v", tc.sql, err)
		}
		got, err := stmt.StatementType()
		if err != nil {
			_ = stmt.Close()
			t.Fatalf("StatementType(%q): %v", tc.sql, err)
		}
		if got != tc.want {
			_ = stmt.Close()
			t.Fatalf("StatementType(%q) = %s, want %s", tc.sql, got, tc.want)
		}
		if tc.want == duckdb.StatementTypeSelect {
			ptypes, err := stmt.ParamTypes()
			if err != nil {
				_ = stmt.Close()
				t.Fatalf("ParamTypes: %v", err)
			}
			if len(ptypes) != 3 {
				_ = stmt.Close()
				t.Fatalf("len(ParamTypes) = %d, want 3", len(ptypes))
			}
			if ptypes[0].Kind != duckdb.TypeDecimal || ptypes[0].DecimalWidth != 12 || ptypes[0].DecimalScale != 2 {
				_ = stmt.Close()
				t.Fatalf("param 0 = %+v, want DECIMAL(12,2)", ptypes[0])
			}
			if ptypes[1].Kind != duckdb.TypeInteger {
				_ = stmt.Close()
				t.Fatalf("param 1 = %+v, want INTEGER", ptypes[1])
			}
			if ptypes[2].Kind != duckdb.TypeInvalid && ptypes[2].Kind != duckdb.TypeSQLNull {
				_ = stmt.Close()
				t.Fatalf("param 2 = %+v, want TypeInvalid/SQLNull", ptypes[2])
			}
		}
		if err := stmt.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// 3. Conn.DescribeTable (success & missing table error)
	cols, err := conn.DescribeTable(ctx, "", "", "accounts")
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}
	if len(cols) != 3 {
		t.Fatalf("DescribeTable len = %d, want 3", len(cols))
	}
	if cols[0].Name != "id" || cols[0].Type.Kind != duckdb.TypeBigint || cols[0].HasDefault {
		t.Fatalf("col 0 = %+v", cols[0])
	}
	if cols[1].Name != "balance" || cols[1].Type.Kind != duckdb.TypeDecimal || cols[1].Type.DecimalWidth != 12 || cols[1].Type.DecimalScale != 2 || !cols[1].HasDefault {
		t.Fatalf("col 1 = %+v", cols[1])
	}
	if cols[2].Name != "label" || cols[2].Type.Kind != duckdb.TypeVarchar || !cols[2].HasDefault {
		t.Fatalf("col 2 = %+v", cols[2])
	}

	_, err = conn.DescribeTable(ctx, "", "", "no_such_table")
	var nativeErr *duckdb.Error
	if !errors.As(err, &nativeErr) || nativeErr.Kind != duckdb.ErrorKindCatalog {
		t.Fatalf("DescribeTable missing = %v, want ErrorKindCatalog", err)
	}
}
