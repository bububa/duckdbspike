package duckdb_test

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bububa/duckdbspike/duckdb"
)

type nativeRoundTripFunc func(*http.Request) (*http.Response, error)

func (f nativeRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestNativeAutoLoadDiscoveryAndDownload(t *testing.T) {
	libPath := os.Getenv("DUCKDB_LIB_PATH")
	shaHex := os.Getenv("DUCKDB_LIB_SHA256")
	if libPath == "" || shaHex == "" {
		t.Skip("DUCKDB_LIB_PATH and DUCKDB_LIB_SHA256 are required")
	}

	ctx := context.Background()

	// 1. Offline discovery via WithSearchPaths finds the installed library and executes SQL.
	lib1, err := duckdb.AutoLoad(
		ctx,
		t.TempDir(),
		duckdb.WithSearchPaths(filepath.Dir(libPath)),
		duckdb.WithSkipSystemPaths(true),
		duckdb.WithDisableDownload(true),
		duckdb.WithExpectedSHA256(shaHex),
	)
	if err != nil {
		t.Fatalf("AutoLoad(discovery): %v", err)
	}
	db1, err := lib1.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "1"})
	if err != nil {
		t.Fatal(err)
	}
	c1, err := db1.Connect(ctx)
	if err != nil {
		_ = db1.Close()
		t.Fatal(err)
	}
	_, row := querySingleRow(t, c1, "SELECT 42::BIGINT")
	if v, _ := row[0].Int64(); v != 42 {
		t.Fatalf("AutoLoad discovery SELECT 42 = %d", v)
	}
	_ = c1.Close()
	_ = db1.Close()

	// 2. End-to-end download + zip extraction + SHA-256 verification + real Load
	//    served from an in-memory HTTP RoundTripper (zero external network needed).
	rawLib, err := os.ReadFile(libPath)
	if err != nil {
		t.Fatal(err)
	}
	var zipBuf bytes.Buffer
	zw := zip.NewWriter(&zipBuf)
	w, err := zw.Create(filepath.Base(libPath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(rawLib); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	zipData := zipBuf.Bytes()

	var downloads atomic.Int32
	client := &http.Client{
		Transport: nativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
			downloads.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(bytes.NewReader(zipData)),
			}, nil
		}),
	}

	dlDir := t.TempDir()
	lib2, err := duckdb.AutoLoad(
		ctx,
		dlDir,
		duckdb.WithHTTPClient(client),
		duckdb.WithDownloadBaseURL("https://local-test.invalid/duckdb"),
		duckdb.WithSkipSystemPaths(true),
		duckdb.WithExpectedSHA256(shaHex),
	)
	if err != nil {
		t.Fatalf("AutoLoad(download): %v", err)
	}
	if downloads.Load() != 1 {
		t.Fatalf("expected 1 download, got %d", downloads.Load())
	}

	// Second call reuses the downloaded cache without hitting HTTP again.
	if _, err := duckdb.AutoLoad(
		ctx,
		dlDir,
		duckdb.WithHTTPClient(client),
		duckdb.WithDownloadBaseURL("https://local-test.invalid/duckdb"),
		duckdb.WithSkipSystemPaths(true),
		duckdb.WithExpectedSHA256(shaHex),
	); err != nil {
		t.Fatalf("AutoLoad(cached): %v", err)
	}
	if downloads.Load() != 1 {
		t.Fatalf("expected cache hit (1 total download), got %d", downloads.Load())
	}

	db2, err := lib2.Open(ctx, ":memory:", duckdb.Option{Name: "threads", Value: "1"})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	c2, err := db2.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	_, row2 := querySingleRow(t, c2, "SELECT 'autoload-ok'")
	if s, _ := row2[0].Varchar(); s != "autoload-ok" {
		t.Fatalf("AutoLoad downloaded query = %q, want autoload-ok", s)
	}
}
