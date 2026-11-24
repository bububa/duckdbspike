package duckdb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"unsafe"

	"github.com/bububa/duckdbspike/internal/abi"
)

func TestP4bVFSFileAndCatalogMockZeroLeaks(t *testing.T) {
	var (
		clientCtxAlive int
		fsAlive        int
		optsAlive      int
		fhAlive        int
		catAlive       int
		entryAlive     int
		sqlPrepared    []string
		buf            []byte
		pos            int64
		synced         bool
	)
	typeNameBuf := append([]byte("duckdb"), 0)
	entryNameBuf := append([]byte("orders"), 0)

	api := &abi.API{
		CreateConfig:  func(out *uintptr) int32 { *out = 1; return abi.StateSuccess },
		DestroyConfig: func(cfg *uintptr) { *cfg = 0 },
		OpenExt: func(path *byte, outDB *uintptr, cfg uintptr, outErr *uintptr) int32 {
			*outDB = 1
			return abi.StateSuccess
		},
		Close:      func(db *uintptr) { *db = 0 },
		Connect:    func(db uintptr, outConn *uintptr) int32 { *outConn = 2; return abi.StateSuccess },
		Disconnect: func(conn *uintptr) { *conn = 0 },
		Interrupt:  func(conn uintptr) {},
		ConnectionGetClientContext: func(conn uintptr, out *uintptr) {
			clientCtxAlive++
			*out = 50
		},
		DestroyClientContext: func(cc *uintptr) {
			if *cc != 0 {
				clientCtxAlive--
				*cc = 0
			}
		},
		ClientContextGetFileSystem: func(cc uintptr) uintptr {
			fsAlive++
			return 60
		},
		DestroyFileSystem: func(fs *uintptr) {
			if *fs != 0 {
				fsAlive--
				*fs = 0
			}
		},
		CreateFileOpenOptions: func() uintptr {
			optsAlive++
			return 70
		},
		FileOpenOptionsSetFlag: func(opts uintptr, flag int32, value bool) int32 { return abi.StateSuccess },
		DestroyFileOpenOptions: func(opts *uintptr) {
			if *opts != 0 {
				optsAlive--
				*opts = 0
			}
		},
		FileSystemOpen: func(fs uintptr, path *byte, opts uintptr, outFile *uintptr) int32 {
			fhAlive++
			*outFile = 80
			return abi.StateSuccess
		},
		FileHandleWrite: func(fh uintptr, p *byte, size int64) int64 {
			src := unsafe.Slice(p, int(size))
			end := int(pos) + len(src)
			if end > len(buf) {
				grown := make([]byte, end)
				copy(grown, buf)
				buf = grown
			}
			copy(buf[pos:], src)
			pos += size
			return size
		},
		FileHandleRead: func(fh uintptr, p *byte, size int64) int64 {
			if pos >= int64(len(buf)) {
				return 0
			}
			dst := unsafe.Slice(p, int(size))
			n := copy(dst, buf[pos:])
			pos += int64(n)
			return int64(n)
		},
		FileHandleTell: func(fh uintptr) int64 { return pos },
		FileHandleSize: func(fh uintptr) int64 { return int64(len(buf)) },
		FileHandleSeek: func(fh uintptr, position int64) int32 {
			pos = position
			return abi.StateSuccess
		},
		FileHandleSync:  func(fh uintptr) int32 { synced = true; return abi.StateSuccess },
		FileHandleClose: func(fh uintptr) int32 { return abi.StateSuccess },
		DestroyFileHandle: func(fh *uintptr) {
			if *fh != 0 {
				fhAlive--
				*fh = 0
			}
		},
		ClientContextGetCatalog: func(cc uintptr, name *byte) uintptr {
			if abi.GoString(uintptr(unsafe.Pointer(name))) == "missing" {
				return 0
			}
			catAlive++
			return 90
		},
		CatalogGetTypeName: func(cat uintptr) uintptr {
			return uintptr(unsafe.Pointer(&typeNameBuf[0]))
		},
		CatalogGetEntry: func(cat uintptr, cc uintptr, et int32, schema *byte, name *byte) uintptr {
			if abi.GoString(uintptr(unsafe.Pointer(name))) != "orders" {
				return 0
			}
			entryAlive++
			return 91
		},
		CatalogEntryGetType: func(entry uintptr) int32 { return abi.CatalogEntryTypeTable },
		CatalogEntryGetName: func(entry uintptr) uintptr {
			return uintptr(unsafe.Pointer(&entryNameBuf[0]))
		},
		DestroyCatalog: func(cat *uintptr) {
			if *cat != 0 {
				catAlive--
				*cat = 0
			}
		},
		DestroyCatalogEntry: func(entry *uintptr) {
			if *entry != 0 {
				entryAlive--
				*entry = 0
			}
		},
		Prepare: func(conn uintptr, q *byte, outStmt *uintptr) int32 {
			sqlPrepared = append(sqlPrepared, abi.GoString(uintptr(unsafe.Pointer(q))))
			*outStmt = 100
			return abi.StateSuccess
		},
		DestroyPrepare: func(stmt *uintptr) { *stmt = 0 },
		NParams:        func(stmt uintptr) uint64 { return 0 },
		PendingPreparedStreaming: func(stmt uintptr, out *uintptr) int32 {
			*out = 101
			return abi.StateSuccess
		},
		PendingExecuteTask: func(p uintptr) int32 { return abi.PendingResultReady },
		ExecutePending:     func(p uintptr, res *abi.Result) int32 { return abi.StateSuccess },
		DestroyPending:     func(p *uintptr) { *p = 0 },
		RowsChanged:        func(res *abi.Result) uint64 { return 0 },
		DestroyResult:      func(res *abi.Result) {},
	}

	lib := &Library{api: api}
	ctx := context.Background()
	db, err := lib.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// 1. VFS File Write/Sync/Tell/Size/Seek/Read/EOF/Close lifecycle.
	if _, err := conn.OpenFile(ctx, "/tmp/x", FileOpenOptions{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty FileOpenOptions = %v, want ErrInvalidInput", err)
	}
	f, err := conn.OpenFile(ctx, "/tmp/demo.txt", FileOpenOptions{Read: true, Write: true, Create: true})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if n, err := f.Write([]byte("hello vfs")); err != nil || n != 9 {
		t.Fatalf("Write = (%d, %v)", n, err)
	}
	if err := f.Sync(); err != nil || !synced {
		t.Fatalf("Sync = %v (synced=%v)", err, synced)
	}
	if sz, err := f.Size(); err != nil || sz != 9 {
		t.Fatalf("Size = (%d, %v)", sz, err)
	}
	if p, err := f.Tell(); err != nil || p != 9 {
		t.Fatalf("Tell = (%d, %v)", p, err)
	}
	if p, err := f.Seek(-3, io.SeekEnd); err != nil || p != 6 {
		t.Fatalf("SeekEnd = (%d, %v)", p, err)
	}
	tail, err := io.ReadAll(f)
	if err != nil || !bytes.Equal(tail, []byte("vfs")) {
		t.Fatalf("ReadAll tail = (%q, %v)", tail, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("idempotent Close: %v", err)
	}
	if _, err := f.Read(make([]byte, 4)); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close Read = %v, want ErrClosed", err)
	}

	// Unclosed file must be cleaned up by Conn.Close().
	if _, err := conn.OpenFile(ctx, "/tmp/leak_check.txt", FileOpenOptions{Read: true}); err != nil {
		t.Fatal(err)
	}

	// 2. Catalog API on autocommit Conn (auto BEGIN READ ONLY / ROLLBACK).
	if _, _, err := conn.CatalogTypeName(ctx, ""); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty catalogName = %v, want ErrInvalidInput", err)
	}
	typName, ok, err := conn.CatalogTypeName(ctx, "memory")
	if err != nil || !ok || typName != "duckdb" {
		t.Fatalf("CatalogTypeName = (%q, %v, %v)", typName, ok, err)
	}
	if len(sqlPrepared) != 2 || sqlPrepared[0] != "BEGIN TRANSACTION READ ONLY" || sqlPrepared[1] != "ROLLBACK" {
		t.Fatalf("expected auto read-only tx wrapper, got %v", sqlPrepared)
	}

	// 3. Catalog API inside active Tx -> ZERO SQL executed.
	sqlPrepared = nil
	tx, err := conn.BeginTx(ctx, TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sqlPrepared = nil
	entry, ok, err := tx.LookupCatalogEntry(ctx, "memory", CatalogEntryTable, "main", "orders")
	if err != nil || !ok || entry.Name != "orders" || entry.Type != CatalogEntryTable {
		t.Fatalf("LookupCatalogEntry = (%+v, %v, %v)", entry, ok, err)
	}
	if len(sqlPrepared) != 0 {
		t.Fatalf("tx.LookupCatalogEntry executed unexpected SQL: %v", sqlPrepared)
	}
	if _, ok, err := tx.LookupCatalogEntry(ctx, "memory", CatalogEntryTable, "main", "nope"); err != nil || ok {
		t.Fatalf("missing entry = (%v, %v)", ok, err)
	}
	if _, ok, err := tx.CatalogTypeName(ctx, "missing"); err != nil || ok {
		t.Fatalf("missing catalog = (%v, %v)", ok, err)
	}
	_ = tx.Rollback(ctx)

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if clientCtxAlive != 0 || fsAlive != 0 || optsAlive != 0 || fhAlive != 0 || catAlive != 0 || entryAlive != 0 {
		t.Fatalf("leaked handles: cc=%d fs=%d opts=%d fh=%d cat=%d entry=%d", clientCtxAlive, fsAlive, optsAlive, fhAlive, catAlive, entryAlive)
	}
}
