package abi

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"unsafe"
)

func TestSymbolsCoverEveryAPIField(t *testing.T) {
	var api API
	syms := api.symbols()
	rt := reflect.TypeOf(api)
	if len(syms) != rt.NumField() {
		t.Fatalf("symbols() lists %d entries, API struct has %d fields", len(syms), rt.NumField())
	}
	seenName := make(map[string]struct{}, len(syms))
	seenAddr := make(map[uintptr]struct{}, len(syms))
	base := reflect.ValueOf(&api).Elem()
	for _, s := range syms {
		if _, dup := seenName[s.name]; dup {
			t.Fatalf("duplicate symbol name %q", s.name)
		}
		seenName[s.name] = struct{}{}
		ptr := reflect.ValueOf(s.field).Pointer()
		if _, dup := seenAddr[ptr]; dup {
			t.Fatalf("duplicate API field pointer for %q", s.name)
		}
		seenAddr[ptr] = struct{}{}
	}
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Type.Kind() != reflect.Func {
			t.Fatalf("API field %s has kind %s, want func", f.Name, f.Type.Kind())
		}
		addr := base.Field(i).Addr().Pointer()
		if _, ok := seenAddr[addr]; !ok {
			t.Fatalf("API field %s is not listed in symbols()", f.Name)
		}
	}
}

func TestResolveRejectsMissingWithoutPartialPublish(t *testing.T) {
	var api API
	err := api.resolve(func(name string) (uintptr, error) {
		if name == "duckdb_fetch_chunk" || name == "duckdb_interrupt" {
			return 0, errors.New("not found")
		}
		return 0x1000, nil
	})
	if !errors.Is(err, ErrMissingSymbol) {
		t.Fatalf("got err=%v, want ErrMissingSymbol", err)
	}
	if !strings.Contains(err.Error(), "duckdb_fetch_chunk") || !strings.Contains(err.Error(), "duckdb_interrupt") {
		t.Fatalf("error %q should list all missing symbols", err)
	}
	v := reflect.ValueOf(api)
	for i := 0; i < v.NumField(); i++ {
		if !v.Field(i).IsNil() {
			t.Fatalf("API field %s was partially published on resolve failure", v.Type().Field(i).Name)
		}
	}
}

func TestVerifyVersionRejectsMismatch(t *testing.T) {
	bad := append([]byte("v1.5.5"), 0)
	good := append([]byte(PinnedVersion), 0)
	if err := verifyVersion(&API{LibraryVersion: func() uintptr { return uintptr(unsafe.Pointer(&bad[0])) }}); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("got %v, want ErrVersionMismatch", err)
	}
	if err := verifyVersion(&API{LibraryVersion: func() uintptr { return 0 }}); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("empty version: got %v, want ErrVersionMismatch", err)
	}
	if err := verifyVersion(&API{LibraryVersion: func() uintptr { return uintptr(unsafe.Pointer(&good[0])) }}); err != nil {
		t.Fatalf("pinned version rejected: %v", err)
	}
}

func TestCStringAndGoString(t *testing.T) {
	if got := GoString(0); got != "" {
		t.Fatalf("GoString(0) = %q, want empty", got)
	}
	b, err := CString("hello")
	if err != nil {
		t.Fatalf("CString failed: %v", err)
	}
	if len(b) != 6 || b[5] != 0 || string(b[:5]) != "hello" {
		t.Fatalf("unexpected CString bytes: %v", b)
	}
	if got := GoString(uintptr(unsafe.Pointer(&b[0]))); got != "hello" {
		t.Fatalf("GoString round-trip = %q, want hello", got)
	}
	if _, err := CString("a\x00b"); err == nil {
		t.Fatal("expected error for interior NUL")
	}
}

func TestLoadPathAndChecksumGuards(t *testing.T) {
	if !platformAccepted() {
		_, err := Load("/nonexistent", strings.Repeat("0", 64))
		if !errors.Is(err, ErrUnsupportedPlatform) {
			t.Fatalf("got %v, want ErrUnsupportedPlatform", err)
		}
		return
	}

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	goodFile := filepath.Join(dir, "fake.dylib")
	payload := []byte("not a real dylib")
	if err := os.WriteFile(goodFile, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	sumBytes := sha256.Sum256(payload)
	goodSum := hex.EncodeToString(sumBytes[:])

	// Malformed checksums.
	for _, bad := range []string{"", "abc", strings.ToUpper(goodSum), strings.Repeat("g", 64)} {
		if _, err := Load(goodFile, bad); !errors.Is(err, ErrChecksumMismatch) {
			t.Fatalf("bad sha %q: got %v, want ErrChecksumMismatch", bad, err)
		}
	}

	// Relative path and NUL in path.
	if _, err := Load("relative/libduckdb.dylib", goodSum); !errors.Is(err, ErrUnsafeLibraryPath) {
		t.Fatalf("relative path: got %v, want ErrUnsafeLibraryPath", err)
	}
	if _, err := Load(goodFile+"\x00x", goodSum); !errors.Is(err, ErrUnsafeLibraryPath) {
		t.Fatalf("NUL path: got %v, want ErrUnsafeLibraryPath", err)
	}
	if _, err := Load(dir, goodSum); !errors.Is(err, ErrUnsafeLibraryPath) {
		t.Fatalf("directory path: got %v, want ErrUnsafeLibraryPath", err)
	}
	if _, err := Load(filepath.Join(dir, "missing.dylib"), goodSum); !errors.Is(err, ErrUnsafeLibraryPath) {
		t.Fatalf("missing path: got %v, want ErrUnsafeLibraryPath", err)
	}

	if runtime.GOOS == "windows" {
		// UNC/network paths must be rejected.
		if _, err := Load(`\\server\share\duckdb.dll`, goodSum); !errors.Is(err, ErrUnsafeLibraryPath) {
			t.Fatalf("UNC path: got %v, want ErrUnsafeLibraryPath", err)
		}
	} else {
		// Group-writable file rejected, including through a symlink.
		writableFile := filepath.Join(dir, "writable.dylib")
		if err := os.WriteFile(writableFile, payload, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(writableFile, 0o620); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(writableFile, goodSum); !errors.Is(err, ErrUnsafeLibraryPath) {
			t.Fatalf("group-writable file: got %v, want ErrUnsafeLibraryPath", err)
		}
		linkPath := filepath.Join(dir, "link.dylib")
		if err := os.Symlink(writableFile, linkPath); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(linkPath, goodSum); !errors.Is(err, ErrUnsafeLibraryPath) {
			t.Fatalf("symlink to group-writable file: got %v, want ErrUnsafeLibraryPath", err)
		}

		// Group-writable parent directory rejected.
		writableDir := filepath.Join(dir, "wdir")
		if err := os.Mkdir(writableDir, 0o700); err != nil {
			t.Fatal(err)
		}
		fileInWritableDir := filepath.Join(writableDir, "lib.dylib")
		if err := os.WriteFile(fileInWritableDir, payload, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(writableDir, 0o720); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(fileInWritableDir, goodSum); !errors.Is(err, ErrUnsafeLibraryPath) {
			t.Fatalf("group-writable dir: got %v, want ErrUnsafeLibraryPath", err)
		}
	}

	// Wrong checksum rejected before dlopen.
	wrongSum := strings.Repeat("0", 64)
	if wrongSum == goodSum {
		wrongSum = strings.Repeat("1", 64)
	}
	if _, err := Load(goodFile, wrongSum); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("wrong checksum: got %v, want ErrChecksumMismatch", err)
	}

	// Matching checksum on a non-dylib / wrong-arch binary fails at dlopen.
	if _, err := Load(goodFile, goodSum); !errors.Is(err, ErrLibraryOpen) {
		t.Fatalf("invalid dylib: got %v, want ErrLibraryOpen", err)
	}
}

func findCCompiler() (string, error) {
	if envCC := os.Getenv("CC"); envCC != "" {
		return exec.LookPath(envCC)
	}
	for _, name := range []string{"cc", "gcc", "clang"} {
		if path, err := exec.LookPath(name); err == nil {
			return path, nil
		}
	}
	return "", errors.New("none of cc, gcc, or clang found in PATH")
}

func TestNativeABILayoutMatchesHeader(t *testing.T) {
	headerDir := os.Getenv("DUCKDB_HEADER_DIR")
	if headerDir == "" {
		t.Skip("DUCKDB_HEADER_DIR not set; skipping C header ABI layout check")
	}
	cc, err := findCCompiler()
	if err != nil {
		t.Skipf("C compiler not found: %v", err)
	}
	probeSrc := `#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include "duckdb.h"
struct ArrowSchema {
	const char *format;
	const char *name;
	const char *metadata;
	int64_t flags;
	int64_t n_children;
	struct ArrowSchema **children;
	struct ArrowSchema *dictionary;
	void (*release)(struct ArrowSchema *);
	void *private_data;
};
struct ArrowArray {
	int64_t length;
	int64_t null_count;
	int64_t offset;
	int64_t n_buffers;
	int64_t n_children;
	const void **buffers;
	struct ArrowArray **children;
	struct ArrowArray *dictionary;
	void (*release)(struct ArrowArray *);
	void *private_data;
};
#define S(t) printf("size %s %zu %zu\n", #t, sizeof(t), _Alignof(t))
#define O(t, f) printf("off %s.%s %zu\n", #t, #f, offsetof(t, f))
int main(void) {
	S(duckdb_result);
	O(duckdb_result, deprecated_error_message);
	O(duckdb_result, internal_data);
	S(duckdb_hugeint);
	S(duckdb_uhugeint);
	S(duckdb_decimal);
	O(duckdb_decimal, value);
	S(duckdb_string_t);
	O(duckdb_string_t, value.pointer.ptr);
	O(duckdb_string_t, value.inlined.inlined);
	S(duckdb_list_entry);
	S(duckdb_interval);
	S(duckdb_date);
	S(duckdb_time);
	S(duckdb_time_ns);
	S(duckdb_time_tz);
	S(duckdb_timestamp);
	S(duckdb_timestamp_s);
	S(duckdb_timestamp_ms);
	S(duckdb_timestamp_ns);
	S(duckdb_query_progress_type);
	O(duckdb_query_progress_type, percentage);
	O(duckdb_query_progress_type, rows_processed);
	O(duckdb_query_progress_type, total_rows_to_process);
	S(struct ArrowSchema);
	O(struct ArrowSchema, release);
	O(struct ArrowSchema, private_data);
	S(struct ArrowArray);
	O(struct ArrowArray, release);
	O(struct ArrowArray, private_data);
	return 0;
}
`
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "probe.c")
	binPath := filepath.Join(tmp, "probe")
	if runtime.GOOS == "windows" {
		binPath += ".exe"
	}
	if err := os.WriteFile(srcPath, []byte(probeSrc), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(cc, "-std=c11", "-I", headerDir, "-o", binPath, srcPath).CombinedOutput(); err != nil {
		t.Fatalf("compile ABI probe: %v\n%s", err, out)
	}
	out, err := exec.Command(binPath).Output()
	if err != nil {
		t.Fatalf("run ABI probe: %v", err)
	}
	got := strings.ReplaceAll(string(out), "\r\n", "\n")
	want := strings.Join([]string{
		fmt.Sprintf("size duckdb_result %d %d", unsafe.Sizeof(Result{}), unsafe.Alignof(Result{})),
		fmt.Sprintf("off duckdb_result.deprecated_error_message %d", unsafe.Offsetof(Result{}.DeprecatedErrorMessage)),
		fmt.Sprintf("off duckdb_result.internal_data %d", unsafe.Offsetof(Result{}.InternalData)),
		fmt.Sprintf("size duckdb_hugeint %d %d", unsafe.Sizeof(Hugeint{}), unsafe.Alignof(Hugeint{})),
		fmt.Sprintf("size duckdb_uhugeint %d %d", unsafe.Sizeof(Uhugeint{}), unsafe.Alignof(Uhugeint{})),
		fmt.Sprintf("size duckdb_decimal %d %d", unsafe.Sizeof(Decimal{}), unsafe.Alignof(Decimal{})),
		fmt.Sprintf("off duckdb_decimal.value %d", unsafe.Offsetof(Decimal{}.Value)),
		fmt.Sprintf("size duckdb_string_t %d %d", unsafe.Sizeof(StringT{}), unsafe.Alignof(StringT{})),
		fmt.Sprintf("off duckdb_string_t.value.pointer.ptr %d", unsafe.Offsetof(StringT{}.Ptr)),
		fmt.Sprintf("off duckdb_string_t.value.inlined.inlined %d", unsafe.Offsetof(StringT{}.Prefix)),
		fmt.Sprintf("size duckdb_list_entry %d %d", unsafe.Sizeof(ListEntry{}), unsafe.Alignof(ListEntry{})),
		fmt.Sprintf("size duckdb_interval %d %d", unsafe.Sizeof(Interval{}), unsafe.Alignof(Interval{})),
		fmt.Sprintf("size duckdb_date %d %d", unsafe.Sizeof(Date{}), unsafe.Alignof(Date{})),
		fmt.Sprintf("size duckdb_time %d %d", unsafe.Sizeof(Time{}), unsafe.Alignof(Time{})),
		fmt.Sprintf("size duckdb_time_ns %d %d", unsafe.Sizeof(TimeNS{}), unsafe.Alignof(TimeNS{})),
		fmt.Sprintf("size duckdb_time_tz %d %d", unsafe.Sizeof(TimeTZ{}), unsafe.Alignof(TimeTZ{})),
		fmt.Sprintf("size duckdb_timestamp %d %d", unsafe.Sizeof(Timestamp{}), unsafe.Alignof(Timestamp{})),
		fmt.Sprintf("size duckdb_timestamp_s %d %d", unsafe.Sizeof(TimestampS{}), unsafe.Alignof(TimestampS{})),
		fmt.Sprintf("size duckdb_timestamp_ms %d %d", unsafe.Sizeof(TimestampMS{}), unsafe.Alignof(TimestampMS{})),
		fmt.Sprintf("size duckdb_timestamp_ns %d %d", unsafe.Sizeof(TimestampNS{}), unsafe.Alignof(TimestampNS{})),
		fmt.Sprintf("size duckdb_query_progress_type %d %d", unsafe.Sizeof(QueryProgress{}), unsafe.Alignof(QueryProgress{})),
		fmt.Sprintf("off duckdb_query_progress_type.percentage %d", unsafe.Offsetof(QueryProgress{}.Percentage)),
		fmt.Sprintf("off duckdb_query_progress_type.rows_processed %d", unsafe.Offsetof(QueryProgress{}.RowsProcessed)),
		fmt.Sprintf("off duckdb_query_progress_type.total_rows_to_process %d", unsafe.Offsetof(QueryProgress{}.TotalRowsToProcess)),
		fmt.Sprintf("size struct ArrowSchema %d %d", unsafe.Sizeof(ArrowSchema{}), unsafe.Alignof(ArrowSchema{})),
		fmt.Sprintf("off struct ArrowSchema.release %d", unsafe.Offsetof(ArrowSchema{}.Release)),
		fmt.Sprintf("off struct ArrowSchema.private_data %d", unsafe.Offsetof(ArrowSchema{}.PrivateData)),
		fmt.Sprintf("size struct ArrowArray %d %d", unsafe.Sizeof(ArrowArray{}), unsafe.Alignof(ArrowArray{})),
		fmt.Sprintf("off struct ArrowArray.release %d", unsafe.Offsetof(ArrowArray{}.Release)),
		fmt.Sprintf("off struct ArrowArray.private_data %d", unsafe.Offsetof(ArrowArray{}.PrivateData)),
	}, "\n") + "\n"
	if got != want {
		t.Fatalf("ABI layout mismatch:\ngot C:\n%s\nwant Go:\n%s", got, want)
	}
}

func TestNativeLoad(t *testing.T) {
	libPath := os.Getenv("DUCKDB_LIB_PATH")
	libSHA := os.Getenv("DUCKDB_LIB_SHA256")
	if libPath == "" || libSHA == "" {
		t.Skip("DUCKDB_LIB_PATH / DUCKDB_LIB_SHA256 not set; skipping real native load test")
	}
	api, err := Load(libPath, libSHA)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got := GoString(api.LibraryVersion()); got != PinnedVersion {
		t.Fatalf("LibraryVersion() = %q, want %q", got, PinnedVersion)
	}
	v := reflect.ValueOf(*api)
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).IsNil() {
			t.Fatalf("resolved API field %s is nil", v.Type().Field(i).Name)
		}
	}
}
