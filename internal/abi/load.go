package abi

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"unsafe"

	"github.com/ebitengine/purego"
)

// Load errors. Each is returned wrapped with context.
var (
	ErrUnsupportedPlatform = errors.New("duckdb: platform has no native acceptance record")
	ErrUnsafeLibraryPath   = errors.New("duckdb: unsafe native library path")
	ErrChecksumMismatch    = errors.New("duckdb: native library checksum mismatch")
	ErrMissingSymbol       = errors.New("duckdb: native library is missing a required symbol")
	ErrVersionMismatch     = errors.New("duckdb: native library version mismatch")
	ErrLibraryOpen         = errors.New("duckdb: native library cannot be opened")
)

// Load verifies and opens the approved native library at path and resolves the
// complete API table. The library stays mapped for the life of the process;
// there is deliberately no unload.
//
// The path must be absolute and contain no NUL bytes; symlinks are resolved
// first and the resolved file and its directory must not be group- or
// world-writable. The SHA-256 digest of the resolved file must equal
// wantSHA256 (64 lower-case hex digits), and the loaded library must report
// PinnedVersion.
func Load(path, wantSHA256 string) (*API, error) {
	if !platformAccepted() {
		return nil, fmt.Errorf("%w: %s/%s", ErrUnsupportedPlatform, runtime.GOOS, runtime.GOARCH)
	}
	if !isSHA256Hex(wantSHA256) {
		return nil, fmt.Errorf("%w: expected 64 lowercase hex digits, got %q", ErrChecksumMismatch, wantSHA256)
	}
	resolved, err := checkLibraryPath(path)
	if err != nil {
		return nil, err
	}
	got, err := fileSHA256(resolved)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLibraryOpen, err)
	}
	if got != wantSHA256 {
		return nil, fmt.Errorf("%w: %s has %s, want %s", ErrChecksumMismatch, resolved, got, wantSHA256)
	}
	lookup, err := openLibrary(resolved)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLibraryOpen, err)
	}
	return loadFromLookup(lookup)
}

func loadFromLookup(lookup func(string) (uintptr, error)) (*API, error) {
	api := &API{}
	if err := api.resolve(lookup); err != nil {
		return nil, err
	}
	if err := verifyVersion(api); err != nil {
		return nil, err
	}
	return api, nil
}

func verifyVersion(api *API) error {
	if version := GoString(api.LibraryVersion()); version != PinnedVersion {
		return fmt.Errorf("%w: loaded %q, pinned %q", ErrVersionMismatch, version, PinnedVersion)
	}
	return nil
}

// resolve binds every symbol or none: a failure leaves the receiver untouched
// (all function fields nil) and reports all missing names at once.
func (a *API) resolve(lookup func(string) (uintptr, error)) (err error) {
	symbols := a.symbols()
	addrs := make([]uintptr, len(symbols))
	var missing []string
	for i, s := range symbols {
		addr, lookupErr := lookup(s.name)
		if lookupErr != nil || addr == 0 {
			missing = append(missing, s.name)
			continue
		}
		addrs[i] = addr
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: %v", ErrMissingSymbol, missing)
	}
	defer func() {
		if r := recover(); r != nil {
			*a = API{}
			err = fmt.Errorf("%w: register: %v", ErrLibraryOpen, r)
		}
	}()
	for i, s := range symbols {
		purego.RegisterFunc(s.field, addrs[i])
	}
	return nil
}

func isSHA256Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func checkLibraryPath(path string) (string, error) {
	if strings.IndexByte(path, 0) >= 0 {
		return "", fmt.Errorf("%w: path contains NUL", ErrUnsafeLibraryPath)
	}
	if err := checkLibraryPathPrefix(path); err != nil {
		return "", err
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("%w: %q is not absolute", ErrUnsafeLibraryPath, path)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrUnsafeLibraryPath, err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrUnsafeLibraryPath, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%w: %s is not a regular file", ErrUnsafeLibraryPath, resolved)
	}
	if err := checkLibraryPermissions(resolved, info); err != nil {
		return "", err
	}
	return resolved, nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// GoString copies a NUL-terminated native string. It returns "" for 0.
func GoString(p uintptr) string {
	if p == 0 {
		return ""
	}
	base := Pointer(p)
	n := 0
	for *(*byte)(unsafe.Add(base, n)) != 0 {
		n++
	}
	return string(unsafe.Slice((*byte)(base), n))
}

// Pointer converts a native (C heap) address to unsafe.Pointer. It must only
// be used for memory owned by the native library, never for Go memory; the Go
// garbage collector neither moves nor tracks such addresses.
func Pointer(p uintptr) unsafe.Pointer {
	return *(*unsafe.Pointer)(unsafe.Pointer(&p))
}

// CString returns a NUL-terminated copy of s. Interior NUL bytes are rejected
// because native C-string parameters would silently truncate them.
func CString(s string) ([]byte, error) {
	if i := strings.IndexByte(s, 0); i >= 0 {
		return nil, fmt.Errorf("duckdb: string contains NUL at byte %d", i)
	}
	b := make([]byte, len(s)+1)
	copy(b, s)
	return b, nil
}
