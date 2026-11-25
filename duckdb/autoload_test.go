package duckdb

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bububa/duckdbspike/internal/abi"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func buildTestZip(t *testing.T, entries map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range entries {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func sha256HexBytes(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestParsePathListAndLDSoConf(t *testing.T) {
	// Unix-style LD_LIBRARY_PATH / DYLD_LIBRARY_PATH parsing.
	unixIsAbs := func(p string) bool { return strings.HasPrefix(p, "/") }
	gotUnix := parsePathList(".:/opt/lib::/usr/local/lib:/opt/lib:rel/dir:/bad\x00path", ':', false, false, unixIsAbs)
	wantUnix := []string{"/opt/lib", "/usr/local/lib"}
	if len(gotUnix) != len(wantUnix) || gotUnix[0] != wantUnix[0] || gotUnix[1] != wantUnix[1] {
		t.Fatalf("parsePathList(unix) = %v, want %v", gotUnix, wantUnix)
	}

	// Windows-style PATH parsing with quotes, UNC rejection, and relative filtering.
	winIsAbs := func(p string) bool {
		return len(p) >= 3 && ((p[0] >= 'A' && p[0] <= 'Z') || (p[0] >= 'a' && p[0] <= 'z')) && p[1] == ':' && (p[2] == '\\' || p[2] == '/')
	}
	rawWin := ` ;"C:\Program Files\duckdb";\\fileserver\share;//smb/share;relative\path;D:\libs;"C:\Program Files\duckdb"`
	gotWin := parsePathList(rawWin, ';', true, true, winIsAbs)
	wantWin := []string{`C:\Program Files\duckdb`, `D:\libs`}
	if len(gotWin) != len(wantWin) || gotWin[0] != wantWin[0] || gotWin[1] != wantWin[1] {
		t.Fatalf("parsePathList(windows) = %v, want %v", gotWin, wantWin)
	}

	// Linux ld.so.conf parsing with nested include, comments, relative path skip, and cycle guard.
	root := t.TempDir()
	confDir := filepath.Join(root, "ld.so.conf.d")
	if err := os.MkdirAll(confDir, 0o700); err != nil {
		t.Fatal(err)
	}
	mainConf := filepath.Join(root, "ld.so.conf")
	subA := filepath.Join(confDir, "a.conf")
	subB := filepath.Join(confDir, "b.conf")

	dir1 := filepath.Join(root, "lib1")
	dir2 := filepath.Join(root, "lib2")
	if err := os.WriteFile(mainConf, []byte("# comment\ninclude ld.so.conf.d/*.conf\n"+dir1+"\nrelative/ignored\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(subA, []byte(dir2+" # inline comment\ninclude "+mainConf+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(subB, []byte(dir1+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gotConf := parseLDSoConf(mainConf, 4)
	if len(gotConf) != 2 || gotConf[0] != dir2 || gotConf[1] != dir1 {
		t.Fatalf("parseLDSoConf = %v, want [%s %s]", gotConf, dir2, dir1)
	}
}

func TestAutoLoadDiscoveryPrioritySymlinkAndNonFatalSkip(t *testing.T) {
	ctx := context.Background()
	payload := []byte("verified-duckdb-binary-v1.5.6")
	wantSHA := sha256HexBytes(payload)
	artifact := abi.Artifact{
		GOOS:        runtime.GOOS,
		GOARCH:      runtime.GOARCH,
		Version:     PinnedVersion,
		ArchiveName: "libduckdb-test.zip",
		LibraryName: "libduckdb.test",
		AliasNames:  []string{"libduckdb.test.1.5.6"},
		SHA256:      wantSHA,
	}

	root := t.TempDir()
	wrongDir := filepath.Join(root, "wrong")
	goodDir := filepath.Join(root, "good")
	dlDir := filepath.Join(root, "dl")
	for _, d := range []string{wrongDir, goodDir, dlDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	// Put a wrong-version binary in wrongDir (must be skipped non-fatally).
	if err := os.WriteFile(filepath.Join(wrongDir, artifact.LibraryName), []byte("old-duckdb-v1.4.0"), 0o700); err != nil {
		t.Fatal(err)
	}

	// On Unix, also put a matching binary in a world-writable (0777) dir that must be skipped non-fatally.
	var searchPaths []string
	searchPaths = append(searchPaths, wrongDir)
	if runtime.GOOS != "windows" {
		unsafeDir := filepath.Join(root, "unsafe")
		if err := os.MkdirAll(unsafeDir, 0o777); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(unsafeDir, 0o777); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(unsafeDir, artifact.LibraryName), payload, 0o700); err != nil {
			t.Fatal(err)
		}
		searchPaths = append(searchPaths, unsafeDir)
	}

	// Put matching binary under alias name in goodDir (plus a symlink to verify dedupe).
	goodFile := filepath.Join(goodDir, "libduckdb.test.1.5.6")
	if err := os.WriteFile(goodFile, payload, 0o700); err != nil {
		t.Fatal(err)
	}
	searchPaths = append(searchPaths, goodDir)

	var loadedPath, loadedSHA string
	fakeLoad := func(p, sha string) (*Library, error) {
		loadedPath = p
		loadedSHA = sha
		return &Library{}, nil
	}

	cfg := autoLoadConfig{
		searchPaths:     searchPaths,
		skipSystemPaths: true,
		disableDownload: true,
	}
	lib, err := autoLoadWith(ctx, dlDir, artifact, cfg, fakeLoad)
	if err != nil || lib == nil {
		t.Fatalf("autoLoadWith discovery failed: lib=%v err=%v", lib, err)
	}
	wantGood, err := filepath.EvalSymlinks(goodFile)
	if err != nil {
		t.Fatal(err)
	}
	if loadedPath != wantGood || loadedSHA != wantSHA {
		t.Fatalf("loaded = (%q, %q), want (%q, %q)", loadedPath, loadedSHA, wantGood, wantSHA)
	}

	// Direct file path in WithSearchPaths also works.
	loadedPath = ""
	cfgDirect := autoLoadConfig{
		searchPaths:     []string{goodFile},
		skipSystemPaths: true,
		disableDownload: true,
	}
	if _, err := autoLoadWith(ctx, dlDir, artifact, cfgDirect, fakeLoad); err != nil {
		t.Fatalf("direct file search path failed: %v", err)
	}
	if loadedPath != wantGood {
		t.Fatalf("direct loadedPath = %q, want %q", loadedPath, wantGood)
	}

	// Offline mode with only wrong-version candidate returns ErrLibraryNotFound with diagnostic context.
	cfgMissing := autoLoadConfig{
		searchPaths:     []string{wrongDir},
		skipSystemPaths: true,
		disableDownload: true,
	}
	_, err = autoLoadWith(ctx, dlDir, artifact, cfgMissing, fakeLoad)
	if !errors.Is(err, ErrLibraryNotFound) {
		t.Fatalf("offline missing err = %v, want ErrLibraryNotFound", err)
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected skipped checksum reason in error, got: %v", err)
	}
}

func TestAutoLoadDownloadExtractCacheAndConcurrency(t *testing.T) {
	ctx := context.Background()
	payload := []byte("downloaded-duckdb-shared-library-bytes")
	wantSHA := sha256HexBytes(payload)
	artifact := abi.Artifact{
		GOOS:        runtime.GOOS,
		GOARCH:      runtime.GOARCH,
		Version:     PinnedVersion,
		ArchiveName: "libduckdb-test.zip",
		LibraryName: "libduckdb.test",
		SHA256:      wantSHA,
	}
	zipBytes := buildTestZip(t, map[string][]byte{
		"duckdb.h":           []byte("// header"),
		artifact.LibraryName: payload,
	})

	var httpHits atomic.Int32
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			httpHits.Add(1)
			wantSuffix := "/" + PinnedVersion + "/" + artifact.ArchiveName
			if !strings.HasSuffix(req.URL.Path, wantSuffix) {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Status:     "404 Not Found",
					Body:       io.NopCloser(strings.NewReader("not found")),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(bytes.NewReader(zipBytes)),
			}, nil
		}),
	}

	// Even when the caller's downloadPath has loose permissions (like /tmp 0777),
	// AutoLoad isolates the library inside a 0700 versioned subdirectory so Load succeeds.
	dlBase := filepath.Join(t.TempDir(), "loose-parent")
	if err := os.MkdirAll(dlBase, 0o777); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		_ = os.Chmod(dlBase, 0o777)
	}

	cfg := autoLoadConfig{
		httpClient:      client,
		downloadBaseURL: "https://mirror.example/duckdb",
		skipSystemPaths: true,
	}

	// Run 6 concurrent goroutines: must download once and all succeed cleanly under -race.
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	paths := make(chan string, 6)
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := autoLoadWith(ctx, dlBase, artifact, cfg, func(p, sha string) (*Library, error) {
				if sha != wantSHA {
					return nil, ErrChecksumMismatch
				}
				paths <- p
				return &Library{}, nil
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	close(paths)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent autoLoadWith error: %v", err)
		}
	}
	if hits := httpHits.Load(); hits != 1 {
		t.Fatalf("HTTP download count = %d, want 1", hits)
	}

	// Subsequent call hits the versioned cache without any HTTP request.
	var cachedPath string
	if _, err := autoLoadWith(ctx, dlBase, artifact, cfg, func(p, _ string) (*Library, error) {
		cachedPath = p
		return &Library{}, nil
	}); err != nil {
		t.Fatalf("cached autoLoadWith error: %v", err)
	}
	if httpHits.Load() != 1 {
		t.Fatalf("expected cache hit with 0 extra HTTP requests, got %d total", httpHits.Load())
	}
	if !strings.Contains(cachedPath, "duckdb-"+PinnedVersion) {
		t.Fatalf("cached path %q not in versioned subdir", cachedPath)
	}

	// Verify no temporary files remain in targetDir.
	entries, err := os.ReadDir(filepath.Dir(cachedPath))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != artifact.LibraryName {
		t.Fatalf("unexpected files left in cache dir: %v", entries)
	}
}

func TestAutoLoadDownloadErrorHandlingAndCleanup(t *testing.T) {
	ctx := context.Background()
	goodPayload := []byte("expected-payload")
	wantSHA := sha256HexBytes(goodPayload)
	artifact := abi.Artifact{
		GOOS:        runtime.GOOS,
		GOARCH:      runtime.GOARCH,
		Version:     PinnedVersion,
		ArchiveName: "libduckdb-test.zip",
		LibraryName: "libduckdb.test",
		SHA256:      wantSHA,
	}
	noopLoad := func(string, string) (*Library, error) { return &Library{}, nil }

	// Nil context and canceled context.
	if _, err := autoLoadWith(nil, t.TempDir(), artifact, autoLoadConfig{skipSystemPaths: true}, noopLoad); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nil ctx err = %v, want ErrInvalidInput", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := autoLoadWith(cctx, t.TempDir(), artifact, autoLoadConfig{skipSystemPaths: true}, noopLoad); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx err = %v, want context.Canceled", err)
	}
	// Invalid expected SHA-256 and NUL downloadPath.
	if _, err := autoLoadWith(ctx, t.TempDir(), artifact, autoLoadConfig{expectedSHA256: "short", skipSystemPaths: true}, noopLoad); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("bad SHA err = %v, want ErrChecksumMismatch", err)
	}
	if _, err := autoLoadWith(ctx, "bad\x00dir", artifact, autoLoadConfig{skipSystemPaths: true}, noopLoad); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("NUL downloadPath err = %v, want ErrInvalidInput", err)
	}

	// Corrupted payload checksum mismatch must reject and clean up temp files.
	corruptZip := buildTestZip(t, map[string][]byte{artifact.LibraryName: []byte("tampered")})
	dlDir := t.TempDir()
	_, err := autoLoadWith(ctx, dlDir, artifact, autoLoadConfig{
		skipSystemPaths: true,
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(bytes.NewReader(corruptZip))}, nil
		})},
	}, noopLoad)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("corrupt zip err = %v, want ErrChecksumMismatch", err)
	}
	if entries, _ := os.ReadDir(filepath.Join(dlDir, "duckdb-"+PinnedVersion)); len(entries) != 0 {
		t.Fatalf("temp files leaked after checksum mismatch: %v", entries)
	}

	// Zip Slip path traversal entry is ignored -> ErrLibraryNotFound.
	slipZip := buildTestZip(t, map[string][]byte{"../" + artifact.LibraryName: goodPayload})
	_, err = autoLoadWith(ctx, t.TempDir(), artifact, autoLoadConfig{
		skipSystemPaths: true,
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(bytes.NewReader(slipZip))}, nil
		})},
	}, noopLoad)
	if !errors.Is(err, ErrLibraryNotFound) {
		t.Fatalf("zip slip err = %v, want ErrLibraryNotFound", err)
	}

	// Duplicate matching entries rejected with ErrUnsafeLibraryPath.
	dupZip := buildTestZip(t, map[string][]byte{
		artifact.LibraryName:          goodPayload,
		"sub/" + artifact.LibraryName: goodPayload,
	})
	_, err = autoLoadWith(ctx, t.TempDir(), artifact, autoLoadConfig{
		skipSystemPaths: true,
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(bytes.NewReader(dupZip))}, nil
		})},
	}, noopLoad)
	if !errors.Is(err, ErrUnsafeLibraryPath) {
		t.Fatalf("duplicate entry err = %v, want ErrUnsafeLibraryPath", err)
	}

	// Symlink entry inside zip rejected with ErrUnsafeLibraryPath.
	var symBuf bytes.Buffer
	zw := zip.NewWriter(&symBuf)
	hdr := &zip.FileHeader{Name: artifact.LibraryName}
	hdr.SetMode(os.ModeSymlink | 0o777)
	w, err := zw.CreateHeader(hdr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("/etc/passwd"))
	_ = zw.Close()
	_, err = autoLoadWith(ctx, t.TempDir(), artifact, autoLoadConfig{
		skipSystemPaths: true,
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(bytes.NewReader(symBuf.Bytes()))}, nil
		})},
	}, noopLoad)
	if !errors.Is(err, ErrUnsafeLibraryPath) {
		t.Fatalf("symlink entry err = %v, want ErrUnsafeLibraryPath", err)
	}

	// HTTP 404 returns ErrLibraryOpen.
	_, err = autoLoadWith(ctx, t.TempDir(), artifact, autoLoadConfig{
		skipSystemPaths: true,
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusNotFound, Status: "404 Not Found", Body: io.NopCloser(strings.NewReader("missing"))}, nil
		})},
	}, noopLoad)
	if !errors.Is(err, ErrLibraryOpen) {
		t.Fatalf("HTTP 404 err = %v, want ErrLibraryOpen", err)
	}
}
