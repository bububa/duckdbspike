package duckdb

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/bububa/duckdbspike/internal/abi"
)

const (
	autoLoadMaxArchiveBytes int64 = 512 << 20 // 512 MiB cap for release zip
	autoLoadMaxLibraryBytes int64 = 512 << 20 // 512 MiB cap for extracted shared library
	autoLoadDefaultTimeout        = 5 * time.Minute
)

var autoLoadMu sync.Mutex

type autoLoadConfig struct {
	httpClient      *http.Client
	downloadBaseURL string
	searchPaths     []string
	skipSystemPaths bool
	disableDownload bool
	expectedSHA256  string
}

// AutoLoadOption configures AutoLoad discovery and download behavior.
type AutoLoadOption func(*autoLoadConfig)

// WithHTTPClient overrides the HTTP client used when AutoLoad downloads the
// native DuckDB release archive (useful for proxies, custom timeouts, or tests).
func WithHTTPClient(client *http.Client) AutoLoadOption {
	return func(c *autoLoadConfig) { c.httpClient = client }
}

// WithDownloadBaseURL overrides the release download base URL (for example an
// internal mirror or artifact proxy). AutoLoad requests
// "<baseURL>/<PinnedVersion>/<archiveName>" and still enforces SHA-256 verification.
func WithDownloadBaseURL(baseURL string) AutoLoadOption {
	return func(c *autoLoadConfig) { c.downloadBaseURL = baseURL }
}

// WithSearchPaths prepends explicit directories or direct library file paths
// checked before downloadPath and platform system directories.
func WithSearchPaths(paths ...string) AutoLoadOption {
	return func(c *autoLoadConfig) {
		c.searchPaths = append(c.searchPaths, paths...)
	}
}

// WithSkipSystemPaths disables scanning executable, environment variable
// (LD_LIBRARY_PATH/DYLD_*/PATH), and OS standard library directories so only
// WithSearchPaths and downloadPath are inspected.
func WithSkipSystemPaths(skip bool) AutoLoadOption {
	return func(c *autoLoadConfig) { c.skipSystemPaths = skip }
}

// WithDisableDownload disables network fallback so AutoLoad only discovers and
// loads an already-installed verified library, returning ErrLibraryNotFound if
// none matches.
func WithDisableDownload(disable bool) AutoLoadOption {
	return func(c *autoLoadConfig) { c.disableDownload = disable }
}

// WithExpectedSHA256 overrides the expected 64-character lowercase hex SHA-256
// digest for custom or internally built PinnedVersion libraries. Verification
// cannot be disabled; invalid hex strings are rejected with ErrChecksumMismatch.
func WithExpectedSHA256(sha256Hex string) AutoLoadOption {
	return func(c *autoLoadConfig) { c.expectedSHA256 = sha256Hex }
}

// AutoLoad locates an installed DuckDB native library matching PinnedVersion
// and its verified SHA-256 across platform-specific library paths and
// downloadPath. Discovery verifies path safety, permissions, and SHA-256
// before opening any file so wrong-version binaries are never mapped via dlopen.
//
// If no verified installation is found and downloads are enabled, AutoLoad
// downloads the official DuckDB release archive into a versioned 0700
// subdirectory of downloadPath, verifies the extracted shared library's SHA-256
// atomically, and loads it via Load.
//
// If downloadPath is empty, AutoLoad defaults to a "duckdbspike" subdirectory
// under os.UserCacheDir().
func AutoLoad(ctx context.Context, downloadPath string, opts ...AutoLoadOption) (*Library, error) {
	artifact, err := abi.CurrentArtifact()
	if err != nil {
		return nil, err
	}
	var cfg autoLoadConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return autoLoadWith(ctx, downloadPath, artifact, cfg, Load)
}

func autoLoadWith(
	ctx context.Context,
	downloadPath string,
	artifact abi.Artifact,
	cfg autoLoadConfig,
	loadFn func(path, sha256Hex string) (*Library, error),
) (*Library, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	wantSHA := artifact.SHA256
	if cfg.expectedSHA256 != "" {
		wantSHA = cfg.expectedSHA256
	}
	if err := abi.ValidateSHA256Hex(wantSHA); err != nil {
		return nil, err
	}

	absDownloadDir, err := resolveDownloadBaseDir(downloadPath)
	if err != nil {
		return nil, err
	}
	versionedDir := filepath.Join(absDownloadDir, "duckdb-"+artifact.Version)
	targetFile := filepath.Join(versionedDir, artifact.LibraryName)

	var extraDirs []string
	if !cfg.skipSystemPaths {
		if exe, exeErr := os.Executable(); exeErr == nil && exe != "" {
			extraDirs = append(extraDirs, filepath.Dir(exe))
		}
		extraDirs = append(extraDirs, platformEnvLibraryDirs()...)
		extraDirs = append(extraDirs, platformSystemLibraryDirs()...)
	}

	candidates := expandSearchCandidates(
		cfg.searchPaths,
		[]string{versionedDir, absDownloadDir},
		extraDirs,
		artifact.LibraryNames(),
	)

	if resolved, skipped := probeLibraryCandidates(candidates, wantSHA); resolved != "" {
		return loadFn(resolved, wantSHA)
	} else if cfg.disableDownload {
		if len(skipped) > 0 {
			return nil, fmt.Errorf("%w: checked %d candidate paths (skipped: %s)", ErrLibraryNotFound, len(candidates), strings.Join(skipped, "; "))
		}
		return nil, fmt.Errorf("%w: checked %d candidate paths for %s (%s)", ErrLibraryNotFound, len(candidates), artifact.LibraryName, artifact.Version)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	resolved, err := downloadVerifiedLibrary(ctx, versionedDir, targetFile, artifact, wantSHA, cfg)
	if err != nil {
		return nil, err
	}
	return loadFn(resolved, wantSHA)
}

func resolveDownloadBaseDir(downloadPath string) (string, error) {
	if strings.IndexByte(downloadPath, 0) >= 0 {
		return "", fmt.Errorf("%w: downloadPath contains NUL", ErrInvalidInput)
	}
	if runtime.GOOS == "windows" && isUNCPath(downloadPath) {
		return "", fmt.Errorf("%w: UNC downloadPath %q is not allowed", ErrUnsafeLibraryPath, downloadPath)
	}
	if strings.TrimSpace(downloadPath) == "" {
		cacheDir, err := os.UserCacheDir()
		if err != nil {
			return "", fmt.Errorf("%w: resolve user cache dir: %v", ErrInvalidInput, err)
		}
		downloadPath = filepath.Join(cacheDir, "duckdbspike")
	}
	abs, err := filepath.Abs(downloadPath)
	if err != nil {
		return "", fmt.Errorf("%w: resolve downloadPath %q: %v", ErrInvalidInput, downloadPath, err)
	}
	if runtime.GOOS == "windows" && isUNCPath(abs) {
		return "", fmt.Errorf("%w: UNC downloadPath %q is not allowed", ErrUnsafeLibraryPath, abs)
	}
	return abs, nil
}

// probeLibraryCandidates checks candidates in priority order, deduplicating
// canonical symlink targets so the same physical file is hashed at most once.
// Non-matching checksums or unsafe-permission directories are recorded and
// skipped rather than aborting discovery.
func probeLibraryCandidates(candidates []string, wantSHA string) (matched string, skipped []string) {
	seenCanonical := make(map[string]struct{}, len(candidates))
	for _, cand := range candidates {
		if _, err := os.Lstat(cand); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			skipped = append(skipped, fmt.Sprintf("%s: %v", cand, err))
			continue
		}
		canonical, err := filepath.EvalSymlinks(cand)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			skipped = append(skipped, fmt.Sprintf("%s: %v", cand, err))
			continue
		}
		if _, dup := seenCanonical[canonical]; dup {
			continue
		}
		seenCanonical[canonical] = struct{}{}

		resolved, err := abi.CheckLibraryCandidate(canonical, wantSHA)
		if err == nil {
			return resolved, skipped
		}
		skipped = append(skipped, fmt.Sprintf("%s: %v", canonical, err))
	}
	return "", skipped
}

func downloadVerifiedLibrary(
	ctx context.Context,
	targetDir, targetFile string,
	artifact abi.Artifact,
	wantSHA string,
	cfg autoLoadConfig,
) (string, error) {
	autoLoadMu.Lock()
	defer autoLoadMu.Unlock()

	// Re-check after acquiring the mutex in case a concurrent caller completed
	// the download while we waited.
	if resolved, err := abi.CheckLibraryCandidate(targetFile, wantSHA); err == nil {
		return resolved, nil
	}

	if err := os.MkdirAll(targetDir, 0o700); err != nil {
		return "", fmt.Errorf("%w: create download directory %q: %v", ErrLibraryOpen, targetDir, err)
	}
	if err := applyCacheDirPermissions(targetDir); err != nil {
		return "", fmt.Errorf("%w: set permissions on %q: %v", ErrUnsafeLibraryPath, targetDir, err)
	}

	zipFile, err := os.CreateTemp(targetDir, ".duckdb-archive-*.zip")
	if err != nil {
		return "", fmt.Errorf("%w: create temp archive file: %v", ErrLibraryOpen, err)
	}
	zipPath := zipFile.Name()
	defer os.Remove(zipPath)

	downloadURL := artifact.DownloadURL(cfg.downloadBaseURL)
	client := cfg.httpClient
	if client == nil {
		client = &http.Client{Timeout: autoLoadDefaultTimeout}
	}
	if err := fetchArchiveToFile(ctx, client, downloadURL, zipFile); err != nil {
		_ = zipFile.Close()
		return "", err
	}
	if err := zipFile.Close(); err != nil {
		return "", fmt.Errorf("%w: close temp archive: %v", ErrLibraryOpen, err)
	}

	if err := extractVerifiedLibraryFromZip(zipPath, targetDir, targetFile, artifact.LibraryName, wantSHA); err != nil {
		return "", err
	}
	return abi.CheckLibraryCandidate(targetFile, wantSHA)
}

func fetchArchiveToFile(ctx context.Context, client *http.Client, url string, dst *os.File) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("%w: build download request for %q: %v", ErrInvalidInput, url, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("%w: download %s: %v", ErrLibraryOpen, url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: download %s: unexpected HTTP status %s", ErrLibraryOpen, url, resp.Status)
	}

	limited := io.LimitReader(resp.Body, autoLoadMaxArchiveBytes+1)
	n, err := io.Copy(dst, limited)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("%w: read archive %s: %v", ErrLibraryOpen, url, err)
	}
	if n > autoLoadMaxArchiveBytes {
		return fmt.Errorf("%w: archive %s exceeds maximum allowed size %d bytes", ErrLimitExceeded, url, autoLoadMaxArchiveBytes)
	}
	return nil
}

func extractVerifiedLibraryFromZip(zipPath, targetDir, targetFile, libName, wantSHA string) error {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("%w: open downloaded zip: %v", ErrLibraryOpen, err)
	}
	defer zr.Close()

	var matchedEntry *zip.File
	for _, f := range zr.File {
		norm := strings.ReplaceAll(f.Name, "\\", "/")
		if strings.HasPrefix(norm, "/") || strings.Contains(norm, "\x00") {
			continue
		}
		clean := path.Clean(norm)
		if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(clean, "/../") {
			continue
		}
		if path.Base(clean) != libName {
			continue
		}
		if f.FileInfo().IsDir() || f.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: archive entry %q is not a regular file", ErrUnsafeLibraryPath, f.Name)
		}
		if matchedEntry != nil {
			return fmt.Errorf("%w: archive contains multiple entries named %q", ErrUnsafeLibraryPath, libName)
		}
		matchedEntry = f
	}
	if matchedEntry == nil {
		return fmt.Errorf("%w: archive does not contain %q", ErrLibraryNotFound, libName)
	}

	rc, err := matchedEntry.Open()
	if err != nil {
		return fmt.Errorf("%w: open archive entry %q: %v", ErrLibraryOpen, matchedEntry.Name, err)
	}
	defer rc.Close()

	tmpFile, err := os.CreateTemp(targetDir, ".duckdb-lib-*.tmp")
	if err != nil {
		return fmt.Errorf("%w: create temp library file: %v", ErrLibraryOpen, err)
	}
	tmpPath := tmpFile.Name()
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	hasher := sha256.New()
	limited := io.LimitReader(rc, autoLoadMaxLibraryBytes+1)
	n, err := io.Copy(io.MultiWriter(tmpFile, hasher), limited)
	if err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("%w: extract %q: %v", ErrLibraryOpen, matchedEntry.Name, err)
	}
	if n > autoLoadMaxLibraryBytes {
		_ = tmpFile.Close()
		return fmt.Errorf("%w: extracted library %q exceeds maximum allowed size %d bytes", ErrLimitExceeded, matchedEntry.Name, autoLoadMaxLibraryBytes)
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("%w: sync extracted library: %v", ErrLibraryOpen, err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("%w: close extracted library: %v", ErrLibraryOpen, err)
	}

	gotSHA := hex.EncodeToString(hasher.Sum(nil))
	if gotSHA != wantSHA {
		return fmt.Errorf("%w: downloaded %s has %s, want %s", ErrChecksumMismatch, libName, gotSHA, wantSHA)
	}
	if err := applyDownloadedLibraryPermissions(tmpPath); err != nil {
		return fmt.Errorf("%w: set permissions on %q: %v", ErrUnsafeLibraryPath, tmpPath, err)
	}
	if err := os.Rename(tmpPath, targetFile); err != nil {
		// Handle cross-process races (especially on Windows where os.Rename
		// cannot overwrite an existing file) by accepting an already-verified target.
		if _, checkErr := abi.CheckLibraryCandidate(targetFile, wantSHA); checkErr == nil {
			return nil
		}
		return fmt.Errorf("%w: install extracted library to %q: %v", ErrLibraryOpen, targetFile, err)
	}
	cleanupTmp = false
	return nil
}
