package abi

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestApprovedArtifactsManifest(t *testing.T) {
	accepted := []struct {
		goos, goarch string
		wantLib      string
	}{
		{"darwin", "arm64", "libduckdb.dylib"},
		{"linux", "amd64", "libduckdb.so"},
		{"linux", "arm64", "libduckdb.so"},
		{"windows", "amd64", "duckdb.dll"},
	}
	for _, tc := range accepted {
		a, ok := ArtifactFor(tc.goos, tc.goarch)
		if !ok {
			t.Fatalf("ArtifactFor(%s, %s) missing", tc.goos, tc.goarch)
		}
		if a.Version != PinnedVersion {
			t.Fatalf("%s/%s Version = %q, want %q", tc.goos, tc.goarch, a.Version, PinnedVersion)
		}
		if a.LibraryName != tc.wantLib {
			t.Fatalf("%s/%s LibraryName = %q, want %q", tc.goos, tc.goarch, a.LibraryName, tc.wantLib)
		}
		if a.ArchiveName == "" {
			t.Fatalf("%s/%s empty ArchiveName", tc.goos, tc.goarch)
		}
		if err := ValidateSHA256Hex(a.SHA256); err != nil {
			t.Fatalf("%s/%s invalid SHA256 %q: %v", tc.goos, tc.goarch, a.SHA256, err)
		}
		if names := a.LibraryNames(); len(names) == 0 || names[0] != tc.wantLib {
			t.Fatalf("%s/%s LibraryNames = %v", tc.goos, tc.goarch, names)
		}
		wantURL := DefaultDownloadBaseURL + "/" + PinnedVersion + "/" + a.ArchiveName
		if got := a.DownloadURL(""); got != wantURL {
			t.Fatalf("DownloadURL default = %q, want %q", got, wantURL)
		}
		if got := a.DownloadURL("https://mirror.internal/duckdb/"); got != "https://mirror.internal/duckdb/"+PinnedVersion+"/"+a.ArchiveName {
			t.Fatalf("DownloadURL mirror = %q", got)
		}
	}

	if _, ok := ArtifactFor("darwin", "amd64"); ok {
		t.Fatal("unaccepted darwin/amd64 must not be in approvedArtifacts")
	}

	cur, err := CurrentArtifact()
	if platformAccepted() {
		if err != nil {
			t.Fatalf("CurrentArtifact on accepted platform failed: %v", err)
		}
		if cur.GOOS != runtime.GOOS || cur.GOARCH != runtime.GOARCH {
			t.Fatalf("CurrentArtifact mismatch: %+v", cur)
		}
	} else if !errors.Is(err, ErrUnsupportedPlatform) {
		t.Fatalf("CurrentArtifact on unaccepted platform err = %v, want ErrUnsupportedPlatform", err)
	}
}

func TestCheckLibraryCandidate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "safe")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	payload := []byte("fake duckdb native payload for checksum check")
	sum := sha256.Sum256(payload)
	wantHex := hex.EncodeToString(sum[:])

	libPath := filepath.Join(dir, "libduckdb.test")
	if err := os.WriteFile(libPath, payload, 0o700); err != nil {
		t.Fatal(err)
	}

	resolved, err := CheckLibraryCandidate(libPath, wantHex)
	if err != nil {
		t.Fatalf("CheckLibraryCandidate(valid) failed: %v", err)
	}
	wantResolved, err := filepath.EvalSymlinks(libPath)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != wantResolved {
		t.Fatalf("resolved = %q, want %q", resolved, wantResolved)
	}

	// Symlink resolves to canonical target.
	linkPath := filepath.Join(dir, "libduckdb.link")
	if err := os.Symlink(libPath, linkPath); err == nil {
		gotLink, err := CheckLibraryCandidate(linkPath, wantHex)
		if err != nil || gotLink != wantResolved {
			t.Fatalf("CheckLibraryCandidate(symlink) = (%q, %v), want %q", gotLink, err, wantResolved)
		}
	}

	// Wrong checksum.
	badHex := "0000000000000000000000000000000000000000000000000000000000000000"
	if _, err := CheckLibraryCandidate(libPath, badHex); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("wrong checksum err = %v, want ErrChecksumMismatch", err)
	}
	if _, err := CheckLibraryCandidate(libPath, "not-hex"); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("invalid hex err = %v, want ErrChecksumMismatch", err)
	}

	// Directory is not a regular file.
	if _, err := CheckLibraryCandidate(dir, wantHex); !errors.Is(err, ErrUnsafeLibraryPath) {
		t.Fatalf("directory candidate err = %v, want ErrUnsafeLibraryPath", err)
	}

	// Unix group/world-writable check.
	if runtime.GOOS != "windows" {
		unsafeDir := filepath.Join(t.TempDir(), "unsafe")
		if err := os.Mkdir(unsafeDir, 0o777); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(unsafeDir, 0o777); err != nil {
			t.Fatal(err)
		}
		unsafeLib := filepath.Join(unsafeDir, "libduckdb.test")
		if err := os.WriteFile(unsafeLib, payload, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := CheckLibraryCandidate(unsafeLib, wantHex); !errors.Is(err, ErrUnsafeLibraryPath) {
			t.Fatalf("unsafe dir candidate err = %v, want ErrUnsafeLibraryPath", err)
		}
	}
}
