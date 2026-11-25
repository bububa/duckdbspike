package abi

import (
	"fmt"
	"runtime"
	"strings"
)

// DefaultDownloadBaseURL is the official DuckDB GitHub Releases base URL.
const DefaultDownloadBaseURL = "https://github.com/duckdb/duckdb/releases/download"

// Artifact describes the approved native DuckDB release archive and shared
// library digest for one accepted GOOS/GOARCH target.
type Artifact struct {
	GOOS        string
	GOARCH      string
	Version     string
	ArchiveName string
	LibraryName string
	AliasNames  []string
	SHA256      string
}

// LibraryNames returns LibraryName followed by any platform AliasNames (for
// example versioned SONAMEs on Linux).
func (a Artifact) LibraryNames() []string {
	out := make([]string, 0, 1+len(a.AliasNames))
	if a.LibraryName != "" {
		out = append(out, a.LibraryName)
	}
	out = append(out, a.AliasNames...)
	return out
}

// DownloadURL constructs the full archive URL from baseURL (falling back to
// DefaultDownloadBaseURL when baseURL is empty).
func (a Artifact) DownloadURL(baseURL string) string {
	base := strings.TrimRight(baseURL, "/")
	if base == "" {
		base = DefaultDownloadBaseURL
	}
	return base + "/" + a.Version + "/" + a.ArchiveName
}

var approvedArtifacts = map[string]Artifact{
	"darwin/arm64": {
		GOOS:        "darwin",
		GOARCH:      "arm64",
		Version:     PinnedVersion,
		ArchiveName: "libduckdb-osx-universal.zip",
		LibraryName: "libduckdb.dylib",
		SHA256:      "75a519cb914ae36ca22cff0995a8a2e8c9a46990b8c0a479514f741a0d93f344",
	},
	"linux/amd64": {
		GOOS:        "linux",
		GOARCH:      "amd64",
		Version:     PinnedVersion,
		ArchiveName: "libduckdb-linux-amd64.zip",
		LibraryName: "libduckdb.so",
		AliasNames:  []string{"libduckdb.so." + strings.TrimPrefix(PinnedVersion, "v")},
		SHA256:      "3f4962cdd266eab2b479a5b5a4a6d063d279b2954ae814ed00385d553b747633",
	},
	"linux/arm64": {
		GOOS:        "linux",
		GOARCH:      "arm64",
		Version:     PinnedVersion,
		ArchiveName: "libduckdb-linux-arm64.zip",
		LibraryName: "libduckdb.so",
		AliasNames:  []string{"libduckdb.so." + strings.TrimPrefix(PinnedVersion, "v")},
		SHA256:      "23686593dba4ec22c6e581bb1c843b3964e84c4bc4d0063d8b4daddd2e20e30a",
	},
	"windows/amd64": {
		GOOS:        "windows",
		GOARCH:      "amd64",
		Version:     PinnedVersion,
		ArchiveName: "libduckdb-windows-amd64.zip",
		LibraryName: "duckdb.dll",
		SHA256:      "7e90bdef028d57b45490d6f53530e3c012a5d9abdc4c6723449200663dc84ed0",
	},
}

// ArtifactFor returns the approved native artifact metadata for goos/goarch.
func ArtifactFor(goos, goarch string) (Artifact, bool) {
	a, ok := approvedArtifacts[goos+"/"+goarch]
	if !ok {
		return Artifact{}, false
	}
	if len(a.AliasNames) > 0 {
		a.AliasNames = append([]string(nil), a.AliasNames...)
	}
	return a, true
}

// CurrentArtifact returns the approved native artifact for the current runtime
// platform, or ErrUnsupportedPlatform if the platform has no acceptance record.
func CurrentArtifact() (Artifact, error) {
	if !platformAccepted() {
		return Artifact{}, fmt.Errorf("%w: %s/%s", ErrUnsupportedPlatform, runtime.GOOS, runtime.GOARCH)
	}
	a, ok := ArtifactFor(runtime.GOOS, runtime.GOARCH)
	if !ok {
		return Artifact{}, fmt.Errorf("%w: %s/%s", ErrUnsupportedPlatform, runtime.GOOS, runtime.GOARCH)
	}
	return a, nil
}
