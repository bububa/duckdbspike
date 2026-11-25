//go:build windows

package duckdb

import (
	"os"
	"path/filepath"
)

func platformEnvLibraryDirs() []string {
	return parsePathList(os.Getenv("PATH"), filepath.ListSeparator, true, true, filepath.IsAbs)
}

func platformSystemLibraryDirs() []string {
	var dirs []string
	if localAppData := os.Getenv("LOCALAPPDATA"); localAppData != "" {
		dirs = append(dirs, filepath.Join(localAppData, "duckdb"))
	}
	if progFiles := os.Getenv("ProgramFiles"); progFiles != "" {
		dirs = append(dirs, filepath.Join(progFiles, "duckdb"))
	}
	if sysRoot := os.Getenv("SystemRoot"); sysRoot != "" {
		dirs = append(dirs, filepath.Join(sysRoot, "System32"))
	}
	return dirs
}

// On Windows, NTFS ACLs enforce file access and Go's Chmod only toggles the
// read-only attribute. Keep downloaded DLLs readable/writable for owner cleanup.
func applyDownloadedLibraryPermissions(string) error { return nil }

func applyCacheDirPermissions(string) error { return nil }
