//go:build !darwin && !linux && !windows

package duckdb

import "os"

func platformEnvLibraryDirs() []string    { return nil }
func platformSystemLibraryDirs() []string { return nil }

func applyDownloadedLibraryPermissions(path string) error {
	return os.Chmod(path, 0o700)
}

func applyCacheDirPermissions(dir string) error {
	return os.Chmod(dir, 0o700)
}
