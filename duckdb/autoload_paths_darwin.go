//go:build darwin

package duckdb

import (
	"os"
	"path/filepath"
)

func platformEnvLibraryDirs() []string {
	var dirs []string
	for _, envKey := range []string{"DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH"} {
		dirs = append(dirs, parsePathList(os.Getenv(envKey), filepath.ListSeparator, false, false, filepath.IsAbs)...)
	}
	return dirs
}

func platformSystemLibraryDirs() []string {
	dirs := []string{
		"/opt/homebrew/lib",
		"/usr/local/lib",
		"/opt/local/lib",
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		dirs = append(dirs, filepath.Join(home, "lib"))
	}
	return dirs
}

func applyDownloadedLibraryPermissions(path string) error {
	return os.Chmod(path, 0o700)
}

func applyCacheDirPermissions(dir string) error {
	return os.Chmod(dir, 0o700)
}
