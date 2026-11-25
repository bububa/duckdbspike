//go:build linux

package duckdb

import (
	"os"
	"path/filepath"
	"runtime"
)

func platformEnvLibraryDirs() []string {
	return parsePathList(os.Getenv("LD_LIBRARY_PATH"), filepath.ListSeparator, false, false, filepath.IsAbs)
}

func platformSystemLibraryDirs() []string {
	var dirs []string
	dirs = append(dirs, "/usr/local/lib", "/usr/local/lib64")
	switch runtime.GOARCH {
	case "amd64":
		dirs = append(dirs, "/usr/lib/x86_64-linux-gnu", "/lib/x86_64-linux-gnu")
	case "arm64":
		dirs = append(dirs, "/usr/lib/aarch64-linux-gnu", "/lib/aarch64-linux-gnu")
	}
	dirs = append(dirs, "/usr/lib64", "/usr/lib")
	dirs = append(dirs, parseLDSoConf("/etc/ld.so.conf", 4)...)
	return dirs
}

func applyDownloadedLibraryPermissions(path string) error {
	return os.Chmod(path, 0o700)
}

func applyCacheDirPermissions(dir string) error {
	return os.Chmod(dir, 0o700)
}
