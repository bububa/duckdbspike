//go:build !windows

package duckdb_test

import (
	"io/fs"
	"os"
	"runtime"
	"syscall"
)

func fileLiveSize(_ string, entry fs.DirEntry) (int64, bool) {
	info, err := entry.Info()
	if err != nil {
		return 0, false
	}
	return info.Size(), true
}

func openFDCount() int {
	dir := "/dev/fd"
	if runtime.GOOS == "linux" {
		dir = "/proc/self/fd"
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return -1
	}
	return len(entries)
}

func maxRSSBytes() int64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	if runtime.GOOS == "linux" {
		return ru.Maxrss * 1024
	}
	return ru.Maxrss
}
