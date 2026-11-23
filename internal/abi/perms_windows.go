//go:build windows

package abi

import (
	"fmt"
	"os"
	"strings"
)

// checkLibraryPathPrefix rejects UNC/network paths before hitting the
// filesystem.
func checkLibraryPathPrefix(path string) error {
	if strings.HasPrefix(path, `\\`) || strings.HasPrefix(path, "//") {
		return fmt.Errorf("%w: UNC path %q is not allowed", ErrUnsafeLibraryPath, path)
	}
	return nil
}

// checkLibraryPermissions rejects resolved UNC/network paths on Windows. POSIX
// group/other write-bit checks do not apply because Go's os.Stat synthesizes
// 0666/0777 mode bits on Windows where access control is enforced by NTFS ACLs;
// binary integrity is verified via SHA-256 before LoadLibrary.
func checkLibraryPermissions(resolved string, _ os.FileInfo) error {
	return checkLibraryPathPrefix(resolved)
}
