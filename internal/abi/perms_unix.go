//go:build !windows

package abi

import (
	"fmt"
	"os"
	"path/filepath"
)

func checkLibraryPathPrefix(string) error { return nil }

func checkLibraryPermissions(resolved string, info os.FileInfo) error {
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%w: %s is group/world writable", ErrUnsafeLibraryPath, resolved)
	}
	dirInfo, err := os.Stat(filepath.Dir(resolved))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnsafeLibraryPath, err)
	}
	if dirInfo.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%w: directory of %s is group/world writable", ErrUnsafeLibraryPath, resolved)
	}
	return nil
}
