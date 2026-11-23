//go:build windows

package duckdb_test

import (
	"io/fs"
	"syscall"
)

// fileLiveSize queries the NTFS file control block directly via
// GetFileInformationByHandle because FindFirstFile/FindNextFile directory
// entries (used by os.ReadDir) can report a stale 0-byte size while a temp
// spill file is still open and being written.
func fileLiveSize(path string, entry fs.DirEntry) (int64, bool) {
	p, err := syscall.UTF16PtrFromString(path)
	if err == nil {
		const shareAll = syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE | syscall.FILE_SHARE_DELETE
		h, createErr := syscall.CreateFile(p, 0, shareAll, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
		if createErr == nil {
			var info syscall.ByHandleFileInformation
			infoErr := syscall.GetFileInformationByHandle(h, &info)
			_ = syscall.CloseHandle(h)
			if infoErr == nil {
				return int64(info.FileSizeHigh)<<32 | int64(info.FileSizeLow), true
			}
		}
	}
	info, err := entry.Info()
	if err != nil {
		return 0, false
	}
	return info.Size(), true
}

func openFDCount() int {
	return -1
}

func maxRSSBytes() int64 {
	return 0
}
