//go:build windows && amd64

package abi

import "syscall"

// platformAccepted reports whether this GOOS/GOARCH has a recorded real-native
// acceptance run (ABI, types, cancellation, read-only, resources). Add a
// platform only together with its acceptance record; cross-compilation is not
// acceptance.
func platformAccepted() bool { return true }

// openLibrary maps the DLL for the life of the process (no FreeLibrary).
func openLibrary(path string) (func(string) (uintptr, error), error) {
	handle, err := syscall.LoadLibrary(path)
	if err != nil {
		return nil, err
	}
	return func(name string) (uintptr, error) { return syscall.GetProcAddress(handle, name) }, nil
}
