//go:build (darwin && arm64) || (linux && (amd64 || arm64))

package abi

import "github.com/ebitengine/purego"

// platformAccepted reports whether this GOOS/GOARCH has a recorded real-native
// acceptance run (ABI, types, cancellation, read-only, resources). Add a
// platform only together with its acceptance record; cross-compilation is not
// acceptance.
func platformAccepted() bool { return true }

// openLibrary maps the library for the life of the process (no dlclose).
func openLibrary(path string) (func(string) (uintptr, error), error) {
	handle, err := purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_LOCAL)
	if err != nil {
		return nil, err
	}
	return func(name string) (uintptr, error) { return purego.Dlsym(handle, name) }, nil
}
