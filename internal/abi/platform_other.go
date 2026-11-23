//go:build !((darwin && arm64) || (linux && (amd64 || arm64)))

package abi

import "errors"

// platformAccepted is false: this platform has no real-native acceptance run.
func platformAccepted() bool { return false }

func openLibrary(string) (func(string) (uintptr, error), error) {
	return nil, errors.New("unreachable: platform not accepted")
}
