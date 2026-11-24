package abi

import (
	"sync"
	"sync/atomic"

	"github.com/ebitengine/purego"
)

var (
	schemaReleaseOnce sync.Once
	schemaReleaseCB   uintptr

	arrayReleaseOnce sync.Once
	arrayReleaseCB   uintptr

	exportedArrowSeq     atomic.Uintptr
	exportedArrowCount   atomic.Int64
	exportedArrowHandles sync.Map // map[uintptr]func()
)

func nextExportedArrowID() uintptr {
	for {
		if id := exportedArrowSeq.Add(1); id != 0 {
			return id
		}
	}
}

func runExportedCleanup(id uintptr) {
	if id == 0 {
		return
	}
	v, ok := exportedArrowHandles.LoadAndDelete(id)
	if !ok {
		return
	}
	exportedArrowCount.Add(-1)
	if cleanup, ok := v.(func()); ok && cleanup != nil {
		cleanup()
	}
}

// ArrowSchemaReleaseCallback returns the process-wide singleton C release
// callback for Go-exported ArrowSchema structs. It uses at most one purego
// callback slot for the entire process.
func ArrowSchemaReleaseCallback() uintptr {
	schemaReleaseOnce.Do(func() {
		schemaReleaseCB = purego.NewCallback(func(_ purego.CDecl, ptr uintptr) uintptr {
			if ptr == 0 {
				return 0
			}
			s := (*ArrowSchema)(Pointer(ptr))
			id := s.PrivateData
			s.Release = 0
			s.PrivateData = 0
			runExportedCleanup(id)
			return 0
		})
	})
	return schemaReleaseCB
}

// ArrowArrayReleaseCallback returns the process-wide singleton C release
// callback for Go-exported ArrowArray structs. It uses at most one purego
// callback slot for the entire process.
func ArrowArrayReleaseCallback() uintptr {
	arrayReleaseOnce.Do(func() {
		arrayReleaseCB = purego.NewCallback(func(_ purego.CDecl, ptr uintptr) uintptr {
			if ptr == 0 {
				return 0
			}
			a := (*ArrowArray)(Pointer(ptr))
			id := a.PrivateData
			a.Release = 0
			a.PrivateData = 0
			runExportedCleanup(id)
			return 0
		})
	})
	return arrayReleaseCB
}

// RegisterExportedArrowSchema attaches the singleton C release callback to raw
// and registers cleanup to run exactly once when C or Go releases raw.
// Callers must keep raw (and all memory referenced by raw) pinned until cleanup
// executes.
func RegisterExportedArrowSchema(raw *ArrowSchema, cleanup func()) {
	if raw == nil {
		return
	}
	id := nextExportedArrowID()
	exportedArrowHandles.Store(id, cleanup)
	exportedArrowCount.Add(1)
	raw.PrivateData = id
	raw.Release = ArrowSchemaReleaseCallback()
}

// RegisterExportedArrowArray attaches the singleton C release callback to raw
// and registers cleanup to run exactly once when C or Go releases raw (even if
// a C consumer shallow-copies *raw before invoking Release). Callers must keep
// all memory referenced by raw pinned until cleanup executes.
func RegisterExportedArrowArray(raw *ArrowArray, cleanup func()) {
	if raw == nil {
		return
	}
	id := nextExportedArrowID()
	exportedArrowHandles.Store(id, cleanup)
	exportedArrowCount.Add(1)
	raw.PrivateData = id
	raw.Release = ArrowArrayReleaseCallback()
}

// MarkArrowSchemaChildRelease sets a no-op release callback on a child or
// dictionary ArrowSchema node owned by a parent registered with
// RegisterExportedArrowSchema.
func MarkArrowSchemaChildRelease(raw *ArrowSchema) {
	if raw == nil {
		return
	}
	raw.PrivateData = 0
	raw.Release = ArrowSchemaReleaseCallback()
}

// MarkArrowArrayChildRelease sets a no-op release callback on a child or
// dictionary ArrowArray node owned by a parent registered with
// RegisterExportedArrowArray.
func MarkArrowArrayChildRelease(raw *ArrowArray) {
	if raw == nil {
		return
	}
	raw.PrivateData = 0
	raw.Release = ArrowArrayReleaseCallback()
}

// ExportedArrowHandleCount returns the number of currently live exported Arrow
// schemas and arrays awaiting release. Intended for leak detection in tests.
func ExportedArrowHandleCount() int64 {
	return exportedArrowCount.Load()
}
