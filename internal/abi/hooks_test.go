package abi

import "testing"

func TestHookHandleRegistryLifecycleAndIdempotency(t *testing.T) {
	base := HookHandleCount()

	id1 := RegisterHookHandle("alpha")
	id2 := RegisterHookHandle(42)
	if id1 == 0 || id2 == 0 || id1 == id2 {
		t.Fatalf("expected distinct non-zero handle IDs, got %d and %d", id1, id2)
	}
	if got := HookHandleCount(); got != base+2 {
		t.Fatalf("HookHandleCount = %d, want %d", got, base+2)
	}

	v1, ok := LookupHookHandle(id1)
	if !ok || v1 != "alpha" {
		t.Fatalf("LookupHookHandle(%d) = (%v, %v), want (alpha, true)", id1, v1, ok)
	}
	v2, ok := LookupHookHandle(id2)
	if !ok || v2 != 42 {
		t.Fatalf("LookupHookHandle(%d) = (%v, %v), want (42, true)", id2, v2, ok)
	}
	if _, ok := LookupHookHandle(0); ok {
		t.Fatal("LookupHookHandle(0) unexpectedly succeeded")
	}

	// CloneHookHandle creates an independent handle ID pointing to the same value.
	cloneID := CloneHookHandle(id1)
	if cloneID == 0 || cloneID == id1 {
		t.Fatalf("CloneHookHandle(%d) = %d, want distinct non-zero ID", id1, cloneID)
	}
	if CloneHookHandle(0) != 0 {
		t.Fatal("CloneHookHandle(0) unexpectedly returned non-zero")
	}
	if got := HookHandleCount(); got != base+3 {
		t.Fatalf("HookHandleCount after clone = %d, want %d", got, base+3)
	}
	if cv, ok := LookupHookHandle(cloneID); !ok || cv != "alpha" {
		t.Fatalf("LookupHookHandle(clone=%d) = (%v, %v), want (alpha, true)", cloneID, cv, ok)
	}

	// Releasing twice must be idempotent and never underflow HookHandleCount.
	ReleaseHookHandle(id1)
	ReleaseHookHandle(id1)
	ReleaseHookHandle(0)
	if _, ok := LookupHookHandle(id1); ok {
		t.Fatalf("handle %d still present after release", id1)
	}
	// Cloned handle remains alive after original is released.
	if cv, ok := LookupHookHandle(cloneID); !ok || cv != "alpha" {
		t.Fatalf("cloned handle lost after releasing original")
	}
	ReleaseHookHandle(cloneID)
	if got := HookHandleCount(); got != base+1 {
		t.Fatalf("HookHandleCount = %d, want %d", got, base+1)
	}

	ReleaseHookHandle(id2)
	if got := HookHandleCount(); got != base {
		t.Fatalf("HookHandleCount = %d, want %d", got, base)
	}
}

func TestSingletonHookCallbacksAreStable(t *testing.T) {
	pairs := []struct {
		name string
		fn   func() uintptr
	}{
		{"delete", HookDeleteCallback},
		{"copy", HookCopyCallback},
		{"replacement_scan", ReplacementScanCallback},
		{"tf_bind", TableFunctionBindCallback},
		{"tf_init", TableFunctionInitCallback},
		{"tf_local_init", TableFunctionLocalInitCallback},
		{"tf_exec", TableFunctionExecCallback},
		{"sf_bind", ScalarFunctionBindCallback},
		{"sf_init", ScalarFunctionInitCallback},
		{"sf_exec", ScalarFunctionExecCallback},
		{"agg_size", AggregateStateSizeCallback},
		{"agg_init", AggregateInitCallback},
		{"agg_update", AggregateUpdateCallback},
		{"agg_combine", AggregateCombineCallback},
		{"agg_finalize", AggregateFinalizeCallback},
		{"agg_destroy", AggregateDestroyCallback},
		{"cast_exec", CastFunctionExecCallback},
		{"copy_bind", CopyFunctionBindCallback},
		{"copy_init", CopyFunctionGlobalInitCallback},
		{"copy_sink", CopyFunctionSinkCallback},
		{"copy_finalize", CopyFunctionFinalizeCallback},
		{"log_write", LogStorageWriteCallback},
	}

	seen := make(map[uintptr]string, len(pairs))
	for _, p := range pairs {
		c1 := p.fn()
		c2 := p.fn()
		if c1 == 0 || c1 != c2 {
			t.Fatalf("%s callback not stable: %#x vs %#x", p.name, c1, c2)
		}
		if prev, dup := seen[c1]; dup {
			t.Fatalf("%s callback collides with %s at %#x", p.name, prev, c1)
		}
		seen[c1] = p.name
	}
	if len(seen) != 22 {
		t.Fatalf("expected 22 distinct singleton callbacks, got %d", len(seen))
	}
}
