package crdt

import (
	"encoding/hex"
	"strings"
	"testing"
)

var uuidAllocationSink string

func TestUnit_UUIDV4_AllocatesOnlyItsRandomBufferAndReturnedString(t *testing.T) {
	allocs := testing.AllocsPerRun(100, func() { uuidAllocationSink = uuidV4() })
	t.Logf("uuidV4 allocations: %g", allocs)
	if allocs > 2 {
		t.Fatalf("uuidV4 allocated %g objects, want at most the random buffer and returned string", allocs)
	}
}

func TestUnit_UUIDV4_VersionAndVariant(t *testing.T) {
	for range 100 {
		doc := New()
		guid := doc.GUID()
		doc.Destroy()
		if len(guid) != 36 || guid[8] != '-' || guid[13] != '-' || guid[18] != '-' || guid[23] != '-' {
			t.Fatalf("invalid UUID format: %q", guid)
		}
		b, err := hex.DecodeString(strings.ReplaceAll(guid, "-", ""))
		if err != nil || len(b) != 16 {
			t.Fatalf("invalid UUID hex %q: %v", guid, err)
		}
		if b[6]>>4 != 4 || b[8]&0xc0 != 0x80 {
			t.Fatalf("UUID %q does not have version 4 and RFC-4122 variant bits", guid)
		}
	}
}
