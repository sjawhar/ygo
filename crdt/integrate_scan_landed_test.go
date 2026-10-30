package crdt

import "testing"

// A bounded document rolls back an apply that fails after it parked a
// struct, so the landed update carries nothing: the parked struct is gone.
func TestUnit_IntegrateScan_RolledBackApplyLandsNothingItParked(t *testing.T) {
	update, _ := failingAfterParking(t)
	room := New(WithClientID(1), WithMaxIntegrateScan(1<<20))
	before := EncodeStateAsUpdateV1(room, nil)
	landed, err := ApplyUpdateV1Landed(room, update, nil)
	if err == nil {
		t.Fatal("the update with a short delete set applied")
	}
	if string(before) != string(EncodeStateAsUpdateV1(room, nil)) {
		t.Fatal("the bounded document kept part of the failed apply")
	}
	if landed != nil {
		t.Fatalf("a rolled-back apply landed %x", landed)
	}
}
