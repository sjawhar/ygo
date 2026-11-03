package crdt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A drain taken back on a bounded document (drainParked) parks again every
// struct it took out of the queue, so the park record's list of structs a
// drain took out (#340's transactionParked.left, which the failed-apply encode
// reads) goes back to what it was as the drain began: a struct listed there
// and parked again would be left out of the bytes the room relays.
func TestUnit_IntegrateScan_ATakenBackDrainKeepsTheParkRecordsDrainedList(t *testing.T) {
	doc := New(WithClientID(1), WithMaxIntegrateScan(1<<20))
	earlier := &Item{ID: ID{Client: 7, Clock: 0}, Content: NewContentDeleted(1)}
	drained := &Item{ID: ID{Client: 7, Clock: 1}, Content: NewContentDeleted(1)}
	doc.Transact(func(txn *Transaction) {
		txn.Local = false
		beginRemoteApply(txn, doc.store.StateVector())
		require.NotNil(t, txn.applyJournal())
		rec := txn.parkRecord()
		rec.items = append(rec.items, earlier, drained)
		rec.left = append(rec.left, earlier)
		drainParked(txn, func() {
			rec.left = append(rec.left, drained) // the drain took it out...
			panic(integrateScanBudgetExceeded{}) // ...and is taken back
		})
		require.Equal(t, []*Item{earlier}, rec.left, "the taken-back drain's structs are parked again, so not listed")
	})
}
