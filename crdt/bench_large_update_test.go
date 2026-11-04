package crdt

import (
	"math/rand/v2"
	"sync"
	"testing"
)

// largeUpdate is a whole-document update of a few MB, as a room's load or a
// client's SyncStep2 sends: three writers each insert 20,000 32-character
// strings and delete 4,000 characters at seeded random positions, syncing
// every fifth round, and the update is the merged document's full state. It
// holds 138,183 structs, more than the default pending cap of 100,000.
var largeUpdate struct {
	once   sync.Once
	v1, v2 []byte
}

func largeUpdateFixture(b *testing.B) (v1, v2 []byte) {
	b.Helper()
	largeUpdate.once.Do(func() {
		rng := rand.New(rand.NewPCG(2526, 10))
		writers := []*Doc{New(WithClientID(1)), New(WithClientID(2)), New(WithClientID(3))}
		for round := range 20 {
			for _, w := range writers {
				text := w.GetText("t")
				n := text.Len()
				w.Transact(func(txn *Transaction) {
					for range 1000 {
						text.Insert(txn, rng.IntN(n+1), "abcdefghijklmnopqrstuvwxyz012345", nil)
						n += 32
					}
					for range 200 {
						if n > 1 {
							text.Delete(txn, rng.IntN(n-1), 1)
							n--
						}
					}
				})
			}
			if round%5 == 4 {
				for _, a := range writers {
					for _, c := range writers {
						if a != c {
							if err := ApplyUpdateV1(c, EncodeStateAsUpdateV1(a, c.StateVector()), nil); err != nil {
								panic(err)
							}
						}
					}
				}
			}
		}
		all := New()
		for _, w := range writers {
			if err := ApplyUpdateV1(all, EncodeStateAsUpdateV1(w, nil), nil); err != nil {
				panic(err)
			}
		}
		largeUpdate.v1 = EncodeStateAsUpdateV1(all, nil)
		var err error
		if largeUpdate.v2, err = UpdateV1ToV2(largeUpdate.v1); err != nil {
			panic(err)
		}
	})
	return largeUpdate.v1, largeUpdate.v2
}

func benchmarkApplyLargeUpdate(b *testing.B, v2 bool, opts ...DocOption) {
	v1Update, v2Update := largeUpdateFixture(b)
	update, apply := v1Update, ApplyUpdateV1
	if v2 {
		update, apply = v2Update, ApplyUpdateV2
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := apply(New(opts...), update, nil); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(update)), "update-bytes")
}

// The large update applied to an empty document at the default pending cap:
// it has more structs than the room the cap leaves, so the apply runs the full
// pending-cap check before it integrates anything.
func BenchmarkApplyLargeUpdateV1(b *testing.B) { benchmarkApplyLargeUpdate(b, false) }
func BenchmarkApplyLargeUpdateV2(b *testing.B) { benchmarkApplyLargeUpdate(b, true) }

// The same update at a cap above its struct count (1<<20), where only the
// cheap bound on the update's struct count runs.
func BenchmarkApplyLargeUpdateHighCapV1(b *testing.B) {
	benchmarkApplyLargeUpdate(b, false, WithMaxPendingItems(1<<20))
}

func BenchmarkApplyLargeUpdateHighCapV2(b *testing.B) {
	benchmarkApplyLargeUpdate(b, true, WithMaxPendingItems(1<<20))
}
