package crdt

import "testing"

// On a bounded document a parked struct that panics in the drain is dropped as
// on any document, and the update whose apply ran the drain stands: a bounded
// document refuses an update whole, and refusing this one would put the struct
// back and refuse every later update that fills its gap.
func TestUnit_IntegrateScan_DrainDropsAPanickingParkedStructAndTheUpdateStands(t *testing.T) {
	a, b, c, x := drainPanicUpdates(t)
	for _, v2 := range []bool{false, true} {
		name := map[bool]string{false: "V1", true: "V2"}[v2]
		t.Run(name, func(t *testing.T) {
			room := New(WithClientID(1), WithMaxIntegrateScan(1<<20))
			for _, u := range [][]byte{b, c, x} {
				lazyApply(t, room, u, v2)
			}
			for _, item := range room.store.pending.items {
				if item.ID == (ID{Client: 13, Clock: 1}) {
					item.Content = panicContent{} // "b" panics when the drain tries it
				}
			}
			var emitted [][]byte
			room.OnUpdate(func(u []byte, _ any) { emitted = append(emitted, u) })

			fill := a
			if v2 {
				var err error
				if fill, err = UpdateV1ToV2(a); err != nil {
					t.Fatal(err)
				}
				if err = ApplyUpdateV2(room, fill, nil); err != nil {
					t.Fatalf("the update that filled the gap was refused: %v", err)
				}
			} else if err := ApplyUpdateV1(room, fill, nil); err != nil {
				t.Fatalf("the update that filled the gap was refused: %v", err)
			}
			want := New(WithClientID(2))
			lazyApply(t, want, a, false)
			lazyApply(t, want, x, false)
			if got, w := room.GetText("t").ToString(), want.GetText("t").ToString(); got != w {
				t.Fatalf("room after the drain = %q, want %q", got, w)
			}
			if got := room.PendingStats().Items; got != 1 {
				t.Fatalf("pending items after the drain = %d, want 1 (\"c\"; \"b\" dropped)", got)
			}
			if len(emitted) != 1 {
				t.Fatalf("OnUpdate calls = %d, want 1", len(emitted))
			}

			// The sender re-sends "b"; the room converges with a peer that saw everything.
			lazyApply(t, room, b, v2)
			peer := New(WithClientID(4))
			for _, u := range [][]byte{a, b, c, x} {
				lazyApply(t, peer, u, false)
			}
			if got, w := room.GetText("t").ToString(), peer.GetText("t").ToString(); got != w {
				t.Fatalf("room after b is re-sent = %q, peer = %q", got, w)
			}
			if got := room.PendingStats().Items; got != 0 {
				t.Fatalf("pending items after b is re-sent = %d, want 0", got)
			}
		})
	}
}
