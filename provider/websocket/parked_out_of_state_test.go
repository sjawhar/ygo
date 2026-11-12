package websocket_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/crdt"
	ygws "github.com/reearth/ygo/provider/websocket"
	ygsync "github.com/reearth/ygo/sync"
)

// A room whose document keeps its parked content out of its state
// (crdt.Doc.SetParkedOutOfState) answers a joiner's SyncStep1 without what it
// parks, and its OnStateAnswer hook weighs the answer without it; off, both
// carry it.
func TestInteg_ParkedOutOfState_TheStateAnswerCarriesNothingParked(t *testing.T) {
	for name, on := range map[string]bool{"on": true, "off": false} {
		t.Run(name, func(t *testing.T) {
			structs := make(chan int, 4)
			srv, ts := hocuspocusServer(t, func(srv *ygws.Server) {
				srv.OnLoadDocument = func(_ context.Context, _ string, doc *crdt.Doc) error {
					doc.SetParkedOutOfState(on)
					return nil
				}
				srv.OnStateAnswer = func(info ygws.StateAnswerInfo) (func(), error) {
					structs <- info.Structs
					return func() {}, nil
				}
			})
			editor := hpDial(t, ts, "room", false, crdt.New(crdt.WithClientID(8)))
			<-structs

			// Client 7's "ef" waits for its "cd", which the room never gets.
			writer := crdt.New(crdt.WithClientID(7))
			ab := textUpdate(writer, "ab")
			textUpdate(writer, "cd")
			late := textUpdate(writer, "ef")
			hpSend(t, editor, "room", tagSync, ygsync.EncodeUpdate(ab))
			hpSend(t, editor, "room", tagSync, ygsync.EncodeUpdate(late))
			require.Len(t, syncStatuses(t, editor, "room"), 2)
			room := srv.GetDoc("room")
			require.Equal(t, 1, room.PendingStats().Items, "the room parks the late frame")

			watcherDoc := crdt.New(crdt.WithClientID(9))
			hpDial(t, ts, "room", false, watcherDoc)
			want, parked := room.StructCount(), 0
			if !on {
				want, parked = want+1, 1
			}
			assert.Equal(t, want, <-structs, "the answer's weight")
			assert.Equal(t, parked, watcherDoc.PendingStats().Items, "what the answer carried parked")
			assert.Equal(t, "ab", watcherDoc.GetText("t").ToString())
		})
	}
}
