package websocket

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/crdt"
)

// The hook reads which connection sent an update from PeerUpdateInfo.Context:
// two connections to one room, each named on its request context, write in
// turn, and the hook finds each one's name on the context of its own update,
// as OnTokenAuthContext finds it on the context of its token.
func TestUnit_Server_OnPeerUpdate_TheContextIsTheConnectionsRequestContext(t *testing.T) {
	const room = "r1"
	srv := NewServer()
	srv.HocuspocusFraming = true
	var (
		mu   sync.Mutex
		seen = map[string]any{}
	)
	srv.OnPeerUpdate = func(info PeerUpdateInfo, apply func() error) error {
		var name any = "no context"
		if info.Context != nil {
			name = info.Context.Value(connectionKey{})
		}
		doc := crdt.New()
		if err := crdt.ApplyUpdateV1(doc, info.Update, nil); err != nil {
			return err
		}
		mu.Lock()
		for _, key := range doc.GetMap("writes").Keys() {
			seen[key] = name
		}
		mu.Unlock()
		return apply()
	}
	ts := serveNamed(t, srv)

	alice := dialRoomInternal(t, ts, room+"?name=alice")
	bob := dialRoomInternal(t, ts, room+"?name=bob")
	sendWrite(t, alice, room, "from alice", 11)
	sendWrite(t, bob, room, "from bob", 12)
	sendWrite(t, alice, room, "alice again", 13)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, 3, "the hook saw every update")
	assert.Equal(t, map[string]any{"from alice": "alice", "from bob": "bob", "alice again": "alice"}, seen,
		"each update's Context is the request context of the connection that sent it")
	assert.ElementsMatch(t, []string{"from alice", "from bob", "alice again"}, srv.GetDoc(room).GetMap("writes").Keys(),
		"the hook applied every update")
}
