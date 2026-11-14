package websocket_test

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/awareness"
	"github.com/reearth/ygo/crdt"
	"github.com/reearth/ygo/encoding"
	ygws "github.com/reearth/ygo/provider/websocket"
	ygsync "github.com/reearth/ygo/sync"
)

// The close every update is refused with in these tests.
const (
	refusalCode   = 4413
	refusalReason = "too large"
)

// storedDoc is a PersistenceAdapter whose every room loads the same stored
// state.
type storedDoc []byte

func (d storedDoc) LoadDoc(string) ([]byte, error) { return d, nil }
func (storedDoc) StoreUpdate(string, []byte) error { return nil }

// refusingServer serves rooms that load stored and refuse every update a peer
// sends with a close of refusalCode and refusalReason. configure, when
// non-nil, runs before the server starts.
func refusingServer(t *testing.T, stored []byte, configure func(*ygws.Server)) *httptest.Server {
	t.Helper()
	srv := ygws.NewServerWithPersistence(storedDoc(stored))
	srv.OnPeerUpdate = func(ygws.PeerUpdateInfo, func() error) error {
		return &ygws.PeerClose{Code: refusalCode, Reason: refusalReason}
	}
	if configure != nil {
		configure(srv)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// serverLog is a log destination that the server's goroutines write while a
// test reads it.
type serverLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *serverLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *serverLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// readClose reads conn until it fails, and requires the failure to be a close
// frame carrying refusalCode.
func readClose(t *testing.T, conn *gws.Conn) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	var err error
	for err == nil {
		_, _, err = conn.ReadMessage()
	}
	var closed *gws.CloseError
	require.ErrorAs(t, err, &closed, "the peer must receive the close frame")
	require.Equal(t, refusalCode, closed.Code)
}

// dialSmallReceiveBuffer connects to room with the client socket's receive
// buffer cut to a few KiB, so that what the server writes while the client
// does not read waits in the server's send buffer.
func dialSmallReceiveBuffer(t *testing.T, ts *httptest.Server, room string) *gws.Conn {
	t.Helper()
	dialer := gws.Dialer{NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if err := c.(*net.TCPConn).SetReadBuffer(4 << 10); err != nil {
			_ = c.Close()
			return nil, err
		}
		return c, nil
	}}
	conn, _, err := dialer.Dial(wsURL(ts, room), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// A peer that has sent frames after the server's last read still receives the
// close frame, with its code and reason. The peer here reads nothing while the
// server writes the room's state and then the close, as a busy browser reads
// nothing, so the close waits in the server's send buffer; and it keeps sending
// awareness all the while, as a browser does. A server that closes the socket
// right after the close frame receives that awareness on a closed socket, which
// resets the connection and discards what the peer had not yet received, the
// close frame among it: the peer then reads a reset and no code.
func TestInteg_CloseHandshake_APeerStillSendingReceivesTheCloseCode(t *testing.T) {
	stored := crdt.New(crdt.WithClientID(1))
	text := stored.GetText("t")
	stored.Transact(func(txn *crdt.Transaction) { text.Insert(txn, 0, strings.Repeat("x", 192<<10), nil) })
	ts := refusingServer(t, crdt.EncodeStateAsUpdateV1(stored, nil), nil)
	conn := dialSmallReceiveBuffer(t, ts, "room")

	doc := crdt.New(crdt.WithClientID(2))
	// Answered with the room's state, which is more than the client's receive
	// buffer holds.
	sendSync(t, conn, ygsync.EncodeSyncStep1(doc))
	// Refused, so the close is written right behind the state.
	sendSync(t, conn, ygsync.EncodeUpdate(textUpdate(doc, "refused")))

	presence := awareness.New(2)
	presence.SetLocalState(map[string]any{"cursor": 7})
	frame := encoding.EncodeBytes(func(enc *encoding.Encoder) {
		enc.WriteVarUint(1) // msgAwareness
		enc.WriteVarBytes(presence.EncodeUpdate(nil))
	})
	require.NoError(t, conn.SetWriteDeadline(time.Now().Add(5*time.Second)))
	stop, sending := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(sending)
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			// Fails once the client has answered the close with its own, or
			// once the connection is gone.
			if conn.WriteMessage(gws.BinaryMessage, frame) != nil {
				return
			}
		}
	}()

	// The server writes what the buffers take, then the close.
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	var err error
	for err == nil {
		_, _, err = conn.ReadMessage()
	}
	close(stop)
	<-sending

	var closed *gws.CloseError
	require.ErrorAs(t, err, &closed, "the peer must receive the close frame")
	assert.Equal(t, refusalCode, closed.Code)
	assert.Equal(t, refusalReason, closed.Text)
}

// A peer that never answers the close frame with its own does not hold its
// connection open: the server closes it once the closing handshake's wait of
// five seconds has passed.
func TestInteg_CloseHandshake_APeerThatNeverAnswersIsClosed(t *testing.T) {
	ts := refusingServer(t, nil, nil)
	conn := dial(t, ts, "room")
	conn.SetCloseHandler(func(int, string) error { return nil }) // sends no close in reply
	doc := crdt.New(crdt.WithClientID(2))
	drainHandshake(t, conn, doc)

	sendSync(t, conn, ygsync.EncodeUpdate(textUpdate(doc, "refused")))
	readClose(t, conn)

	// The peer sends nothing more, so only the server can end the connection,
	// and it must within twice its wait.
	start := time.Now()
	require.NoError(t, conn.NetConn().SetReadDeadline(time.Now().Add(10*time.Second)))
	_, err := conn.NetConn().Read(make([]byte, 1))
	require.Error(t, err)
	assert.False(t, isTimeout(err), "the server must close a peer that never answers its close; still open after %s", time.Since(start))
}

// While the server waits for a closed peer's answer, the room goes on
// broadcasting, and frames for that peer are dropped rather than queued: the
// server writes nothing after the close frame, so they would fill the peer's
// write queue and close the connection as a slow peer's before the peer had
// answered.
func TestInteg_CloseHandshake_RoomTrafficDoesNotCloseAPeerBeingClosed(t *testing.T) {
	const queueSize = 4
	var logs serverLog
	ts := refusingServer(t, nil, func(srv *ygws.Server) {
		srv.PeerWriteQueueSize = queueSize
		srv.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	})
	closing := dial(t, ts, "room")
	closing.SetCloseHandler(func(int, string) error { return nil }) // sends no close in reply
	doc := crdt.New(crdt.WithClientID(2))
	drainHandshake(t, closing, doc)
	sender := dial(t, ts, "room")
	drainHandshake(t, sender, crdt.New())
	watcher := dial(t, ts, "room")
	drainHandshake(t, watcher, crdt.New())
	go func() { // discards whatever the sender is sent
		for {
			if _, _, err := sender.ReadMessage(); err != nil {
				return
			}
		}
	}()

	sendSync(t, closing, ygsync.EncodeUpdate(textUpdate(doc, "refused")))
	readClose(t, closing)

	// Each awareness frame the sender sends is broadcast to the closing peer
	// too. Once the watcher has the last one, every broadcast before it has
	// been offered to the closing peer: far more than its queue holds.
	presence := awareness.New(3)
	for i := range 3 * queueSize {
		presence.SetLocalState(map[string]any{"cursor": i})
		sendAwareness(t, sender, presence.EncodeUpdate(nil))
		for {
			if outerType, _ := readOne(t, watcher, 2*time.Second); outerType == 1 { // msgAwareness
				break
			}
		}
	}
	assert.NotContains(t, logs.String(), "closing slow peer", "a peer being closed must not be closed again as a slow peer")
}
