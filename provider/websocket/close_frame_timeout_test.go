package websocket_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ygws "github.com/reearth/ygo/provider/websocket"
)

// For a peer that does not read, the close frame sent for a cancel cause waits
// until Server.CloseFrameTimeout, or the package's 1 s default when the field
// is unset, before it gives up and the connection is closed, so the peer
// leaves its room no sooner than that.
//
// How long a cancelled peer may take to be told why is the embedder's to say:
// a reader on a slow link that is still taking a large frame when its session
// is revoked needs the close frame behind that frame to wait longer than the
// default, or it is closed with no code. The test is in the shape of
// TestInteg_PeerWriteTimeout_DropsASlowReaderAtTheConfiguredDeadline: the
// client never reads the unbuffered net.Pipe, so the server's handshake write
// blocks, and the close frame waits behind it until its own deadline.
//
// Only that lower bound is asserted. How soon after its deadline the peer is
// torn down is scheduling, which a loaded machine stretches; a server that
// never closes the peer fails at the test binary's deadline instead.
func TestInteg_CloseFrameTimeout_GivesUpOnAPeerThatDoesNotReadAtTheDeadline(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name    string
		timeout time.Duration // Server.CloseFrameTimeout; zero leaves it unset
		want    time.Duration
	}{
		{name: "unset", want: ygws.DefaultCloseFrameTimeout},
		{name: "set over the default", timeout: 3 * time.Second, want: 3 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			expired := beforeTestDeadline(t)

			srv := ygws.NewServer()
			srv.CloseFrameTimeout = c.timeout
			// Two other deadlines would close the peer by themselves: the
			// blocked handshake write's, and the first read's, since this
			// client sends nothing. Both are past any test run, so only the
			// close frame's deadline can close it.
			srv.PeerWriteTimeout = time.Hour
			srv.HandshakeTimeout = time.Hour
			joined := make(chan struct{}, 1)
			srv.OnFirstPeer = func(_ context.Context, _ string) {
				select {
				case joined <- struct{}{}:
				default:
				}
			}
			lastPeer := make(chan struct{}, 1)
			srv.OnLastPeer = func(_ context.Context, _ string) {
				select {
				case lastPeer <- struct{}{}:
				default:
				}
			}

			cancels := make(chan context.CancelCauseFunc, 1)
			ln := newPipeListener()
			ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx, cancel := context.WithCancelCause(r.Context())
				defer cancel(nil)
				cancels <- cancel
				srv.ServeHTTP(w, r.WithContext(ctx))
			}))
			ts.Listener = ln
			ts.Start()
			defer ts.Close()

			conn, _, err := pipeDialer(ln).Dial("ws://pipe/close-frame-deadline-room", nil)
			require.NoError(t, err)
			defer conn.Close()
			select {
			case <-joined:
			case <-expired:
				t.Fatal("the peer did not join its room before the test deadline")
			}

			cancel := <-cancels
			start := time.Now()
			cancel(closeCodeCause{code: 4001, reason: "session_revoked"})

			select {
			case <-lastPeer:
			case <-expired:
				t.Fatalf("the peer was not closed before the test deadline: the close frame "+
					"for the cancel did not give up at its %s deadline", c.want)
			}
			took := time.Since(start)
			if took < c.want {
				t.Fatalf("the peer was closed %s after the cancel, before its %s close-frame deadline",
					took.Round(time.Millisecond), c.want)
			}
			t.Logf("peer closed %s after the cancel, close-frame deadline %s",
				took.Round(time.Millisecond), c.want)
		})
	}
}

// beforeTestDeadline returns a channel that fires one second before the test
// binary's deadline, so a wait on it fails with a message naming what it
// waited for rather than with go test's timeout panic. With no deadline
// (-timeout 0) the channel is nil, and a wait on it lasts as long as go test.
func beforeTestDeadline(t *testing.T) <-chan time.Time {
	t.Helper()
	d, ok := t.Deadline()
	if !ok {
		return nil
	}
	return time.After(time.Until(d) - time.Second)
}
