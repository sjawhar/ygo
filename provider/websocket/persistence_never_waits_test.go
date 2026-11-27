package websocket_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/reearth/ygo/crdt"
	ygws "github.com/reearth/ygo/provider/websocket"
	"github.com/stretchr/testify/require"
)

// persistBuffer is the size of a room's persistence channel (loadRoom): the
// number of committed updates the worker can be behind before the observer's
// handoff had to wait.
const persistBuffer = 256

// A goroutine that commits into a room never waits on the room's persistence
// worker, however far behind the worker is: the buffer exists so that a slow
// storage backend does not block the Transact caller (N-H7). The worker is
// parked inside its first store, as one is whose adapter waits on something the
// committer holds, and the test commits well past the buffer from one goroutine;
// before the fix, the commit after the buffer filled blocked in the observer's
// send, and the committer and the worker waited on each other for good. Once the
// worker is let go it stores every update, in the order they were committed,
// and the room's flush barrier covers them all.
func TestPersistence_ACommitNeverWaitsOnABehindWorker(t *testing.T) {
	const commits = persistBuffer * 3
	for _, tc := range []struct {
		name     string
		coalesce time.Duration
	}{
		{"coalescing enabled (default)", time.Millisecond},
		{"coalescing disabled", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &ctxAdapter{gate: make(chan struct{}), entered: make(chan struct{})}
			s := ygws.NewServerWithPersistence(a)
			s.PersistCoalesceWindow = tc.coalesce
			ctx := context.Background()
			apply := func(mark string) error {
				return s.Apply(ctx, "room", func(_ *crdt.Doc, transact func(func(*crdt.Transaction))) {
					transact(func(txn *crdt.Transaction) {
						txt := txn.GetText("t")
						txt.Insert(txn, txt.Len(), mark, nil)
					})
				})
			}
			require.NoError(t, apply("a"))
			<-a.entered // the worker is parked inside its first store

			want := "a"
			committed := make(chan error, 1)
			go func() {
				for i := range commits {
					if err := apply(fmt.Sprintf("%d", i%10)); err != nil {
						committed <- err
						return
					}
				}
				committed <- nil
			}()
			for i := range commits {
				want += fmt.Sprintf("%d", i%10)
			}
			select {
			case err := <-committed:
				require.NoError(t, err)
			case <-time.After(20 * time.Second):
				close(a.gate)
				t.Fatalf("%d commits past a %d-slot buffer did not return while the worker was parked: a committer waited on the worker", commits, persistBuffer)
			}

			close(a.gate)
			require.NoError(t, s.CloseRoom("room", true))
			require.Equal(t, want, a.text(t, "t"), "the worker did not store every committed update, in commit order")
			sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			require.NoError(t, s.Shutdown(sctx))
		})
	}
}

// Updates queued beyond the buffer when the worker retires are still stored,
// in commit order, after what the worker's final drain stored: the feeder hands
// them to the retired room's stranded writer (#229's guarantee, which a queue
// beyond the channel must keep). Shutdown fires with the worker parked and the
// queue holding updates the channel never saw.
func TestPersistence_UpdatesQueuedBeyondTheBufferSurviveShutdown(t *testing.T) {
	const commits = persistBuffer * 2
	for _, tc := range []struct {
		name     string
		coalesce time.Duration
	}{
		{"coalescing enabled (default)", time.Millisecond},
		{"coalescing disabled", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &ctxAdapter{gate: make(chan struct{}), entered: make(chan struct{})}
			s := ygws.NewServerWithPersistence(a)
			s.PersistCoalesceWindow = tc.coalesce
			ctx := context.Background()
			want := ""
			apply := func(mark string) {
				require.NoError(t, s.Apply(ctx, "room", func(_ *crdt.Doc, transact func(func(*crdt.Transaction))) {
					transact(func(txn *crdt.Transaction) {
						txt := txn.GetText("t")
						txt.Insert(txn, txt.Len(), mark, nil)
					})
				}))
				want += mark
			}
			apply("a")
			<-a.entered
			for i := range commits {
				apply(fmt.Sprintf("%d", i%10))
			}

			shutdownDone := make(chan error, 1)
			go func() {
				sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				shutdownDone <- s.Shutdown(sctx)
			}()
			awaitShutdownRefusal(t, s)
			close(a.gate)
			select {
			case err := <-shutdownDone:
				require.NoError(t, err)
			case <-time.After(20 * time.Second):
				t.Fatal("Shutdown did not return")
			}
			require.Equal(t, want, a.text(t, "t"), "updates queued beyond the buffer at Shutdown were lost or reordered")
		})
	}
}
