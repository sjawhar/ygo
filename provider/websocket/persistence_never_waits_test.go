package websocket_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/reearth/ygo/crdt"
	ygws "github.com/reearth/ygo/provider/websocket"
	"github.com/stretchr/testify/require"
)

// persistBuffer is the size of a room's persistence channel (loadRoom): the
// number of committed updates the channel holds for the worker before the
// observer queues the next one for the room's feeder; before the fix, the
// observer's send waited there instead.
const persistBuffer = 256

// A commit into a room never waits on the room's running persistence worker,
// however far behind the worker is: the buffer exists so that a slow storage
// backend does not block the Transact caller (N-H7). The worker is parked
// inside its first store, as one is whose adapter waits on something the
// committer holds, and the test commits well past the buffer from one
// goroutine; before the fix, the commit after the buffer filled blocked in the
// observer's send, which with such an adapter left the committer and the
// worker waiting on each other for good. Once the worker is let go it stores
// every update, and the room's flush barrier covers them all; with one store
// per commit, it stores them in commit order.
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
			marks := []string{"a"}
			for i := range commits {
				marks = append(marks, fmt.Sprintf("%d", i%10))
			}
			require.NoError(t, apply(marks[0]))
			<-a.entered // the worker is parked inside its first store

			committed := make(chan error, 1)
			go func() {
				for _, mark := range marks[1:] {
					if err := apply(mark); err != nil {
						committed <- err
						return
					}
				}
				committed <- nil
			}()
			select {
			case err := <-committed:
				require.NoError(t, err)
			case <-time.After(20 * time.Second):
				close(a.gate)
				t.Fatalf("%d commits past a %d-slot buffer did not return while the worker was parked: a committer waited on the worker", commits, persistBuffer)
			}

			close(a.gate)
			require.NoError(t, s.CloseRoom("room", true))
			require.Equal(t, strings.Join(marks, ""), a.text(t, "t"), "the worker did not store every committed update")
			if tc.coalesce < 0 {
				requireStoredInCommitOrder(t, a.snapshot(), "t", marks)
			}
			sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			require.NoError(t, s.Shutdown(sctx))
		})
	}
}

// Updates queued beyond the buffer when the worker retires are still stored,
// after what the worker's final drain stored: the feeder hands them to the
// retired room's stranded writer (#229's guarantee, which a queue beyond the
// channel must keep), and with one store per commit they are stored in commit
// order. Shutdown fires with the worker parked and the queue holding updates
// the channel never saw.
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
			var marks []string
			apply := func(mark string) {
				require.NoError(t, s.Apply(ctx, "room", func(_ *crdt.Doc, transact func(func(*crdt.Transaction))) {
					transact(func(txn *crdt.Transaction) {
						txt := txn.GetText("t")
						txt.Insert(txn, txt.Len(), mark, nil)
					})
				}))
				marks = append(marks, mark)
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
			require.Equal(t, strings.Join(marks, ""), a.text(t, "t"), "updates queued beyond the buffer at Shutdown were lost")
			if tc.coalesce < 0 {
				requireStoredInCommitOrder(t, a.snapshot(), "t", marks)
			}
		})
	}
}

// snapshot returns the updates a has stored, in the order it stored them.
func (a *ctxAdapter) snapshot() [][]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([][]byte(nil), a.updates...)
}

// requireStoredInCommitOrder requires one stored update per commit, each
// commit having appended its mark to the text under key, and for every k that
// a replay of the first k stored updates holds exactly the first k marks. A
// replay of all of them cannot see order: a CRDT replay converges whatever
// order its updates come in. Here an update stored ahead of an earlier one
// waits in the replay for the update it was inserted after, so any swap shows
// as a short text.
func requireStoredInCommitOrder(t *testing.T, stored [][]byte, key string, marks []string) {
	t.Helper()
	require.Len(t, stored, len(marks), "expected one stored update per commit")
	d := crdt.New()
	want := ""
	for k, u := range stored {
		require.NoError(t, crdt.ApplyUpdateV1(d, u, nil))
		want += marks[k]
		require.Equal(t, want, d.GetText(key).ToString(),
			"after stored update %d of %d the replay does not hold exactly the first %d commits: updates were stored out of commit order", k+1, len(stored), k+1)
	}
}
