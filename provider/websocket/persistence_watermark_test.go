package websocket_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/crdt"
	ygws "github.com/reearth/ygo/provider/websocket"
)

// pacedAdapter stores only as fast as the test lets it. A store the worker
// issues on its cancellable context - each store of its running loop and, on
// the strict per-update path these tests use, of an on-demand flush - takes a
// token from step before it stores and reports on wrote after it; parked is
// closed when the first such store waits for its token. A store on
// context.Background - the worker's exit drain, an on-demand flush on the
// coalescing path, and every write for a retired worker (persistStranded) - is
// not paced, so a room's retirement never waits on the test. Closing free ends
// the pacing.
type pacedAdapter struct {
	storeLog

	step   chan struct{}
	wrote  chan struct{}
	parked chan struct{}
	once   sync.Once
	free   chan struct{}
}

func newPacedAdapter() *pacedAdapter {
	return &pacedAdapter{
		step:   make(chan struct{}),
		wrote:  make(chan struct{}),
		parked: make(chan struct{}),
		free:   make(chan struct{}),
	}
}

func (a *pacedAdapter) StoreUpdateContext(ctx context.Context, room string, update []byte) error {
	if ctx.Done() == nil {
		return a.StoreUpdate(room, update)
	}
	a.once.Do(func() { close(a.parked) })
	select {
	case <-a.step:
	case <-a.free:
		return a.StoreUpdate(room, update)
	}
	_ = a.StoreUpdate(room, update)
	select {
	case a.wrote <- struct{}{}:
	case <-a.free:
	}
	return nil
}

// storeOne lets one paced store through and waits until it has stored.
func (a *pacedAdapter) storeOne() {
	a.step <- struct{}{}
	<-a.wrote
}

// replayText returns the text under key in a fresh document that updates were
// applied to.
func replayText(t *testing.T, updates [][]byte, key string) string {
	t.Helper()
	d := crdt.New()
	for _, u := range updates {
		require.NoError(t, crdt.ApplyUpdateV1(d, u, nil))
	}
	return d.GetText(key).ToString()
}

// A flush barrier covers the updates committed before it and waits for no
// others: an update committed while it waits queues behind it rather than
// extending it. The worker stores one update for every two that a goroutine
// commits into the room's document, so the queue beyond the buffer grows for
// as long as that goroutine commits and never drains. CloseRoom must still
// return while the goroutine commits, with every update committed before
// CloseRoom was called already stored. A barrier that waited for the queue to
// drain would wait for as long as the goroutine commits.
func TestPersistence_FlushBarrierCoversOnlyEarlierCommits(t *testing.T) {
	a := newPacedAdapter()
	s := ygws.NewServerWithPersistence(a)
	s.PersistCoalesceWindow = -1 // one store per update, each paced by the test
	ctx := context.Background()
	require.NoError(t, s.Apply(ctx, "room", func(_ *crdt.Doc, transact func(func(*crdt.Transaction))) {
		transact(func(txn *crdt.Transaction) {
			txn.GetText("t").Insert(txn, 0, "a", nil)
		})
	}))
	<-a.parked // the worker is inside its first store, waiting for a token
	doc := s.GetDoc("room")
	require.NotNil(t, doc)

	marks := []string{"a"}
	commit := func() {
		mark := fmt.Sprintf("%d", len(marks)%10)
		doc.Transact(func(txn *crdt.Transaction) {
			txt := txn.GetText("t")
			txt.Insert(txn, txt.Len(), mark, nil)
		})
		marks = append(marks, mark)
	}
	for range 2 * persistBuffer {
		commit()
	}
	require.Equal(t, persistBuffer, ygws.PersistenceQueueLen(s, "room"),
		"half the commits fill the buffer, the other half queue beyond it")
	before := strings.Join(marks, "")

	stop := make(chan struct{})
	release := sync.OnceFunc(func() {
		close(stop)
		close(a.free)
	})
	closeStarted := make(chan struct{})
	closeDone := make(chan struct{})
	committerDone := make(chan struct{})
	t.Cleanup(func() {
		release()
		<-committerDone
		<-closeDone
	})

	var closeErr error
	go func() {
		defer close(closeDone)
		close(closeStarted)
		closeErr = s.CloseRoom("room", true)
	}()
	<-closeStarted

	var committed atomic.Int64
	go func() {
		defer close(committerDone)
		for {
			commit()
			commit()
			committed.Add(2)
			select {
			case a.step <- struct{}{}:
			case <-stop:
				return
			}
			select {
			case <-a.wrote:
			case <-stop:
				return
			}
		}
	}()

	select {
	case <-closeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("CloseRoom did not return while a goroutine kept committing into the room: its flush barrier waited for updates committed after it began")
	}
	stored := a.stored()
	during := committed.Load()
	require.NoError(t, closeErr)
	require.Greater(t, during, int64(persistBuffer),
		"the goroutine did not commit past the buffer while CloseRoom ran")
	require.True(t, strings.HasPrefix(replayText(t, stored, "t"), before),
		"CloseRoom returned before every update committed before it was stored")

	release()
	<-committerDone
	sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, s.Shutdown(sctx))
	requireStoredInCommitOrder(t, a.stored(), "t", marks)
}

// A peer's read loop waits, before it applies its next update, for the updates
// queued beyond the buffer when it began to wait and for no others: the feeder
// releases it once those are handed on, while updates committed after it began
// are still queued. A wait that lasted until the queue drained would still be
// waiting here, since the worker is parked again with the buffer full.
func TestPersistence_PeerWaitCoversOnlyEarlierUpdates(t *testing.T) {
	a := newPacedAdapter()
	release := sync.OnceFunc(func() { close(a.free) })
	t.Cleanup(release)
	s := ygws.NewServerWithPersistence(a)
	s.PersistCoalesceWindow = -1 // one store per update, each paced by the test
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
	<-a.parked // the worker is inside its first store, waiting for a token
	for i := range persistBuffer {
		apply(fmt.Sprintf("%d", i%10))
	}
	apply("x")
	apply("y")
	require.Equal(t, 2, ygws.PersistenceQueueLen(s, "room"), "two updates queue beyond the full buffer")

	wait := ygws.PersistenceWait(s, "room") // a peer's read loop begins to wait here
	apply("p")
	apply("q")
	apply("r")

	queuedAtRelease := make(chan int, 1)
	go func() {
		wait()
		queuedAtRelease <- ygws.PersistenceQueueLen(s, "room")
	}()
	// Each store frees a slot in the buffer, and the feeder fills it with the
	// oldest queued update: two stores hand on the two queued before the wait.
	// The worker then waits inside its next store with the buffer full again.
	a.storeOne()
	a.storeOne()
	select {
	case n := <-queuedAtRelease:
		require.Equal(t, 3, n, "the wait returned before the updates queued before it were handed on, or after later ones were")
	case <-time.After(10 * time.Second):
		t.Fatal("the wait did not return once the updates queued before it were handed on: it waited for updates committed after it began")
	}

	release()
	require.NoError(t, s.CloseRoom("room", true))
	requireStoredInCommitOrder(t, a.stored(), "t", marks)
	sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, s.Shutdown(sctx))
}
