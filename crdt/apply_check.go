package crdt

import (
	"errors"
	"fmt"
)

// ErrApplyCheck is why a document with an apply check (WithApplyCheck)
// refuses a remote update: its check returned an error on what the update
// left the document holding. It wraps ErrInvalidUpdate, as every refusal of a
// bounded document does, and the apply returns it inside a RefusedUpdateError
// whose Err also wraps the check's own error: like every remote update a
// bounded document refuses, the update leaves the document as it was.
var ErrApplyCheck = fmt.Errorf("%w: the document's apply check refused what it leaves", ErrInvalidUpdate)

// ErrApplyCheckDrain is the error of a remote apply whose update the
// document's apply check passed and whose drain of the pending queue it
// refused (WithApplyCheck): the drain was taken back, and the update stands
// and has committed. The apply returns it wrapped with the check's own error.
// It does not wrap ErrInvalidUpdate and is not a RefusedUpdateError, since
// the update was taken: what was refused is what earlier updates parked.
var ErrApplyCheckDrain = errors.New("crdt: the document's apply check refused what the pending queue's drain lands; the drain was taken back and the update stands")

// errApplyCheckPanicked is the apply's error, inside the library, for a check
// that panicked: it rolls the apply back as a refusal does, and the panic
// itself reaches the caller once the transaction has committed
// (checkPanicOf).
var errApplyCheckPanicked = errors.New("crdt: the document's apply check panicked")

// ApplyView is the document as a remote apply has left it, for the
// document's apply check (WithApplyCheck) to read while the apply holds the
// document's lock. Every read method of the document's types takes that
// lock and so deadlocks in the check; a view's reads take none, and fill no
// cache of the document, so a read after a refusal finds nothing of the
// refused state.
//
// A view holds no transaction, so the check has nothing to write with: every
// write takes the transaction it writes in. It reads only the types of the
// document it was made for, since a type of another document is not under
// the lock the apply holds, and it reads nothing once the check has returned,
// since the document is no longer under that lock then. Either misuse panics.
//
// The reads of a type that take no lock work in a check too: Len,
// YXmlFragment.Children, YXmlElement.GetAttribute and GetAttributeValues, and
// the Parent of a type. A root type's handle is one the check resolved
// before, as with Doc.GetText: a view resolves no name, since naming a root
// is a write to the document.
type ApplyView struct {
	txn   *Transaction
	drain bool
}

// Drained is whether the check is judging what the apply's drain of the
// pending queue landed, and not what the update itself left. An apply's
// check runs at most twice: once on the update, before the drain, where the
// update changed the document, and once on the drain, where the drain landed
// something an earlier apply had parked. Drained is false on the first of
// those and true on the second. So the one run of an apply whose update
// changed nothing, because the document held all of it or its structs only
// parked, and whose drain landed something, is the drain's, and Drained says
// so. A run that sees false judges the update, whose refusal rolls the whole
// apply back; a run that sees true judges the drain, whose refusal takes back
// the drain alone while the update stands (ErrApplyCheckDrain).
func (v *ApplyView) Drained() bool {
	v.live()
	return v.drain
}

// ChangedTypes is the types the apply has changed so far, as
// Transaction.ChangedTypes names them: in the check of the update, the types
// the update changed, and in the check of a drain, those and the types the
// drain changed.
func (v *ApplyView) ChangedTypes() []SharedType {
	return v.live().ChangedTypes()
}

// TextString is t's text as the apply left it, as YText.ToString returns it.
// A YXmlText is read through the YText it embeds.
func (v *ApplyView) TextString(t *YText) string {
	v.reads(&t.abstractType)
	return t.toStringLocked()
}

// TextDelta is t's content as the apply left it, as YText.ToDelta returns
// it. A YXmlText is read through the YText it embeds.
func (v *ApplyView) TextDelta(t *YText) []Delta {
	v.reads(&t.abstractType)
	return t.toDeltaLocked()
}

// MapGet is m's value for key as the apply left it, and whether m holds one,
// as YMap.Get returns them.
func (v *ApplyView) MapGet(m *YMap, key string) (any, bool) {
	v.reads(&m.abstractType)
	return m.getLocked(key)
}

// MapEntries is m's live entries as the apply left them, as YMap.Entries
// returns them.
func (v *ApplyView) MapEntries(m *YMap) map[string]any {
	v.reads(&m.abstractType)
	return m.entriesLocked()
}

// ArraySlice is a's elements as the apply left them, as YArray.ToSlice
// returns them.
func (v *ApplyView) ArraySlice(a *YArray) []any {
	v.reads(&a.abstractType)
	return a.toSliceLocked()
}

// live is the transaction of the apply v was made for, and panics once the
// check that received v has returned.
func (v *ApplyView) live() *Transaction {
	if v.txn == nil {
		panic("crdt: ApplyView read after the apply check it was made for returned")
	}
	return v.txn
}

// reads panics unless t is a type of the document v was made for, read
// while its check runs.
func (v *ApplyView) reads(t *abstractType) {
	if txn := v.live(); t.doc != txn.doc {
		panic("crdt: ApplyView reads only the document its apply check runs on")
	}
}

// checkApply runs the apply check (WithApplyCheck) where a remote apply has
// integrated the update's structs and applied its delete set, before the
// drain of the pending queue. Its error is the apply's, and refuses the
// update: endRemoteApply rolls the whole apply back. An apply that changed
// nothing is not checked, and an apply with no journal has no check. Whether
// it changed anything is asked of its delete set and of the clocks of the
// clients it added or parked a struct for (applyJournal.touched), the only
// clients whose clocks it can have moved, so the question costs what the
// update carries and not the document's client count.
//
// It runs before the drain, and not once the drain is done, because no
// rollback of the whole apply can follow a drain that succeeded: the drain
// gives up its record of the parked items it changed (drainParked). The drain
// is checked on its own instead (checkDrain).
func checkApply(txn *Transaction) error {
	j := txn.applyJournal()
	if j == nil || j.check == nil || !txn.changesDocAmong(j.touched) {
		return nil
	}
	switch panicked, err := j.runCheck(txn, false); {
	case panicked:
		return errApplyCheckPanicked
	case err != nil:
		return fmt.Errorf("%w: %w", ErrApplyCheck, err)
	}
	return nil
}

// checkDrain runs the apply check on what the drain of the pending queue
// that began at point landed, when it landed anything. A drain that found
// neither parked structs nor parked deletes (queued false) lands nothing, so
// it is not judged and costs nothing more; one that found either is judged
// by drainLanded. A refusal is raised as a panic, which drainParked
// recovers, taking the drain back as it takes back a drain past its
// allowance: the parked structs are parked again as the drain found them,
// and the update that filled their gap stands. The apply then returns the
// refusal as ErrApplyCheckDrain (endRemoteApply).
func checkDrain(txn *Transaction, j *applyJournal, point *applyPoint, queued bool) {
	if j.check == nil || !queued || !drainLanded(txn, point) {
		return
	}
	switch panicked, err := j.runCheck(txn, true); {
	case panicked:
		panic(errApplyCheckPanicked)
	case err != nil:
		txn.boundedApply.drainRefused = fmt.Errorf("%w: %w", ErrApplyCheckDrain, err)
		panic(err)
	}
}

// drainLanded is whether the drain that began at point integrated a struct,
// which moves a client's next clock past point's state vector, or deleted an
// item, which adds to the span of the transaction's delete set. A drain only
// adds to both.
func drainLanded(txn *Transaction, point *applyPoint) bool {
	s := txn.doc.store
	for client := range s.clients {
		if s.NextClock(client) != point.sv.Clock(client) {
			return true
		}
	}
	return deletedSpan(txn.deleteSet) != deletedSpan(point.deleted)
}

// deletedSpan is the clocks ds's ranges cover together.
func deletedSpan(ds DeleteSet) uint64 {
	var n uint64
	for _, ranges := range ds.clients {
		for _, r := range ranges {
			n += r.Len
		}
	}
	return n
}

// runCheck calls j's check with txn's origin and a view of txn's document,
// drain telling the check whether it judges the drain (ApplyView.Drained),
// and returns its error. A panic in the check is kept on txn's apply state,
// to be raised once the transaction has committed and the lock is released
// (checkPanicOf), and reported as panicked, so the apply or its drain is
// taken back first, as for a refusal. The view reads nothing once the check
// has returned. The caller holds the document's lock.
func (j *applyJournal) runCheck(txn *Transaction, drain bool) (panicked bool, err error) {
	view := &ApplyView{txn: txn, drain: drain}
	defer func() {
		view.txn = nil
		if r := recover(); r != nil {
			txn.boundedApply.checkPanic = r
			panicked, err = true, nil
		}
	}()
	return false, j.check(txn.Origin, view)
}

// checkPanicOf is what txn's apply check panicked with, or nil, for the
// commit to raise once the apply has been taken back and the lock released
// (transactInternal).
func checkPanicOf(txn *Transaction) any {
	if txn.boundedApply == nil {
		return nil
	}
	return txn.boundedApply.checkPanic
}
