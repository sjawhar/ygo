package crdt

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/reearth/ygo/encoding"
)

// malformedV2Preload is a V1 update of k delete ranges on clients nobody holds,
// which park in the receiving document.
func malformedV2Preload(k int) []byte {
	ds := newDeleteSet()
	for i := range k {
		ds.add(ID{Client: ClientID(500 + i), Clock: 0}, 1)
	}
	enc := encoding.NewEncoder()
	enc.WriteVarUint(0)
	encodeDeleteSet(enc, ds)
	return enc.Bytes()
}

// applyUpdateV2Landed is ApplyUpdateV1Landed for a V2 update: when the apply
// fails partway, landed is a V1 update of exactly what it integrated.
func applyUpdateV2Landed(doc *Doc, update []byte) (landed []byte, err error) {
	doc.Transact(func(txn *Transaction) {
		txn.Local = false
		if err = applyV2Txn(txn, update); err != nil {
			landed = encodeTxnV1Locked(txn)
		}
	}, nil)
	return landed, err
}

// relayCase applies update to a room at limit with preload parked delete
// ranges and relays what the websocket provider relays: the update when it
// applies, the landed bytes when it fails partway, nothing when nothing landed.
// It returns why a peer that held the room's state before and applies the
// relay differs from the room, or "" when they are equal.
func relayCase(t *testing.T, update []byte, limit, preload int) (diverged string, landed []byte, applyErr error) {
	t.Helper()
	room := New(WithClientID(77))
	peer := New(WithClientID(78))
	defer room.Destroy()
	defer peer.Destroy()
	for _, d := range []*Doc{room, peer} {
		if preload > 0 {
			if err := ApplyUpdateV1(d, malformedV2Preload(preload), nil); err != nil {
				t.Fatalf("preload: %v", err)
			}
		}
	}
	room.SetMaxPendingItems(limit)
	landed, applyErr = applyUpdateV2Landed(room, update)
	var peerErr error
	switch {
	case applyErr == nil:
		peerErr = ApplyUpdateV2(peer, update, nil)
	case landed != nil:
		peerErr = ApplyUpdateV1(peer, landed, nil)
	}
	var why []string
	if peerErr != nil {
		why = append(why, "peer refused the relay: "+peerErr.Error())
	}
	if !bytes.Equal(EncodeStateAsUpdateV1(room, nil), EncodeStateAsUpdateV1(peer, nil)) {
		why = append(why, "state differs")
	}
	if rp, pp := room.PendingStats(), peer.PendingStats(); !reflect.DeepEqual(rp, pp) {
		why = append(why, fmt.Sprintf("pending differs: room %+v, peer %+v", rp, pp))
		if rp.Items > pp.Items {
			why = append(why, parkedNotRelayed)
		}
	}
	return strings.Join(why, "; "), landed, applyErr
}

// parkedNotRelayed marks a divergence in which the room parked items of the
// failed update that the relay does not carry.
const parkedNotRelayed = "the room parked items the relay does not carry"

// Each malformed V2 update in testdata/malformed_v2 reaches the apply at its
// cap, where before the delete-set walk refused it first. A peer that applies
// what the room relays must end equal to the room: EncodeStateAsUpdateV1
// byte-equal and PendingStats equal. The same update at a cap of 1<<20, where
// the walk never ran, is the control.
func TestMalformedV2_RelayedLandedBytesConvergeThePeer(t *testing.T) {
	dir := filepath.Join("testdata", "malformed_v2")
	f, err := os.Open(filepath.Join(dir, "cases.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var cases, failedLanded, failedNothing, applied, diverged, controlDiverged int
	kinds := map[string]int{}
	shown := 0
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		name := fields[0]
		limit, _ := strconv.Atoi(fields[1])
		preload, _ := strconv.Atoi(fields[2])
		update, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		cases++
		why, landed, applyErr := relayCase(t, update, limit, preload)
		switch {
		case applyErr == nil:
			applied++
		case landed != nil:
			failedLanded++
		default:
			failedNothing++
		}
		if why != "" {
			diverged++
			kind := "landed bytes relayed"
			if landed == nil {
				kind = "nothing relayed"
			}
			if strings.Contains(why, parkedNotRelayed) {
				kind += ", room parked items"
			}
			kinds[kind]++
			if shown < 10 {
				shown++
				t.Errorf("%s cap %d preload %d (apply err %v, %d landed bytes): %s", name, limit, preload, applyErr, len(landed), why)
			}
		}
		if why, _, _ := relayCase(t, update, 1<<20, preload); why != "" {
			controlDiverged++
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if cases == 0 {
		t.Fatal("no cases read")
	}
	t.Logf("%d cases: %d failed with landed bytes, %d failed with nothing landed, %d applied", cases, failedLanded, failedNothing, applied)
	t.Logf("diverged at the case's cap: %d %v; at cap 1<<20 (control): %d", diverged, kinds, controlDiverged)
	if diverged > 0 {
		t.Errorf("%d of %d cases leave the peer unequal to the room", diverged, cases)
	}
}
