package crdt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Instrument the retained item graph at buildMergeStore's decoder boundary.
// This counts actual merge work without substituting a
// parent resolver. The public merge is then checked for lossless output.
func mergeGraphWithWork(t *testing.T, clients map[ClientID][]*Item, work *resolverWork, v2 bool) []byte {
	t.Helper()
	work.lengths = 0
	merged, ds, err := buildMergeStore([][]byte{{}}, func(*Doc, []byte) (map[ClientID][]*Item, DeleteSet, error) {
		return clients, newDeleteSet(), nil
	})
	require.NoError(t, err)
	encode, merge := encodeStructStoreV1, MergeUpdatesV1
	if v2 {
		encode, merge = encodeStructStoreV2, MergeUpdatesV2
	}
	n := 0
	for _, items := range clients {
		n += len(items)
	}
	require.LessOrEqual(t, work.lengths, 8*n, "merging must inspect each struct a bounded number of times")
	// The wire encoder dispatches on concrete content types. Unwrap only
	// after the merge work has been counted, then exercise the public codec.
	for _, items := range merged {
		for _, item := range items {
			if wrapped, ok := item.Content.(*resolverWorkContent); ok {
				item.Content = wrapped.Content
			}
		}
	}
	wire := encode(merged, ds, nil)
	public, err := merge(wire)
	require.NoError(t, err)
	require.Equal(t, wire, public, "merging must preserve the origin graph")
	return public
}

func TestUnit_MergeUpdates_InterleavedChainBoundWork(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "V1", true: "V2"}[v2], func(t *testing.T) {
			const people, n = 5, 2000
			source := New(WithClientID(100001))
			defer source.Destroy()
			work := resolverWork{}
			first, rest := interleavedWorkItems(source, people, n, &work)
			clients := make(map[ClientID][]*Item)
			for _, item := range append([]*Item{first}, rest...) {
				item.Content = &resolverWorkContent{NewContentString("x"), &work}
				if item != first {
					item.Parent = nil
				}
				clients[item.ID.Client] = append(clients[item.ID.Client], item)
			}
			wire := mergeGraphWithWork(t, clients, &work, v2)
			dst := New(WithClientID(100002))
			defer dst.Destroy()
			text := dst.GetText("text")
			apply := ApplyUpdateV1
			if v2 {
				apply = ApplyUpdateV2
			}
			require.NoError(t, apply(dst, wire, nil))
			require.Equal(t, n, text.Len())
			require.Zero(t, dst.PendingStats().Items)
			for client := ClientID(1); client <= people; client++ {
				require.Equal(t, uint64(n/people), dst.StateVector().Clock(client))
				for _, item := range dst.store.clients[client] {
					require.Equal(t, text.baseType(), item.Parent)
				}
			}
		})
	}
}

func TestUnit_MergeUpdates_OriginCyclesBoundWork(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "V1", true: "V2"}[v2], func(t *testing.T) {
			const n = 3000
			work := resolverWork{}
			clients := make(map[ClientID][]*Item)
			origins := make(map[ClientID]ClientID)
			add := func(client, origin ClientID) {
				origins[client] = origin
				clients[client] = append(clients[client], &Item{ID: ID{Client: client}, Origin: &ID{Client: origin}, Content: &resolverWorkContent{NewContentDeleted(1), &work}})
			}
			for c := ClientID(1); c <= n; c++ {
				add(c, c)
			}
			for c := ClientID(n + 1); c <= 2*n; c += 2 {
				add(c, c+1)
				add(c+1, c)
			}
			const loop = ClientID(4 * n)
			for c := loop; c < loop+n; c++ {
				add(c, c+1)
			}
			add(loop+n, loop+n/2)
			wire := mergeGraphWithWork(t, clients, &work, v2)
			decode := decodeStructsV1
			if v2 {
				decode = decodeStructsV2
			}
			decoded, _, err := decode(New(), wire)
			require.NoError(t, err)
			require.Len(t, decoded, len(origins))
			for client, items := range clients {
				require.Len(t, decoded[client], len(items))
				for i := range items {
					got := decoded[client][i]
					require.Equal(t, ID{Client: client}, got.ID)
					require.Equal(t, &ID{Client: origins[client]}, got.Origin)
					require.Nil(t, got.Parent, "cycles must remain unrooted")
					require.Equal(t, uint64(1), got.Content.Len())
				}
			}
		})
	}
}
