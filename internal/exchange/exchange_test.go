package exchange

import (
	"encoding/json"
	"math/rand"
	"testing"
)

func cmd(c Command) []byte { b, _ := json.Marshal(c); return b }

func TestMatchingAndDedup(t *testing.T) {
	m := New()
	r := m.Apply(1, cmd(Command{ClientID: 1, Seq: 1, Op: OpNew, Account: 1, Side: Sell, Price: 100, Qty: 5}))
	if r.Events[len(r.Events)-1].Kind != EvRested {
		t.Fatalf("%+v", r)
	}
	r = m.Apply(2, cmd(Command{ClientID: 2, Seq: 1, Op: OpNew, Account: 2, Side: Buy, Price: 101, Qty: 3}))
	if r.Events[1].Kind != EvFill || r.Events[1].Price != 100 || r.Events[1].Other != 1 {
		t.Fatalf("%+v", r)
	}
	h := m.Hash()
	// Retrying the same (client, seq) at a later index returns the cached result and changes nothing.
	d := m.Apply(3, cmd(Command{ClientID: 2, Seq: 1, Op: OpNew, Account: 2, Side: Buy, Price: 101, Qty: 3}))
	if !d.Duplicate || d.Index != 2 || m.Hash() != h || m.Dups != 1 {
		t.Fatalf("%+v dups=%d", d, m.Dups)
	}
	// Older seq is stale.
	s := m.Apply(4, cmd(Command{ClientID: 2, Seq: 0, Op: OpNew, Account: 2, Side: Buy, Price: 1, Qty: 1}))
	if !s.Duplicate || s.Events[0].Reason != "stale_seq" {
		t.Fatalf("%+v", s)
	}
	// Cancel by non-owner is rejected; by owner succeeds with the remaining qty.
	if r := m.Apply(5, cmd(Command{ClientID: 3, Seq: 1, Op: OpCancel, Account: 9, OrderID: 1})); r.Events[0].Reason != "not_owner" {
		t.Fatalf("%+v", r)
	}
	if r := m.Apply(6, cmd(Command{ClientID: 1, Seq: 2, Op: OpCancel, Account: 1, OrderID: 1})); r.Events[0].Kind != EvCancelled || r.Events[0].Qty != 2 {
		t.Fatalf("%+v", r)
	}
}

// Determinism: two machines applying the same commands reach the same hash; Clone is independent.
func TestDeterminismAndClone(t *testing.T) {
	a, b := New(), New()
	rnd := rand.New(rand.NewSource(1))
	for i := uint64(1); i <= 5000; i++ {
		c := Command{ClientID: uint64(1 + rnd.Intn(5)), Seq: i, Op: OpNew, Account: uint32(1 + rnd.Intn(3)),
			Side: Side(rnd.Intn(2)), Price: int64(95 + rnd.Intn(11)), Qty: int64(1 + rnd.Intn(9)), IOC: rnd.Intn(4) == 0}
		if rnd.Intn(5) == 0 {
			c = Command{ClientID: c.ClientID, Seq: i, Op: OpCancel, Account: c.Account, OrderID: uint64(1 + rnd.Intn(int(i)))}
		}
		a.Apply(i, cmd(c))
		b.Apply(i, cmd(c))
		if i == 2500 {
			cl := a.Clone()
			cl.Apply(9999, cmd(Command{ClientID: 77, Seq: 1, Op: OpNew, Side: Buy, Price: 200, Qty: 1000}))
		}
	}
	if a.Hash() != b.Hash() {
		t.Fatal("replicas diverged")
	}
	ba, aa := a.Book(100)
	bb, ab := b.Book(100)
	if len(ba) != len(bb) || len(aa) != len(ab) {
		t.Fatal("books differ")
	}
	if len(ba) > 0 && len(aa) > 0 && ba[0].Price >= aa[0].Price {
		t.Fatal("book crossed")
	}
}
