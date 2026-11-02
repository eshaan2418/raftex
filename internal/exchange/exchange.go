// Package exchange is the replicated state machine: a deterministic price-time matching engine
// plus per-client request deduplication. Every node applies the same committed commands in the
// same order and therefore reaches the same state; an order's id is the Raft log index of the
// command that created it, so ids are identical on every replica.
//
// Exactly-once: each command carries (ClientID, Seq). A client retries with the same Seq after a
// timeout or leader change; if the command was already applied, the cached result is returned
// instead of applying it again.
package exchange

import (
	"encoding/json"
	"hash/fnv"
	"sort"
)

// Side of an order.
type Side uint8

// Sides.
const (
	Buy Side = iota
	Sell
)

// Command kinds.
const (
	OpNew    = "new"
	OpCancel = "cancel"
)

// Command is what clients submit and Raft replicates.
type Command struct {
	ClientID uint64 `json:"c"`
	Seq      uint64 `json:"q"`
	Op       string `json:"op"`
	Account  uint32 `json:"a"`
	Side     Side   `json:"s"`
	Price    int64  `json:"p"` // ticks; 0 = market
	Qty      int64  `json:"n"`
	IOC      bool   `json:"ioc"`
	OrderID  uint64 `json:"id"` // cancel target
}

// Event kinds.
const (
	EvAccepted  = "accepted"
	EvRejected  = "rejected"
	EvFill      = "fill"
	EvRested    = "rested"
	EvCancelled = "cancelled"
)

// Event is one outcome of a command.
type Event struct {
	Kind    string `json:"k"`
	OrderID uint64 `json:"id"`
	Other   uint64 `json:"o,omitempty"`
	Price   int64  `json:"p,omitempty"`
	Qty     int64  `json:"n,omitempty"`
	Reason  string `json:"r,omitempty"`
}

// Result is the deterministic output of applying one command.
type Result struct {
	Index     uint64  `json:"index"` // log index at which the command was applied
	Duplicate bool    `json:"dup,omitempty"`
	Events    []Event `json:"events"`
}

type order struct {
	id      uint64
	account uint32
	side    Side
	price   int64
	qty     int64
}

type level struct {
	price  int64
	orders []*order // FIFO
}

type session struct {
	seq    uint64
	result Result
}

// Machine is the replicated exchange state.
type Machine struct {
	bids, asks []*level // bids descending, asks ascending (best first)
	byID       map[uint64]*order
	sessions   map[uint64]*session
	applied    uint64
	hash       uint64
	Applies    uint64
	Dups       uint64
}

// New returns an empty machine.
func New() *Machine {
	return &Machine{byID: map[uint64]*order{}, sessions: map[uint64]*session{}, hash: 14695981039346656037}
}

// Applied returns the last applied log index.
func (m *Machine) Applied() uint64 { return m.applied }

// Hash is a running FNV-1a over every applied (index, result): equal on all replicas that applied
// the same prefix.
func (m *Machine) Hash() uint64 { return m.hash }

// Apply applies the command encoded in data at log index idx. Malformed data is rejected
// deterministically (every replica reaches the same verdict).
func (m *Machine) Apply(idx uint64, data []byte) Result {
	m.applied = idx
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		r := Result{Index: idx, Events: []Event{{Kind: EvRejected, Reason: "malformed"}}}
		m.fold(r)
		return r
	}
	if s, ok := m.sessions[c.ClientID]; ok && c.ClientID != 0 {
		if c.Seq == s.seq {
			m.Dups++
			r := s.result
			r.Duplicate = true
			return r // not folded: a duplicate does not change state
		}
		if c.Seq < s.seq {
			m.Dups++
			return Result{Index: idx, Duplicate: true, Events: []Event{{Kind: EvRejected, Reason: "stale_seq"}}}
		}
	}
	m.Applies++
	var r Result
	switch c.Op {
	case OpNew:
		r = m.newOrder(idx, c)
	case OpCancel:
		r = m.cancel(idx, c)
	default:
		r = Result{Index: idx, Events: []Event{{Kind: EvRejected, Reason: "bad_op"}}}
	}
	if c.ClientID != 0 {
		m.sessions[c.ClientID] = &session{seq: c.Seq, result: r}
	}
	m.fold(r)
	return r
}

func (m *Machine) fold(r Result) {
	h := fnv.New64a()
	b, _ := json.Marshal(r)
	var x [8]byte
	for i := 0; i < 8; i++ {
		x[i] = byte(m.hash >> (8 * i))
	}
	_, _ = h.Write(x[:])
	_, _ = h.Write(b)
	m.hash = h.Sum64()
}

func (m *Machine) newOrder(idx uint64, c Command) Result {
	r := Result{Index: idx}
	if c.Qty <= 0 || c.Price < 0 || (c.Side != Buy && c.Side != Sell) || (c.Price == 0 && !c.IOC) {
		r.Events = append(r.Events, Event{Kind: EvRejected, OrderID: idx, Reason: "invalid"})
		return r
	}
	o := &order{id: idx, account: c.Account, side: c.Side, price: c.Price, qty: c.Qty}
	r.Events = append(r.Events, Event{Kind: EvAccepted, OrderID: idx, Price: c.Price, Qty: c.Qty})
	opp := &m.asks
	if c.Side == Sell {
		opp = &m.bids
	}
	for o.qty > 0 && len(*opp) > 0 {
		lv := (*opp)[0]
		if c.Price != 0 && ((c.Side == Buy && lv.price > c.Price) || (c.Side == Sell && lv.price < c.Price)) {
			break
		}
		mk := lv.orders[0]
		q := o.qty
		if mk.qty < q {
			q = mk.qty
		}
		o.qty -= q
		mk.qty -= q
		r.Events = append(r.Events, Event{Kind: EvFill, OrderID: idx, Other: mk.id, Price: lv.price, Qty: q})
		if mk.qty == 0 {
			lv.orders = lv.orders[1:]
			delete(m.byID, mk.id)
			if len(lv.orders) == 0 {
				*opp = (*opp)[1:]
			}
		}
	}
	if o.qty == 0 {
		return r
	}
	if c.IOC {
		r.Events = append(r.Events, Event{Kind: EvCancelled, OrderID: idx, Qty: o.qty, Reason: "ioc"})
		return r
	}
	m.rest(o)
	r.Events = append(r.Events, Event{Kind: EvRested, OrderID: idx, Price: o.price, Qty: o.qty})
	return r
}

func (m *Machine) rest(o *order) {
	side := &m.bids
	better := func(a, b int64) bool { return a > b }
	if o.side == Sell {
		side = &m.asks
		better = func(a, b int64) bool { return a < b }
	}
	i := sort.Search(len(*side), func(i int) bool { return !better((*side)[i].price, o.price) })
	if i < len(*side) && (*side)[i].price == o.price {
		(*side)[i].orders = append((*side)[i].orders, o)
	} else {
		*side = append(*side, nil)
		copy((*side)[i+1:], (*side)[i:])
		(*side)[i] = &level{price: o.price, orders: []*order{o}}
	}
	m.byID[o.id] = o
}

func (m *Machine) cancel(idx uint64, c Command) Result {
	r := Result{Index: idx}
	o, ok := m.byID[c.OrderID]
	if !ok {
		r.Events = append(r.Events, Event{Kind: EvRejected, OrderID: c.OrderID, Reason: "unknown_order"})
		return r
	}
	if o.account != c.Account {
		r.Events = append(r.Events, Event{Kind: EvRejected, OrderID: c.OrderID, Reason: "not_owner"})
		return r
	}
	side := &m.bids
	if o.side == Sell {
		side = &m.asks
	}
	for li, lv := range *side {
		if lv.price != o.price {
			continue
		}
		for k, x := range lv.orders {
			if x == o {
				lv.orders = append(lv.orders[:k], lv.orders[k+1:]...)
				break
			}
		}
		if len(lv.orders) == 0 {
			*side = append((*side)[:li], (*side)[li+1:]...)
		}
		break
	}
	delete(m.byID, o.id)
	r.Events = append(r.Events, Event{Kind: EvCancelled, OrderID: o.id, Qty: o.qty, Reason: "user"})
	return r
}

// Level is a public view of one price level.
type Level struct {
	Price  int64 `json:"price"`
	Qty    int64 `json:"qty"`
	Orders int   `json:"orders"`
}

// Book returns the top n levels per side.
func (m *Machine) Book(n int) (bids, asks []Level) {
	view := func(side []*level) []Level {
		var out []Level
		for i, lv := range side {
			if i >= n {
				break
			}
			var q int64
			for _, o := range lv.orders {
				q += o.qty
			}
			out = append(out, Level{lv.price, q, len(lv.orders)})
		}
		return out
	}
	return view(m.bids), view(m.asks)
}

// Clone deep-copies the machine (used by the linearizability model checker).
func (m *Machine) Clone() *Machine {
	c := New()
	c.applied, c.hash, c.Applies, c.Dups = m.applied, m.hash, m.Applies, m.Dups
	cp := func(side []*level) []*level {
		out := make([]*level, len(side))
		for i, lv := range side {
			nl := &level{price: lv.price, orders: make([]*order, len(lv.orders))}
			for k, o := range lv.orders {
				oc := *o
				nl.orders[k] = &oc
				c.byID[oc.id] = &oc
			}
			out[i] = nl
		}
		return out
	}
	c.bids, c.asks = cp(m.bids), cp(m.asks)
	for k, s := range m.sessions {
		sc := *s
		c.sessions[k] = &sc
	}
	return c
}
