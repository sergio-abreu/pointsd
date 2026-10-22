// Package ledger stores points entries.
//
// In production the Store is backed by Postgres (table ledger_entries).
// MemoryStore is the stand-in used for local runs and tests.
package ledger

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Kind string

const (
	KindAward    Kind = "award"
	KindReversal Kind = "reversal"
)

// Entry is one line of a member's points ledger.
type Entry struct {
	MemberID  string
	PaymentID string
	Kind      Kind
	Points    int64  // positive for awards, negative for reversals
	Month     string // settlement month of the payment, "2006-01" in UTC
}

// ErrUnavailable is returned when the ledger cannot be reached. Retrying may succeed.
var ErrUnavailable = errors.New("ledger: unavailable")

// ErrVersionConflict is returned when the version of the ledger does not match the expected version.
var ErrVersionConflict = errors.New("ledger: version conflict")

// ErrEntryExists is returned when an entry with the same paymentID and kind already exists.
var ErrEntryExists = errors.New("ledger: entry already exists")

// ErrEntryNotFound is returned when an entry with the given paymentID and kind is not found.
var ErrEntryNotFound = errors.New("ledger: entry not found")

type Store interface {
	GetVersion(ctx context.Context, memberID string) (int, error)
	GetEntry(ctx context.Context, paymentID string, kind Kind) (Entry, error)
	HasEntry(ctx context.Context, paymentID string, kind Kind) (bool, error)
	AwardedInMonth(ctx context.Context, memberID, month string) (int64, int, error)
	Append(ctx context.Context, e Entry, version int) error
	Balance(ctx context.Context, memberID string) (int64, error)
}

type Option func(*MemoryStore)

// WithLatency delays every call, approximating a database round trip.
func WithLatency(d time.Duration) Option {
	return func(s *MemoryStore) { s.latency = d }
}

// WithFaults installs a hook run at the start of every call (after the
// round trip). A non-nil error is returned to the caller and the call has no effect.
func WithFaults(f func(op string) error) Option {
	return func(s *MemoryStore) { s.faults = f }
}

type MemoryStore struct {
	latency time.Duration
	faults  func(op string) error

	mu       sync.Mutex
	entries  Entries
	balances Balances
}

type Entries map[string]Entry

func (e Entries) pk(paymentID string, kind Kind) string {
	return paymentID + ":" + string(kind)
}

func (e Entries) GetEntry(paymentID string, kind Kind) (Entry, bool) {
	entry, ok := e[e.pk(paymentID, kind)]
	return entry, ok
}

func (e Entries) Append(entry Entry) error {
	pk := e.pk(entry.PaymentID, entry.Kind)
	if _, ok := e[pk]; ok {
		return ErrEntryExists
	}
	e[pk] = entry
	return nil
}

type Balances struct {
	Overall OverallBalances
	Month   MonthBalances
	version map[string]int // optmistict lock version
}

func (b *Balances) Add(memberID string, month string, balance int64) {
	b.version[memberID] += 1
	b.Month.Add(memberID, month, balance)
	b.Overall.Add(memberID, balance)
}

type OverallBalances map[string]int64

func (b OverallBalances) pk(memberID string) string {
	return memberID
}

func (b OverallBalances) Get(memberID string) (int64, bool) {
	balance, ok := b[b.pk(memberID)]
	return balance, ok
}

func (b OverallBalances) Add(memberID string, balance int64) {
	b[b.pk(memberID)] += balance
}

type MonthBalances map[string]int64

func (b MonthBalances) pk(memberID string, month string) string {
	return memberID + ":" + month
}

func (b MonthBalances) Get(memberID string, month string) (int64, bool) {
	balance, ok := b[b.pk(memberID, month)]
	return balance, ok
}

func (b MonthBalances) Add(memberID string, month string, balance int64) {
	pk := b.pk(memberID, month)
	b[pk] += balance
}

func NewMemoryStore(opts ...Option) *MemoryStore {
	s := &MemoryStore{entries: Entries{}, balances: Balances{Overall: OverallBalances{}, Month: MonthBalances{}, version: make(map[string]int)}}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *MemoryStore) roundTrip(ctx context.Context, op string) error {
	if s.latency > 0 {
		select {
		case <-time.After(s.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.faults != nil {
		return s.faults(op)
	}
	return nil
}

func (s *MemoryStore) GetVersion(ctx context.Context, memberID string) (int, error) {
	if err := s.roundTrip(ctx, "get_version"); err != nil {
		return 0, err
	}
	return s.balances.version[memberID], nil
}

func (s *MemoryStore) GetEntry(ctx context.Context, paymentID string, kind Kind) (Entry, error) {
	if err := s.roundTrip(ctx, "get_entry"); err != nil {
		return Entry{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries.GetEntry(paymentID, kind)
	if !ok {
		return Entry{}, ErrEntryNotFound
	}
	return entry, nil
}

func (s *MemoryStore) HasEntry(ctx context.Context, paymentID string, kind Kind) (bool, error) {
	if err := s.roundTrip(ctx, "has_entry"); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.entries.GetEntry(paymentID, kind)
	return ok, nil
}

func (s *MemoryStore) AwardedInMonth(ctx context.Context, memberID, month string) (int64, int, error) {
	if err := s.roundTrip(ctx, "awarded_in_month"); err != nil {
		return 0, 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	balance, _ := s.balances.Month.Get(memberID, month)
	return balance, s.balances.version[memberID], nil
}

func (s *MemoryStore) Append(ctx context.Context, e Entry, version int) error {
	if err := s.roundTrip(ctx, "append"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.balances.version[e.MemberID] != version {
		return ErrVersionConflict
	}

	if err := s.entries.Append(e); err != nil {
		return err
	}
	s.balances.Add(e.MemberID, e.Month, e.Points)
	return nil
}

func (s *MemoryStore) Balance(ctx context.Context, memberID string) (int64, error) {
	if err := s.roundTrip(ctx, "balance"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	balance, _ := s.balances.Overall.Get(memberID)
	return balance, nil
}
