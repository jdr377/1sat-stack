package overlay

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/b-open-io/1sat-stack/pkg/store"
	"github.com/b-open-io/1sat-stack/pkg/worker"
	"github.com/bsv-blockchain/go-sdk/chainhash"
)

type scriptedBatchStore struct {
	store.Store

	mu      sync.Mutex
	pages   [][]store.ScoredMember
	search  int
	removed atomic.Int32
}

func (s *scriptedBatchStore) Search(context.Context, *store.SearchCfg) ([]store.ScoredMember, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.search >= len(s.pages) {
		return nil, nil
	}
	page := s.pages[s.search]
	s.search++
	return page, nil
}

func (s *scriptedBatchStore) ZRem(context.Context, []byte, ...[]byte) error {
	s.removed.Add(1)
	return nil
}

func TestOverlaySyncBatchDirectResetWhileHandlerActive(t *testing.T) {
	for _, concurrency := range []int{1, 4} {
		t.Run(fmt.Sprintf("concurrency_%d", concurrency), func(t *testing.T) {
			var txid chainhash.Hash
			txid[0] = 1
			firstMember := string(append(txid[:], 0, 0, 0, 0))
			secondMember := string(append(txid[:], 1, 0, 0, 0))
			queue := &scriptedBatchStore{pages: [][]store.ScoredMember{
				{{Member: []byte(firstMember), Score: 1}},
				{{Member: []byte(secondMember), Score: 2}},
			}}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			firstActive := make(chan struct{})
			releaseFirst := make(chan struct{})
			secondBatchStarted := make(chan struct{})
			var batchCount atomic.Int32
			var submissions atomic.Int32
			var firstOnce sync.Once
			var secondBatchOnce sync.Once

			w := worker.New(&worker.Config{
				Store:     queue,
				Key:       "q:test",
				Limiter:   make(chan struct{}, concurrency),
				PageSize:  1,
				PollDelay: time.Millisecond,
				Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
				BatchHandler: func() worker.Handler {
					directSeen := &sync.Map{}
					if batchCount.Add(1) == 2 {
						secondBatchOnce.Do(func() { close(secondBatchStarted) })
					}
					return func(context.Context, string, float64) error {
						return processDirectOnce(directSeen, &txid, func() error {
							call := submissions.Add(1)
							if call == 1 {
								firstOnce.Do(func() { close(firstActive) })
								<-releaseFirst
							}
							return nil
						})
					}
				},
			})

			workerDone := make(chan error, 1)
			go func() { workerDone <- w.Start(ctx) }()

			awaitSignal(t, firstActive, "first direct handler to become active")
			awaitSignal(t, secondBatchStarted, "second batch reset while first handler is active")
			close(releaseFirst)
			awaitCount(t, func() int32 { return queue.removed.Load() }, 2, "queue removals")
			cancel()
			select {
			case <-workerDone:
			case <-time.After(5 * time.Second):
				t.Fatal("worker did not stop")
			}

			if got := submissions.Load(); got != 2 {
				t.Fatalf("submissions = %d, want 2 (one for each batch)", got)
			}
		})
	}
}

func TestOverlaySyncBatchDirectConcurrentDedup(t *testing.T) {
	directSeen := &sync.Map{}
	var txid chainhash.Hash
	txid[0] = 2

	var submissions atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := processDirectOnce(directSeen, &txid, func() error {
				submissions.Add(1)
				return nil
			}); err != nil {
				t.Errorf("processDirectOnce: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := submissions.Load(); got != 1 {
		t.Fatalf("submissions = %d, want 1 within a batch", got)
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func awaitCount(t *testing.T, current func() int32, want int32, description string) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for current() < want {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s: got %d, want %d", description, current(), want)
		default:
			runtime.Gosched()
		}
	}
}
