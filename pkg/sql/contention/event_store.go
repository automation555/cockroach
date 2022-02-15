// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contention

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	// TODO(azhng): wip: tune these numbers
	eventBatchSize   = 64
	eventChannelSize = 24
)

// eventWriter provides interfaces to write contention event into eventStore.
type eventWriter interface {
	addEvent(roachpb.ContentionEvent)
}

// eventReader provides interface to read contention events from eventStore.
type eventReader interface {
	forEachEvent(func(*contentionpb.ExtendedContentionEvent) error) error
}

// eventBatch is used to batch up multiple contention events to amortize the
// cost of acquiring a mutex.
type eventBatch [eventBatchSize]contentionpb.ExtendedContentionEvent

func (b *eventBatch) len() int {
	for i := 0; i < eventBatchSize; i++ {
		if !b[i].Valid() {
			return i
		}
	}
	return eventBatchSize
}

var eventBatchPool = &sync.Pool{
	New: func() interface{} {
		return &eventBatch{}
	},
}

// eventStore is a contention event store that performs asynchronous contention
// event collection. It subsequently resolves the transaction ID reported in the
// contention event into transaction fingerprint ID.
// eventStore relies on two background goroutines:
// 1. intake goroutine: this goroutine is responsible for inserting batched
//    contention events into the in-memory store, and then queue the batched
//    events into the resolver. This means that the contention events can be
//    immediately visible as early as possible to the readers of the eventStore
//    before the txn id resolution is performed.
// 2. resolver goroutine: this goroutine runs on a timer (controlled via
//    sql.contention.event_store.resolution_interval cluster setting).
//    Periodically, the timer fires and resolver attempts to contact remote
//    nodes to resolve the transaction IDs in the queued contention events
//    into transaction fingerprint IDs. If the attempt is successful, the
//    resolver goroutine will update the stored contention events with the
//    transaction fingerprint IDs.
type eventStore struct {
	st *cluster.Settings

	guard struct {
		*contentionutils.ConcurrentBufferGuard

		// buffer is used to store a batch of contention events to amortize the
		// cost of acquiring mutex. It is used in conjunction with the concurrent
		// buffer guard.
		buffer *eventBatch
	}

	eventBatchChan chan *eventBatch
	closeCh        chan struct{}

	resolver resolverQueue

	mu struct {
		syncutil.RWMutex

		// store is the main in-memory FIFO contention event store.
		// TODO(azhng): wip: might need to implement using map[] if the benchmark
		//  is too bad.
		store *cache.UnorderedCache
	}

	atomic struct {
		// storageSize is used to determine when to start evicting the older
		// contention events.
		storageSize int64
	}
}

var (
	_ eventWriter = &eventStore{}
	_ eventReader = &eventStore{}
)

func newEventStore(st *cluster.Settings, endpoint ResolverEndpoint) *eventStore {
	s := &eventStore{
		st:             st,
		resolver:       newResolver(endpoint, eventBatchSize /* sizeHint */),
		eventBatchChan: make(chan *eventBatch, eventChannelSize),
		closeCh:        make(chan struct{}),
	}

	s.mu.store = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(_ int, _, _ interface{}) bool {
			capacity := StoreCapacity.Get(&st.SV)
			size := atomic.LoadInt64(&s.atomic.storageSize)
			return size > capacity
		},
		OnEvictedEntry: func(entry *cache.Entry) {
			entrySize := int64(entry.Value.(*contentionpb.ExtendedContentionEvent).Size())
			atomic.AddInt64(&s.atomic.storageSize, -entrySize)
		},
	})

	s.guard.buffer = eventBatchPool.Get().(*eventBatch)
	s.guard.ConcurrentBufferGuard = contentionutils.NewConcurrentBufferGuard(
		func() int64 {
			return eventBatchSize
		}, /* limiter */
		func(currentWriterIndex int64) {
			select {
			case s.eventBatchChan <- s.guard.buffer:
			case <-s.closeCh:
			}
			s.guard.buffer = eventBatchPool.Get().(*eventBatch)
		}, /* onBufferFullSync */
	)

	return s
}

// start runs both background goroutines used by eventStore.
func (s *eventStore) start(ctx context.Context, stopper *stop.Stopper) {
	s.startResolver(ctx, stopper)
	s.startEventIntake(ctx, stopper)
}

func (s *eventStore) startEventIntake(ctx context.Context, stopper *stop.Stopper) {
	handleInsert := func(batch []contentionpb.ExtendedContentionEvent) {
		s.resolver.enqueue(batch)
		s.upsertBatch(batch)
	}

	consumeBatch := func(batch *eventBatch) {
		batchLen := batch.len()
		handleInsert(batch[:batchLen])
		*batch = eventBatch{}
		eventBatchPool.Put(batch)
	}

	if err := stopper.RunAsyncTask(ctx, "contention-event-intake", func(ctx context.Context) {
		for {
			select {
			case batch := <-s.eventBatchChan:
				consumeBatch(batch)
			case <-stopper.ShouldQuiesce():
				close(s.closeCh)
				return
			}
		}
	}); err != nil {
		close(s.closeCh)
	}
}

func (s *eventStore) startResolver(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "contention-event-resolver", func(ctx context.Context) {
		// Handles resolution interval changes.
		var resolutionIntervalChanged = make(chan struct{}, 1)
		TxnIDResolutionInterval.SetOnChange(&s.st.SV, func(ctx context.Context) {
			select {
			case resolutionIntervalChanged <- struct{}{}:
			default:
			}
		})

		initialDelay := s.nextResolutionInterval()
		timer := timeutil.NewTimer()
		timer.Reset(initialDelay)

		for {
			waitInterval := s.nextResolutionInterval()
			timer.Reset(waitInterval)

			select {
			case <-timer.C:
				if err := s.flushAndResolve(ctx); err != nil {
					if log.V(1) {
						log.Warningf(ctx, "unexpected error encountered when performing "+
							"txn id resolution %s", err)
					}
				}
				timer.Read = true
			case <-resolutionIntervalChanged:
				continue
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// addEvent implements the eventWriter interface.
func (s *eventStore) addEvent(e roachpb.ContentionEvent) {
	if TxnIDResolutionInterval.Get(&s.st.SV) == 0 {
		return
	}
	s.guard.AtomicWrite(func(writerIdx int64) {
		s.guard.buffer[writerIdx] = contentionpb.ExtendedContentionEvent{
			BlockingEvent: e,
			CollectionTs:  timeutil.Now(),
		}
	})
}

// forEachEvent implements the eventReader interface.
func (s *eventStore) forEachEvent(
	op func(event *contentionpb.ExtendedContentionEvent) error,
) error {
	// First we read all the keys in the eventStore, and then immediately release
	// the read lock. This is to minimize the time we need to hold the lock. This
	// is important since the op() callback can take arbitrary long to execute,
	// we should not be holding the lock while op() is executing.
	s.mu.RLock()
	keys := make([]uuid.UUID, 0, s.mu.store.Len())
	s.mu.store.Do(func(entry *cache.Entry) {
		keys = append(keys, entry.Key.(uuid.UUID))
	})
	s.mu.RUnlock()

	for i := range keys {
		event, ok := s.getEventByBlockingTxnID(keys[i])
		if !ok {
			// The event might have been evicted between reading the keys and
			// getting the event. In this case we simply ignore it.
			continue
		}
		if err := op(&event); err != nil {
			return err
		}
	}

	return nil
}

func (s *eventStore) getEventByBlockingTxnID(
	txnID uuid.UUID,
) (_ contentionpb.ExtendedContentionEvent, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	event, ok := s.mu.store.Get(txnID)
	return event.(contentionpb.ExtendedContentionEvent), ok
}

// flushAndResolve is the main method called by the resolver goroutine each
// time the timer fires. This method does a few things:
// 1. it triggers the batching buffer to flush its content into the intake
//    goroutine. This is to ensure that in the case where we have very low
//    rate of contentions, the contention events won't be permanently trapped
//    in the batching buffer.
// 2. it invokes the resolve() method on the resolverQueue. See inline comments
//    on the method for details.
// 3. it uses the result from the resolver to update the stored contention
//    events with more details.
func (s *eventStore) flushAndResolve(ctx context.Context) error {
	// This forces the write-buffer flushes its batch into resolverQueue.
	s.guard.ForceSync()

	err := s.resolver.resolve(ctx)

	// Ensure that all the resolved contention events are dequeued from the
	// resolver before we bubble up the error.
	s.upsertBatch(s.resolver.dequeue())

	return err
}

// upsertBatch update or insert a batch of contention events into the in-memory
// store
func (s *eventStore) upsertBatch(events []contentionpb.ExtendedContentionEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range events {
		blockingTxnID := events[i].BlockingEvent.TxnMeta.ID
		_, ok := s.mu.store.Get(blockingTxnID)
		if !ok {
			atomic.AddInt64(&s.atomic.storageSize, int64(events[i].Size()+uuid.Size))
		}
		s.mu.store.Add(blockingTxnID, events[i])
	}
}

func (s *eventStore) nextResolutionInterval() time.Duration {
	baseInterval := TxnIDResolutionInterval.Get(&s.st.SV)

	// Jitter the interval a by +/- 15%.
	frac := 1 + (2*rand.Float64()-1)*0.15
	jitteredInterval := time.Duration(frac * float64(baseInterval.Nanoseconds()))
	return jitteredInterval
}