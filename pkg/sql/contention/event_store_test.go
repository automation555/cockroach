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
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestEventStore(t *testing.T) {
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	// Disable automatic txn id resolution to prevent interference.
	TxnIDResolutionInterval.Override(ctx, &st.SV, time.Hour)
	statusServer := newFakeStatusServerCluster()

	store := newEventStore(st, statusServer.txnIDResolution)
	store.start(ctx, stopper)

	// Up to 300 events.
	testSize := rand.Intn(300)

	// Up to 10 nodes.
	numOfCoordinators := rand.Intn(10)

	t.Logf("initializing %d events with %d distinct coordinators",
		testSize, numOfCoordinators)

	// Randomize input.
	testCases := randomlyGenerateTestCases(testSize, numOfCoordinators)
	populateFakeStatusServerCluster(statusServer, testCases)

	input, expected := generateUnresolvedContentionEventsFromTestCases(testCases)
	expectedMap := eventSliceToMap(expected)

	for _, event := range input {
		store.addEvent(event.BlockingEvent)
	}

	// The contention event should immediately be available to be read from
	// the event store (after synchronization). However, certain information will
	// remain unavailable until the resolution is performed.
	testutils.SucceedsWithin(t, func() error {
		store.guard.ForceSync()
		numOfEntries := 0

		if err := store.forEachEvent(func(actual *contentionpb.ExtendedContentionEvent) error {
			numOfEntries++
			expectedEvent, ok := expectedMap[actual.BlockingEvent.TxnMeta.ID]
			if !ok {
				return errors.Newf("expected to found contention event "+
					"with txnID=%s, but it was not found", actual.BlockingEvent.TxnMeta.ID.String())
			}
			if !actual.CollectionTs.After(expectedEvent.CollectionTs) {
				return errors.Newf("expected collection timestamp for the event to "+
					"be at least %s, but it is %s",
					expectedEvent.CollectionTs.String(), actual.CollectionTs.String())
			}
			if actual.BlockingTxnFingerprintID != roachpb.InvalidTransactionFingerprintID {
				return errors.Newf("expect blocking txn fingerprint id to be invalid, "+
					"but it is %d", actual.BlockingTxnFingerprintID)
			}
			if actual.WaitingTxnFingerprintID != roachpb.InvalidTransactionFingerprintID {
				return errors.Newf("expect waiting txn fingerprint id to be invalid, "+
					"but it is %d", actual.WaitingTxnFingerprintID)
			}
			return nil
		}); err != nil {
			return err
		}

		if numOfEntries != len(expectedMap) {
			return errors.Newf("expect to encounter %d events, but only %d events "+
				"were encountered", len(expectedMap), numOfEntries)
		}

		return nil
	}, 3*time.Second)

	// Since we are using the fake status server, there should not be any
	// errors.
	require.NoError(t, store.flushAndResolve(ctx))

	require.NoError(t, store.forEachEvent(
		func(actual *contentionpb.ExtendedContentionEvent) error {
			expectedEvent, ok := expectedMap[actual.BlockingEvent.TxnMeta.ID]
			require.True(t, ok, "expected to found resolved contention event "+
				"with txnID=%s, but it was not found", actual.BlockingEvent.TxnMeta.ID.String())
			assertEventEqual(t, expectedEvent, *actual)
			return nil
		}))
}

func eventSliceToMap(
	events []contentionpb.ExtendedContentionEvent,
) map[uuid.UUID]contentionpb.ExtendedContentionEvent {
	result := make(map[uuid.UUID]contentionpb.ExtendedContentionEvent)

	for _, ev := range events {
		result[ev.BlockingEvent.TxnMeta.ID] = ev
	}

	return result
}

func randomlyGenerateTestCases(testSize int, numOfCoordinator int) []testCase {
	tcs := make([]testCase, 0, testSize)
	for i := 0; i < testSize; i++ {
		tcs = append(tcs, testCase{
			ResolvedTxnID: contentionpb.ResolvedTxnID{
				TxnID:            uuid.FastMakeV4(),
				TxnFingerprintID: roachpb.TransactionFingerprintID(math.MaxUint64 - uint64(i)),
			},
			coordinatorNodeID: int32(rand.Intn(numOfCoordinator)),
		})
	}

	return tcs
}

func assertEventEqual(t *testing.T, expected, actual contentionpb.ExtendedContentionEvent) {
	t.Helper()
	// CollectionTs is generated at the runtime. It's tricky to assert its value.
	// We simply assert that it's nonzero and zero it out afterwards.
	require.True(t, actual.CollectionTs.After(expected.CollectionTs),
		"expected collection timestamp for the event to be non-zero, but it is")

	expected.CollectionTs = time.Time{}
	actual.CollectionTs = time.Time{}

	require.Equal(t, expected, actual)
}