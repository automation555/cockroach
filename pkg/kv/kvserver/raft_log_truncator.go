// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// pendingLogTruncations tracks proposed truncations for a replica that have
// not yet been enacted due to the corresponding RaftAppliedIndex not yet
// being durable. It is a field in the Replica struct
// (Replica.mu.pendingLogTruncations), but it is declared in this file since
// it is really part of the raftLogTruncator state that is per-replica.
type pendingLogTruncations struct {
	// We only track the oldest and latest pending truncation. We cannot track
	// only the latest since it may always be ahead of the durable
	// RaftAppliedIndex, and so we may never be able to truncate. We assume
	// liveness of durability advancement, which means that if no new pending
	// truncations are added, the latest one will eventually be enacted.
	//
	// Note that this liveness assumption is not completely true -- if there are
	// no writes happening to the store, the durability (due to memtable
	// flushes) may not advance. We deem this (a) an uninteresting case, since
	// if there are no writes we possibly don't care about aggressively
	// truncating the log, (b) fixing the liveness assumption is not within
	// scope of the truncator (it has to work with what it is given).
	truncs [2]pendingTruncation
}

func (p *pendingLogTruncations) computePostTruncLogSize(raftLogSize int64) int64 {
	p.iterate(func(trunc pendingTruncation) {
		raftLogSize += trunc.logDeltaBytes
	})
	if raftLogSize < 0 {
		raftLogSize = 0
	}
	return raftLogSize
}

func (p *pendingLogTruncations) computePostTruncFirstIndex(firstIndex uint64) uint64 {
	p.iterate(func(trunc pendingTruncation) {
		if firstIndex < trunc.Index+1 {
			firstIndex = trunc.Index + 1
		}
	})
	return firstIndex
}

func (p *pendingLogTruncations) empty() bool {
	return p.truncs[0] == (pendingTruncation{})
}

// Returns the front of the pending truncations queue, without removing the
// element.
// REQUIRES: !empty()
func (p *pendingLogTruncations) front() pendingTruncation {
	return p.truncs[0]
}

// Pops the front of the pending truncations queues.
// REQUIRES: !empty()
func (p *pendingLogTruncations) pop() {
	p.truncs[0] = pendingTruncation{}
	if !(p.truncs[1] == (pendingTruncation{})) {
		p.truncs[0] = p.truncs[1]
		p.truncs[1] = pendingTruncation{}
	}
}

func (p *pendingLogTruncations) iterate(f func(trunc pendingTruncation)) {
	for _, trunc := range p.truncs {
		if !(trunc == (pendingTruncation{})) {
			f(trunc)
		}
	}
}

type pendingTruncation struct {
	// The pending truncation will truncate entries up to
	// RaftTruncatedState.Index, inclusive.
	roachpb.RaftTruncatedState

	// The logDeltaBytes are computed under the assumption that the
	// truncation is deleting [expectedFirstIndex,RaftTruncatedState.Index]. It
	// originates in ReplicatedEvalResult, where it is accurate.
	// There are two reasons isDeltaTrusted could be considered false here:
	// - The original "accurate" delta does not account for sideloaded files. It
	//   is adjusted on this replica using
	//   SideloadStorage.BytesIfTruncatedFromTo, but it is possible that the
	//   truncated state of this replica is already > expectedFirstIndex. We
	//   don't actually set isDeltaTrusted=false for this case since we will
	//   change Replica.raftLogSizeTrusted to false after enacting this
	//   truncation.
	// - We merge pendingTruncation entries in the pendingTruncations struct. We
	//   are making an effort to have consecutive TruncateLogRequests provide us
	//   stats for index intervals that are adjacent and non-overlapping, but
	//   that behavior is best-effort.
	expectedFirstIndex uint64
	// logDeltaBytes includes the bytes from sideloaded files.
	logDeltaBytes  int64
	isDeltaTrusted bool
}

// raftLogTruncator is responsible for actually enacting truncations.
// Mutex ordering: Replica mutexes > raftLogTruncator.mu
type raftLogTruncator struct {
	store storeForTruncator
	mu    struct {
		syncutil.Mutex
		ranges map[roachpb.RangeID]struct{}
	}
}

func makeRaftLogTruncator(store storeForTruncator) raftLogTruncator {
	t := raftLogTruncator{
		store: store,
	}
	t.mu.ranges = make(map[roachpb.RangeID]struct{})
	return t
}

// storeForTruncator abstracts the interface of Store needed by the truncator.
type storeForTruncator interface {
	getReplicaForTruncator(rangeID roachpb.RangeID) (replicaForTruncator, error)
	// Engine accessor.
	Engine() storage.Engine
}

// replicaForTruncator abstracts the interface of Replica needed by the
// truncator.
//
// A replica has in-memory state that the truncator needs to access and
// mutate, which is potentially protected by a "replica-state" mutex. The
// {lock,unlock}ReplicaState are for acquiring and releasing this potential
// mutex. All the methods listed below between these two methods are protected
// by this mutex, unless otherwise noted.
//
// A replica also has persistent raft state that the truncator is modifying.
// There could be a potential "raft-state" mutex to mutually exclude other
// actions that are concurrently acting on this state. The
// {lock,unlock}RaftState are for acquiring and releasing this potential
// mutex. All the methods listed below between these two methods are protected
// by this mutex, unless otherwise noted.
//
// Lock ordering: raft-state mu < replica-state mu.
//
// We acknowledge that this interface may seem peculiar -- this is due to the
// constraint that it is abstracting Replica.
type replicaForTruncator interface {
	// GetRangeID returns the Range ID.
	GetRangeID() roachpb.RangeID
	// Replica-state concurrency control and getter/setters.
	lockReplicaState()
	getDestroyStatus() destroyStatus
	getTruncatedState() roachpb.RaftTruncatedState
	setTruncatedState(roachpb.RaftTruncatedState)
	// The caller is allowed to mutate the return value Mutating the
	// pendingLogTruncations requires holding both the replica-state and
	// raft-state mutexes. Which means read-only use cases can use either one of
	// these mutexes.
	getPendingTruncs() *pendingLogTruncations
	setTruncationDeltaAndTrusted(deltaBytes int64, isDeltaTrusted bool)
	unlockReplicaState()

	// Raft-state concurrency control and getters and implicit setters (via the
	// Engine exposed by storeForTruncator).
	lockRaftState()
	assertRaftStateLockHeld()
	sideloadedBytesIfTruncatedFromTo(
		_ context.Context, from, to uint64) (freed, retained int64, _ error)
	getStateLoader() stateloader.StateLoader
	unlockRaftState()
}

// addPendingTruncation assumes raft-state mutex is held and replica-state
// mutex is not held. raftExpectedFirstIndex and raftLogDelta have the same
// meaning as in ReplicatedEvalResult. Never called before cluster is at
// LooselyCoupledRaftLogTruncation. If deltaIncludesSideloaded is true, the
// raftLogDelta already includes the contribution of sideloaded files.
func (t *raftLogTruncator) addPendingTruncation(
	ctx context.Context,
	r replicaForTruncator,
	trunc roachpb.RaftTruncatedState,
	raftExpectedFirstIndex uint64,
	raftLogDelta int64,
) {
	r.assertRaftStateLockHeld()
	pendingTrunc := pendingTruncation{
		RaftTruncatedState: trunc,
		expectedFirstIndex: raftExpectedFirstIndex,
		logDeltaBytes:      raftLogDelta,
		isDeltaTrusted:     true,
	}
	// Can read pendingTruncs since holding raft-state mu.
	pendingTruncs := r.getPendingTruncs()
	pos, mergePending, alreadyTruncIndex :=
		func() (pos int, mergePending bool, alreadyTruncIndex uint64) {
			r.lockReplicaState()
			defer r.unlockReplicaState()
			// truncState is guaranteed to be non-nil
			truncState := r.getTruncatedState()
			alreadyTruncIndex = truncState.Index
			pos = 0
			mergePending = false
			for i, n := 0, len(pendingTruncs.truncs); i < n; i++ {
				if !(pendingTruncs.truncs[i] == (pendingTruncation{})) {
					if pendingTruncs.truncs[i].Index > alreadyTruncIndex {
						alreadyTruncIndex = pendingTruncs.truncs[i].Index
					}
					if i == n-1 {
						mergePending = true
						pos = i
					}
				} else {
					pos = i
					break
				}
			}
			return
		}()
	if alreadyTruncIndex >= pendingTrunc.Index {
		// Noop
		return
	}
	// It is possible that alreadyTruncIndex + 1 > raftExpectedFirstIndex. When
	// we merge or enact we will see this problem and set the trusted bit to
	// false. But we can at least avoid double counting sideloaded entries,
	// which can be large, since we do the computation here. That will reduce
	// the undercounting of the bytes in the raft log.
	sideloadedFreed, _, err := r.sideloadedBytesIfTruncatedFromTo(
		ctx, alreadyTruncIndex+1, pendingTrunc.Index+1)
	// Log a loud error since we need to continue enqueuing the truncation.
	if err != nil {
		log.Errorf(ctx, "while computing size of sideloaded files to truncate: %+v", err)
		pendingTrunc.isDeltaTrusted = false
	}
	pendingTrunc.logDeltaBytes -= sideloadedFreed
	if mergePending {
		pendingTrunc.isDeltaTrusted = pendingTrunc.isDeltaTrusted ||
			pendingTruncs.truncs[pos].isDeltaTrusted
		if pendingTruncs.truncs[pos].Index+1 != pendingTrunc.expectedFirstIndex {
			pendingTrunc.isDeltaTrusted = false
		}
		pendingTrunc.logDeltaBytes += pendingTruncs.truncs[pos].logDeltaBytes
		pendingTrunc.expectedFirstIndex = pendingTruncs.truncs[pos].expectedFirstIndex
	}
	// Lock replica-state in order to mutate pendingTruncs.
	r.lockReplicaState()
	pendingTruncs.truncs[pos] = pendingTrunc
	r.unlockReplicaState()

	if pos == 0 {
		if mergePending {
			panic("should never be merging pending truncations at pos 0")
		}
		// First entry in queue of pending truncations for this replica.
		t.mu.Lock()
		t.mu.ranges[r.GetRangeID()] = struct{}{}
		t.mu.Unlock()
	}
}

// Invoked whenever the durability of the store advances. We assume that this
// is coarse in that the advancement of durability will apply to all ranges in
// this store, and most of the preceding pending truncations have their goal
// truncated index become durable in RangeAppliedState.RaftAppliedIndex. This
// coarseness assumption is important for not wasting much work being done in
// this method.
// TODO(sumeer): hook this up to the callback that will be invoked on the
// Store by the Engine (Pebble).
func (t *raftLogTruncator) durabilityAdvanced(ctx context.Context) {
	var ranges []roachpb.RangeID
	func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		n := len(t.mu.ranges)
		if n == 0 {
			return
		}
		ranges = make([]roachpb.RangeID, n)
		i := 0
		for k := range t.mu.ranges {
			ranges[i] = k
			// If another pendingTruncation is added to this Replica, it will not be
			// added back to the map since the Replica already has pending
			// truncations. That is ok: we will try to enact all pending truncations
			// for that Replica below, since there typically will only be one
			// pending, and if there are any remaining we will add it back to the
			// map.
			delete(t.mu.ranges, k)
			i++
		}
	}()
	if len(ranges) == 0 {
		return
	}

	// Create an engine Reader to provide a safe lower bound on what is durable.
	//
	// TODO(sumeer): This is incorrect -- change this reader to only read
	// durable state after merging
	// https://github.com/cockroachdb/pebble/pull/1490 and incorporating into
	// CockroachDB.
	reader := t.store.Engine().NewReadOnly()

	for _, repl := range ranges {
		r, err := t.store.getReplicaForTruncator(repl)
		if err != nil || r == nil {
			// Not found or not the same replica.
			continue
		}
		r.lockRaftState()
		r.lockReplicaState()
		if r.getDestroyStatus().Removed() {
			r.unlockReplicaState()
			r.unlockRaftState()
			continue
		}
		// Not destroyed. We can release Replica.mu below and be sure it will not
		// be replaced by another replica with the same RangeID, since we continue
		// to hold Replica.raftMu.

		truncState := r.getTruncatedState()
		pendingTruncs := r.getPendingTruncs()
		for !pendingTruncs.empty() {
			pendingTrunc := pendingTruncs.front()
			if pendingTrunc.Index <= truncState.Index {
				pendingTruncs.pop()
			}
		}
		if pendingTruncs.empty() {
			r.unlockReplicaState()
			r.unlockRaftState()
			continue
		}
		// Have some pending truncations.
		r.unlockReplicaState()
		popAll := func() {
			r.lockReplicaState()
			for !pendingTruncs.empty() {
				pendingTruncs.pop()
			}
			r.unlockReplicaState()
			r.unlockRaftState()
		}
		// NB: we can read pendingTruncs since we still hold raft-state mutex.
		// Use the reader to decide what is durable.
		stateLoader := r.getStateLoader()
		as, err := stateLoader.LoadRangeAppliedState(ctx, reader)
		if err != nil {
			log.Errorf(ctx, "while loading RangeAppliedState for log truncation: %+v", err)
			// Pop all the truncations.
			popAll()
			continue
		}
		enactIndex := -1
		pendingTruncs.iterate(func(trunc pendingTruncation) {
			if trunc.Index > as.RaftAppliedIndex {
				return
			}
			enactIndex++
		})
		if enactIndex > 0 {
			// Do the truncation.
			batch := t.store.Engine().NewUnindexedBatch(false)
			apply, err := handleTruncatedStateBelowRaftPreApply(ctx, &truncState,
				&pendingTruncs.truncs[enactIndex].RaftTruncatedState, stateLoader, batch)
			if err != nil {
				popAll()
				batch.Close()
				continue
			}
			if !apply {
				panic("unexpected !apply returned from handleTruncatedStateBelowRaftPreApply")
			}
			if err := batch.Commit(false); err != nil {
				popAll()
				continue
			}
			// Truncation done. Need to update the Replica state.
			r.lockReplicaState()
			for i := 0; i <= enactIndex; i++ {
				pendingTrunc := pendingTruncs.front()
				truncState = r.getTruncatedState()
				isDeltaTrusted := true
				if truncState.Index+1 != pendingTrunc.expectedFirstIndex ||
					!pendingTrunc.isDeltaTrusted {
					isDeltaTrusted = false
				}
				r.setTruncationDeltaAndTrusted(pendingTrunc.logDeltaBytes, isDeltaTrusted)
				r.setTruncatedState(pendingTrunc.RaftTruncatedState)

				pendingTruncs.pop()
			}
			if !pendingTruncs.empty() {
				t.mu.Lock()
				t.mu.ranges[repl] = struct{}{}
				t.mu.Unlock()
			}
			r.unlockReplicaState()
		} else {
			t.mu.Lock()
			t.mu.ranges[repl] = struct{}{}
			t.mu.Unlock()
		}
		r.unlockRaftState()
	}
}
