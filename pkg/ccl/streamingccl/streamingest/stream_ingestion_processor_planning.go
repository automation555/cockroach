// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

func distStreamIngestionPlanSpecs(
	streamAddress streamingccl.StreamAddress,
	topology streamclient.Topology,
	nodes []roachpb.NodeID,
	initialHighWater hlc.Timestamp,
	jobID jobspb.JobID,
	streamID streaming.StreamID,
) ([]*execinfrapb.StreamIngestionDataSpec, *execinfrapb.StreamIngestionFrontierSpec, error) {

	// For each stream partition in the topology, assign it to a node.
	streamIngestionSpecs := make([]*execinfrapb.StreamIngestionDataSpec, 0, len(nodes))

	trackedSpans := make([]roachpb.Span, 0)
	for i, partition := range topology {
		// Round robin assign the stream partitions to nodes. Partitions 0 through
		// len(nodes) - 1 creates the spec. Future partitions just add themselves to
		// the partition addresses.
		if i < len(nodes) {
			spec := &execinfrapb.StreamIngestionDataSpec{
				StreamID: 					uint64(streamID),
				JobID:              int64(jobID),
				StartTime:          initialHighWater,
				StreamAddress:      string(streamAddress),
				PartitionAddresses: make([]string, 0),
			}
			streamIngestionSpecs = append(streamIngestionSpecs, spec)
		}
		n := i % len(nodes)

		streamIngestionSpecs[n].PartitionIds = append(streamIngestionSpecs[n].PartitionIds, partition.ID)
		streamIngestionSpecs[n].PartitionSpecs = append(streamIngestionSpecs[n].PartitionSpecs,
			string(partition.SubscriptionToken))
		streamIngestionSpecs[n].PartitionAddresses = append(streamIngestionSpecs[n].PartitionAddresses,
			string(partition.SrcAddr))
		// We create "fake" spans to uniquely identify the partition. This is used
		// to keep track of the resolved ts received for a particular partition in
		// the frontier processor.
		// TODO: how do we assign partition ID?
		trackedSpans = append(trackedSpans, roachpb.Span{
			Key:    roachpb.Key(partition.ID),
			EndKey: roachpb.Key(partition.ID).Next(),
		})
	}

	for index, sis := range streamIngestionSpecs {
		fmt.Println("ingestion spec", index, "has specs:", sis.PartitionSpecs)
	}

	// Create a spec for the StreamIngestionFrontier processor on the coordinator
	// node.
	streamIngestionFrontierSpec := &execinfrapb.StreamIngestionFrontierSpec{
		HighWaterAtStart: initialHighWater, TrackedSpans: trackedSpans, JobID: int64(jobID),
	}

	return streamIngestionSpecs, streamIngestionFrontierSpec, nil
}

func distStreamIngest(
	ctx context.Context,
	execCtx sql.JobExecContext,
	nodes []roachpb.NodeID,
	jobID jobspb.JobID,
	planCtx *sql.PlanningCtx,
	dsp *sql.DistSQLPlanner,
	client streamclient.Client,
	streamID streaming.StreamID,
	streamIngestionSpecs []*execinfrapb.StreamIngestionDataSpec,
	streamIngestionFrontierSpec *execinfrapb.StreamIngestionFrontierSpec,
) error {
	ctx = logtags.AddTag(ctx, "stream-ingest-distsql", nil)
	evalCtx := execCtx.ExtendedEvalContext()
	var noTxn *kv.Txn

	if len(streamIngestionSpecs) == 0 {
		return nil
	}

	// Setup a one-stage plan with one proc per input spec.
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(streamIngestionSpecs))
	for i := range streamIngestionSpecs {
		corePlacement[i].NodeID = nodes[i]
		corePlacement[i].Core.StreamIngestionData = streamIngestionSpecs[i]
	}

	p := planCtx.NewPhysicalPlan()
	p.AddNoInputStage(
		corePlacement,
		execinfrapb.PostProcessSpec{},
		streamIngestionResultTypes,
		execinfrapb.Ordering{},
	)

	execCfg := execCtx.ExecCfg()
	gatewayNodeID, err := execCfg.NodeID.OptionalNodeIDErr(48274)
	if err != nil {
		return err
	}

	// The ResultRouters from the previous stage will feed in to the
	// StreamIngestionFrontier processor.
	p.AddSingleGroupStage(gatewayNodeID,
		execinfrapb.ProcessorCoreUnion{StreamIngestionFrontier: streamIngestionFrontierSpec},
		execinfrapb.PostProcessSpec{}, streamIngestionResultTypes)

	p.PlanToStreamColMap = []int{0}
	dsp.FinalizePlan(planCtx, p)

	frontierUpdates := make(chan hlc.Timestamp)
	rw := makeStreamIngestionResultWriter(ctx, jobID, execCfg.JobRegistry, frontierUpdates)

	recv := sql.MakeDistSQLReceiver(
		ctx,
		rw,
		tree.Rows,
		nil, /* rangeCache */
		noTxn,
		nil, /* clockUpdater */
		evalCtx.Tracing,
		execCfg.ContentionRegistry,
		nil, /* testingPushCallback */
	)
	defer recv.Release()

	ctxWithCancel, cancel := context.WithCancel(ctx)
	cg := ctxgroup.WithContext(ctxWithCancel)
	cg.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case frontier := <-frontierUpdates:
				// TODO: handle stream inactive error
				if err := client.Heartbeat(ctx, streamID, frontier); err != nil {
					return err
				}
			}
		}
	})

	// Copy the evalCtx, as dsp.Run() might change it.
	evalCtxCopy := *evalCtx
	dsp.Run(planCtx, noTxn, p, recv, &evalCtxCopy, nil /* finishedSetupFn */)()
	if rw.Err() != nil {
		return rw.Err()
	}

	cancel()
	//if err := cg.Wait(); err != nil && err.Error() != "context canceled" {
	//	return err // Heartbeat return STATUS_UNKNOWN_RETRY
	//}
	_ = cg.Wait()
	return nil
}

type streamIngestionResultWriter struct {
	ctx             context.Context
	registry        *jobs.Registry
	frontierUpdates chan hlc.Timestamp
	jobID           jobspb.JobID
	rowsAffected    int
	err             error
}

func makeStreamIngestionResultWriter(
	ctx context.Context, jobID jobspb.JobID, registry *jobs.Registry, frontierUpdates chan hlc.Timestamp,
) *streamIngestionResultWriter {
	return &streamIngestionResultWriter{
		ctx:      ctx,
		registry: registry,
		frontierUpdates:   frontierUpdates,
		jobID:    jobID,
	}
}

// AddRow implements the sql.rowResultWriter interface.
func (s *streamIngestionResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	if len(row) == 0 {
		return errors.New("streamIngestionResultWriter received an empty row")
	}
	if row[0] == nil {
		return errors.New("streamIngestionResultWriter expects non-nil row entry")
	}

	// Decode the row, write the ts into job record, and send a heartbeat to source cluster.
	var ingestedHighWatermark hlc.Timestamp
	if err := protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)),
		&ingestedHighWatermark); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, `unmarshalling resolved timestamp`)
	}
	// TODO: use UpdateJobWithTxn ?
	if err := s.registry.UpdateJobWithTxn(ctx, s.jobID, nil /* txn */, false /* useReadLock */,
		func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			return jobs.UpdateHighwaterProgressed(ingestedHighWatermark, md, ju)
	}); err != nil {
		return err
	}
	// TODO: should we only send heartbeat when we have moved forward the frontier?
	s.frontierUpdates <- ingestedHighWatermark
	return nil
}

// IncrementRowsAffected implements the sql.rowResultWriter interface.
func (s *streamIngestionResultWriter) IncrementRowsAffected(ctx context.Context, n int) {
	s.rowsAffected += n
}

// SetError implements the sql.rowResultWriter interface.
func (s *streamIngestionResultWriter) SetError(err error) {
	s.err = err
}

// Err implements the sql.rowResultWriter interface.
func (s *streamIngestionResultWriter) Err() error {
	return s.err
}
