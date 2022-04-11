// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	jsonMetaSentinel = `__crdb__`
)

// emitResolvedTimestamp emits a changefeed-level resolved timestamp to the
// sink.
func emitResolvedTimestamp(
	ctx context.Context, encoder Encoder, sink Sink, resolved hlc.Timestamp,
) error {
	// TODO(dan): Emit more fine-grained (table level) resolved
	// timestamps.
	if err := sink.EmitResolvedTimestamp(ctx, encoder, resolved); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, `resolved %s`, resolved)
	}
	return nil
}

// createProtectedTimestampRecord will create a record to protect the spans for
// this changefeed at the resolved timestamp. The progress struct will be
// updated to refer to this new protected timestamp record.
func createProtectedTimestampRecord(
	ctx context.Context,
	codec keys.SQLCodec,
	pts protectedts.Storage,
	txn *kv.Txn,
	jobID jobspb.JobID,
	targets jobspb.ChangefeedTargets,
	timestamp hlc.Timestamp,
	progress *jobspb.ChangefeedProgress,
) error {
	if !codec.ForSystemTenant() {
		return errors.AssertionFailedf("createProtectedTimestampRecord called on tenant-based changefeed")
	}

	progress.ProtectedTimestampRecord = uuid.MakeV4()
	log.VEventf(ctx, 2, "creating protected timestamp %v at %v",
		progress.ProtectedTimestampRecord, timestamp)
	deprecatedSpansToProtect := makeSpansToProtect(codec, targets)
	targetToProtect := makeTargetToProtect(targets)
	rec := jobsprotectedts.MakeRecord(
		progress.ProtectedTimestampRecord, int64(jobID), timestamp, deprecatedSpansToProtect,
		jobsprotectedts.Jobs, targetToProtect)
	return pts.Protect(ctx, txn, rec)
}

func makeTargetToProtect(targets jobspb.ChangefeedTargets) *ptpb.Target {
	// NB: We add 1 because we're also going to protect system.descriptors.
	// We protect system.descriptors because a changefeed needs all of the history
	// of table descriptors to version data.
	tablesToProtect := make(descpb.IDs, 0, len(targets)+1)
	for t := range targets {
		tablesToProtect = append(tablesToProtect, t)
	}
	tablesToProtect = append(tablesToProtect, keys.DescriptorTableID)
	return ptpb.MakeSchemaObjectsTarget(tablesToProtect)
}

func makeSpansToProtect(codec keys.SQLCodec, targets jobspb.ChangefeedTargets) []roachpb.Span {
	// NB: We add 1 because we're also going to protect system.descriptors.
	// We protect system.descriptors because a changefeed needs all of the history
	// of table descriptors to version data.
	spansToProtect := make([]roachpb.Span, 0, len(targets)+1)
	addTablePrefix := func(id uint32) {
		tablePrefix := codec.TablePrefix(id)
		spansToProtect = append(spansToProtect, roachpb.Span{
			Key:    tablePrefix,
			EndKey: tablePrefix.PrefixEnd(),
		})
	}
	for t := range targets {
		addTablePrefix(uint32(t))
	}
	addTablePrefix(keys.DescriptorTableID)
	return spansToProtect
}

// initialScanFromOptions returns whether or not the options indicate the need
// for an initial scan on the first run.
func initialScanFromOptions(opts map[string]string) bool {
	_, cursor := opts[changefeedbase.OptCursor]
	_, initialScan := opts[changefeedbase.OptInitialScan]
	_, noInitialScan := opts[changefeedbase.OptNoInitialScan]
	return (cursor && initialScan) || (!cursor && !noInitialScan)
}
