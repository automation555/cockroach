// Code generated by "stringer"; DO NOT EDIT.

package clusterversion

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[V21_2-0]
	_ = x[Start22_1-1]
	_ = x[TargetBytesAvoidExcess-2]
	_ = x[AvoidDrainingNames-3]
	_ = x[DrainingNamesMigration-4]
	_ = x[TraceIDDoesntImplyStructuredRecording-5]
	_ = x[AlterSystemTableStatisticsAddAvgSizeCol-6]
	_ = x[AlterSystemStmtDiagReqs-7]
	_ = x[MVCCAddSSTable-8]
	_ = x[InsertPublicSchemaNamespaceEntryOnRestore-9]
	_ = x[UnsplitRangesInAsyncGCJobs-10]
	_ = x[ValidateGrantOption-11]
	_ = x[PebbleFormatBlockPropertyCollector-12]
	_ = x[ProbeRequest-13]
	_ = x[SelectRPCsTakeTracingInfoInband-14]
	_ = x[PreSeedTenantSpanConfigs-15]
	_ = x[SeedTenantSpanConfigs-16]
	_ = x[PublicSchemasWithDescriptors-17]
	_ = x[EnsureSpanConfigReconciliation-18]
	_ = x[EnsureSpanConfigSubscription-19]
	_ = x[EnableSpanConfigStore-20]
	_ = x[ScanWholeRows-21]
	_ = x[SCRAMAuthentication-22]
	_ = x[UnsafeLossOfQuorumRecoveryRangeLog-23]
	_ = x[AlterSystemProtectedTimestampAddColumn-24]
	_ = x[EnableProtectedTimestampsForTenant-25]
	_ = x[DeleteCommentsWithDroppedIndexes-26]
	_ = x[RemoveIncompatibleDatabasePrivileges-27]
	_ = x[AddRaftAppliedIndexTermMigration-28]
	_ = x[PostAddRaftAppliedIndexTermMigration-29]
	_ = x[DontProposeWriteTimestampForLeaseTransfers-30]
	_ = x[TenantSettingsTable-31]
	_ = x[EnableLeaseHolderRemoval-32]
}

const _Key_name = "V21_2Start22_1TargetBytesAvoidExcessAvoidDrainingNamesDrainingNamesMigrationTraceIDDoesntImplyStructuredRecordingAlterSystemTableStatisticsAddAvgSizeColAlterSystemStmtDiagReqsMVCCAddSSTableInsertPublicSchemaNamespaceEntryOnRestoreUnsplitRangesInAsyncGCJobsValidateGrantOptionPebbleFormatBlockPropertyCollectorProbeRequestSelectRPCsTakeTracingInfoInbandPreSeedTenantSpanConfigsSeedTenantSpanConfigsPublicSchemasWithDescriptorsEnsureSpanConfigReconciliationEnsureSpanConfigSubscriptionEnableSpanConfigStoreScanWholeRowsSCRAMAuthenticationUnsafeLossOfQuorumRecoveryRangeLogAlterSystemProtectedTimestampAddColumnEnableProtectedTimestampsForTenantDeleteCommentsWithDroppedIndexesRemoveIncompatibleDatabasePrivilegesAddRaftAppliedIndexTermMigrationPostAddRaftAppliedIndexTermMigrationDontProposeWriteTimestampForLeaseTransfersTenantSettingsTableEnableLeaseHolderRemoval"

var _Key_index = [...]uint16{0, 5, 14, 36, 54, 76, 113, 152, 175, 189, 230, 256, 275, 309, 321, 352, 376, 397, 425, 455, 483, 504, 517, 536, 570, 608, 642, 674, 710, 742, 778, 820, 839, 863}

func (i Key) String() string {
	if i < 0 || i >= Key(len(_Key_index)-1) {
		return "Key(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _Key_name[_Key_index[i]:_Key_index[i+1]]
}
