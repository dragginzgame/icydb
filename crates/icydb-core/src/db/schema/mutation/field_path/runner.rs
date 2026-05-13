use super::*;

///
/// SchemaFieldPathIndexRunnerError
///
/// Fail-closed field-path runner orchestration errors. These classify the
/// phase that prevented publication without matching lower-level error text.
///

#[allow(
    dead_code,
    reason = "0.153 stages field-path runner orchestration before public DDL consumes it"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexRunnerError {
    UnsupportedExecutionPlan,
    TargetMismatch,
    StageRowsFailed,
    StagedStoreRejected,
    IsolatedStoreValidationFailed,
    RuntimeInvalidationIdentity,
    SnapshotPublicationRejected,
    PublishedStoreRejected,
}

impl SchemaFieldPathIndexRunnerError {
    #[must_use]
    pub(in crate::db::schema) const fn phase(self) -> SchemaMutationRunnerPhase {
        match self {
            Self::UnsupportedExecutionPlan | Self::TargetMismatch => {
                SchemaMutationRunnerPhase::Preflight
            }
            Self::StageRowsFailed | Self::StagedStoreRejected => {
                SchemaMutationRunnerPhase::StageStores
            }
            Self::IsolatedStoreValidationFailed => SchemaMutationRunnerPhase::ValidatePhysicalState,
            Self::RuntimeInvalidationIdentity => SchemaMutationRunnerPhase::InvalidateRuntimeState,
            Self::SnapshotPublicationRejected => SchemaMutationRunnerPhase::PublishSnapshot,
            Self::PublishedStoreRejected => SchemaMutationRunnerPhase::PublishPhysicalStore,
        }
    }

    #[must_use]
    const fn validation_status(self) -> SchemaMutationValidationStatus {
        match self {
            Self::RuntimeInvalidationIdentity
            | Self::SnapshotPublicationRejected
            | Self::PublishedStoreRejected => SchemaMutationValidationStatus::Passed,
            Self::IsolatedStoreValidationFailed => SchemaMutationValidationStatus::Failed,
            Self::UnsupportedExecutionPlan
            | Self::TargetMismatch
            | Self::StageRowsFailed
            | Self::StagedStoreRejected => SchemaMutationValidationStatus::NotStarted,
        }
    }

    #[must_use]
    const fn publish_status(self) -> SchemaMutationPublishStatus {
        match self {
            Self::SnapshotPublicationRejected | Self::PublishedStoreRejected => {
                SchemaMutationPublishStatus::Failed
            }
            Self::UnsupportedExecutionPlan
            | Self::TargetMismatch
            | Self::StageRowsFailed
            | Self::StagedStoreRejected
            | Self::IsolatedStoreValidationFailed
            | Self::RuntimeInvalidationIdentity => SchemaMutationPublishStatus::NotStarted,
        }
    }
}

///
/// SchemaFieldPathIndexRunnerFailure
///
/// Typed failure report for a field-path runner attempt. Failures that occur
/// after staged physical writes can carry the rollback report that restored
/// the isolated store image before the runner returned.
///

#[allow(
    dead_code,
    reason = "0.153 stages field-path runner rollback diagnostics before public DDL consumes it"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRunnerFailure {
    error: SchemaFieldPathIndexRunnerError,
    rollback_report: Option<Box<SchemaFieldPathIndexStagedStoreRollbackReport>>,
}

#[allow(
    dead_code,
    reason = "0.153 stages field-path runner rollback diagnostics before public DDL consumes it"
)]
impl SchemaFieldPathIndexRunnerFailure {
    #[must_use]
    const fn without_rollback(error: SchemaFieldPathIndexRunnerError) -> Self {
        Self {
            error,
            rollback_report: None,
        }
    }

    #[must_use]
    fn with_rollback(
        error: SchemaFieldPathIndexRunnerError,
        rollback_report: SchemaFieldPathIndexStagedStoreRollbackReport,
    ) -> Self {
        Self {
            error,
            rollback_report: Some(Box::new(rollback_report)),
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn error(&self) -> SchemaFieldPathIndexRunnerError {
        self.error
    }

    #[must_use]
    pub(in crate::db::schema) const fn phase(&self) -> SchemaMutationRunnerPhase {
        self.error.phase()
    }

    #[must_use]
    pub(in crate::db::schema) fn rollback_report(
        &self,
    ) -> Option<&SchemaFieldPathIndexStagedStoreRollbackReport> {
        self.rollback_report.as_deref()
    }

    #[must_use]
    pub(in crate::db::schema) fn developer_report(
        &self,
        entity_path: &'static str,
        target: &SchemaFieldPathIndexRebuildTarget,
        rows_scanned: usize,
    ) -> SchemaMutationDeveloperReport {
        let index_keys_written = self
            .rollback_report()
            .map_or(0, |report| report.runner_report().index_keys_written());

        SchemaMutationDeveloperReport::field_path_index_addition(
            self.phase(),
            entity_path,
            target,
            rows_scanned,
            index_keys_written,
            self.error.validation_status(),
            self.error.publish_status(),
        )
    }
}

/// SchemaFieldPathIndexRunnerReport
///
/// End-to-end runner report for one accepted field-path index rebuild. It
/// binds the staged write, isolated physical validation, runtime invalidation,
/// and accepted snapshot publication handoff into one typed result.
///

#[allow(
    dead_code,
    reason = "0.153 stages field-path runner orchestration before public DDL consumes it"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRunnerReport {
    store: String,
    write_report: SchemaFieldPathIndexStagedStoreWriteReport,
    validation: SchemaFieldPathIndexIsolatedIndexStoreValidation,
    invalidation_report: SchemaFieldPathIndexRuntimeInvalidationReport,
    publication_report: SchemaFieldPathIndexSnapshotPublicationReport,
    published_store_report: SchemaFieldPathIndexPublishedStoreReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages field-path runner orchestration before public DDL consumes it"
)]
impl SchemaFieldPathIndexRunnerReport {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn write_report(
        &self,
    ) -> &SchemaFieldPathIndexStagedStoreWriteReport {
        &self.write_report
    }

    #[must_use]
    pub(in crate::db::schema) const fn validation(
        &self,
    ) -> &SchemaFieldPathIndexIsolatedIndexStoreValidation {
        &self.validation
    }

    #[must_use]
    pub(in crate::db::schema) const fn invalidation_report(
        &self,
    ) -> &SchemaFieldPathIndexRuntimeInvalidationReport {
        &self.invalidation_report
    }

    #[must_use]
    pub(in crate::db::schema) const fn publication_report(
        &self,
    ) -> &SchemaFieldPathIndexSnapshotPublicationReport {
        &self.publication_report
    }

    #[must_use]
    pub(in crate::db::schema) const fn published_store_report(
        &self,
    ) -> &SchemaFieldPathIndexPublishedStoreReport {
        &self.published_store_report
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        self.published_store_report.runner_report()
    }

    #[must_use]
    pub(in crate::db::schema) fn publication_readiness(
        &self,
    ) -> SchemaFieldPathIndexStagedStorePublicationReadiness {
        self.published_store_report.publication_readiness()
    }

    #[must_use]
    pub(in crate::db::schema) fn developer_report(
        &self,
        entity_path: &'static str,
        target: &SchemaFieldPathIndexRebuildTarget,
    ) -> SchemaMutationDeveloperReport {
        let publish_status = if self.runner_report().physical_work_allows_publication() {
            SchemaMutationPublishStatus::Published
        } else {
            SchemaMutationPublishStatus::Failed
        };

        SchemaMutationDeveloperReport::field_path_index_addition(
            SchemaMutationRunnerPhase::PublishPhysicalStore,
            entity_path,
            target,
            self.runner_report().rows_scanned(),
            self.runner_report().index_keys_written(),
            SchemaMutationValidationStatus::Passed,
            publish_status,
        )
    }
}
/// SchemaFieldPathIndexRunner
///
/// Narrow physical runner for one accepted field-path index rebuild. The
/// runner only accepts the schema-owned field-path execution shape and keeps
/// row staging, isolated store mutation, runtime invalidation, and snapshot
/// publication handoff in a fixed order.
///

#[allow(
    dead_code,
    reason = "0.153 stages field-path runner orchestration before public DDL consumes it"
)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRunner;

#[allow(
    dead_code,
    reason = "0.153 stages field-path runner orchestration before public DDL consumes it"
)]
impl SchemaFieldPathIndexRunner {
    pub(in crate::db::schema) fn run<'a>(
        input: &SchemaMutationRunnerInput<'_>,
        entity_tag: EntityTag,
        target: SchemaFieldPathIndexRebuildTarget,
        rows: impl IntoIterator<Item = SchemaFieldPathIndexRebuildRow<'a>>,
        index_store: &mut IndexStore,
        invalidation_sink: &mut impl SchemaMutationRuntimeInvalidationSink,
        publication_sink: &mut impl SchemaMutationAcceptedSnapshotPublicationSink,
    ) -> Result<SchemaFieldPathIndexRunnerReport, SchemaFieldPathIndexRunnerFailure> {
        Self::validate_execution_plan(input.execution_plan(), &target)
            .map_err(SchemaFieldPathIndexRunnerFailure::without_rollback)?;
        let target_index_id = IndexId::new(entity_tag, target.ordinal());

        let staged = SchemaFieldPathIndexStagedRebuild::from_rows(
            input.accepted_after().entity_path(),
            entity_tag,
            target,
            rows,
        )
        .map_err(|_| {
            SchemaFieldPathIndexRunnerFailure::without_rollback(
                SchemaFieldPathIndexRunnerError::StageRowsFailed,
            )
        })?;
        let staged_store =
            SchemaFieldPathIndexStagedStore::from_rebuild(&staged, input.execution_plan())
                .map_err(|_| {
                    SchemaFieldPathIndexRunnerFailure::without_rollback(
                        SchemaFieldPathIndexRunnerError::StagedStoreRejected,
                    )
                })?;
        let store = staged_store.store().to_string();
        let (write_report, validation) = {
            let mut writer = SchemaFieldPathIndexIsolatedIndexStoreWriter::new(&store, index_store);
            let batch = staged_store.write_batch(&writer);
            let write_report = batch.write_to(&mut writer);
            let Ok(validation) = writer.validate_batch_for_target_index(&target_index_id, &batch)
            else {
                let rollback_report = batch.rollback_plan().rollback_to(&mut writer);
                return Err(SchemaFieldPathIndexRunnerFailure::with_rollback(
                    SchemaFieldPathIndexRunnerError::IsolatedStoreValidationFailed,
                    rollback_report,
                ));
            };

            (write_report, validation)
        };
        let invalidation_plan =
            SchemaFieldPathIndexRuntimeInvalidationPlan::from_isolated_index_store_validation(
                &validation,
                input,
            )
            .map_err(|_| {
                SchemaFieldPathIndexRunnerFailure::without_rollback(
                    SchemaFieldPathIndexRunnerError::RuntimeInvalidationIdentity,
                )
            })?;
        let invalidation_report = invalidation_plan.invalidate_runtime_state(invalidation_sink);
        let publication_plan =
            SchemaFieldPathIndexSnapshotPublicationPlan::from_runtime_invalidation_report(
                &invalidation_report,
                input,
            )
            .map_err(|_| {
                SchemaFieldPathIndexRunnerFailure::without_rollback(
                    SchemaFieldPathIndexRunnerError::SnapshotPublicationRejected,
                )
            })?;
        let publication_report = publication_plan.publish_snapshot(publication_sink);
        let published_store_plan =
            SchemaFieldPathIndexPublishedStorePlan::from_validated_publication(
                &validation,
                &publication_report,
            )
            .map_err(|_| {
                SchemaFieldPathIndexRunnerFailure::without_rollback(
                    SchemaFieldPathIndexRunnerError::PublishedStoreRejected,
                )
            })?;
        let published_store_report = published_store_plan
            .publish_index_store_for_target_index(&target_index_id, index_store)
            .map_err(|_| {
                SchemaFieldPathIndexRunnerFailure::without_rollback(
                    SchemaFieldPathIndexRunnerError::PublishedStoreRejected,
                )
            })?;

        Ok(SchemaFieldPathIndexRunnerReport {
            store,
            write_report,
            validation,
            invalidation_report,
            publication_report,
            published_store_report,
        })
    }

    fn validate_execution_plan(
        execution_plan: &SchemaMutationExecutionPlan,
        target: &SchemaFieldPathIndexRebuildTarget,
    ) -> Result<(), SchemaFieldPathIndexRunnerError> {
        match execution_plan.steps() {
            [
                SchemaMutationExecutionStep::BuildFieldPathIndex {
                    target: planned_target,
                },
                SchemaMutationExecutionStep::ValidatePhysicalWork,
                SchemaMutationExecutionStep::InvalidateRuntimeState,
            ] if planned_target == target => Ok(()),
            [
                SchemaMutationExecutionStep::BuildFieldPathIndex { .. },
                SchemaMutationExecutionStep::ValidatePhysicalWork,
                SchemaMutationExecutionStep::InvalidateRuntimeState,
            ] => Err(SchemaFieldPathIndexRunnerError::TargetMismatch),
            _ => Err(SchemaFieldPathIndexRunnerError::UnsupportedExecutionPlan),
        }
    }
}
