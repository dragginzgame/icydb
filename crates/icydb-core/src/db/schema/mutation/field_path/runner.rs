use super::*;

///
/// SchemaFieldPathIndexRunnerError
///
/// Fail-closed field-path runner orchestration errors. These classify the
/// phase that prevented publication without matching lower-level error text.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexRunnerError {
    UnsupportedMutationPlan,
    TargetMismatch,
    StageRowsFailed,
    StagedStoreRejected,
    IsolatedStoreValidationFailed,
    RuntimeInvalidationIdentity,
    SnapshotPublicationRejected,
    PublishedStoreRejected,
}

///
/// SchemaFieldPathIndexRunnerFailure
///
/// Typed failure report for a field-path runner attempt. Failures that occur
/// after staged physical writes can carry the rollback report that restored
/// the isolated store image before the runner returned.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRunnerFailure {
    error: SchemaFieldPathIndexRunnerError,
    rollback_report: Option<Box<SchemaFieldPathIndexStagedStoreRollbackReport>>,
}

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
    #[cfg(test)]
    pub(in crate::db::schema) const fn error(&self) -> SchemaFieldPathIndexRunnerError {
        self.error
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) fn rollback_report(
        &self,
    ) -> Option<&SchemaFieldPathIndexStagedStoreRollbackReport> {
        self.rollback_report.as_deref()
    }
}

/// SchemaFieldPathIndexRunnerReport
///
/// End-to-end runner report for one accepted field-path index rebuild. It
/// binds the staged write, isolated physical validation, runtime invalidation,
/// and accepted snapshot publication handoff into one typed result.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRunnerReport {
    #[cfg(test)]
    store: String,
    #[cfg(test)]
    write_report: SchemaFieldPathIndexStagedStoreWriteReport,
    #[cfg(test)]
    validation: SchemaFieldPathIndexIsolatedIndexStoreValidation,
    #[cfg(test)]
    invalidation_report: SchemaFieldPathIndexRuntimeInvalidationReport,
    #[cfg(test)]
    publication_report: SchemaFieldPathIndexSnapshotPublicationReport,
    published_store_report: SchemaFieldPathIndexPublishedStoreReport,
}

impl SchemaFieldPathIndexRunnerReport {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn write_report(
        &self,
    ) -> &SchemaFieldPathIndexStagedStoreWriteReport {
        &self.write_report
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn validation(
        &self,
    ) -> &SchemaFieldPathIndexIsolatedIndexStoreValidation {
        &self.validation
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn invalidation_report(
        &self,
    ) -> &SchemaFieldPathIndexRuntimeInvalidationReport {
        &self.invalidation_report
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn publication_report(
        &self,
    ) -> &SchemaFieldPathIndexSnapshotPublicationReport {
        &self.publication_report
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn published_store_report(
        &self,
    ) -> &SchemaFieldPathIndexPublishedStoreReport {
        &self.published_store_report
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(
        &self,
    ) -> &SchemaFieldPathIndexMutationProgress {
        self.published_store_report.runner_report()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) fn publication_readiness(
        &self,
    ) -> SchemaFieldPathIndexStagedStorePublicationReadiness {
        self.published_store_report.publication_readiness()
    }

    #[must_use]
    pub(in crate::db::schema) const fn mutation_metrics(
        &self,
        entity_path: &'static str,
    ) -> SchemaFieldPathIndexMutationMetrics {
        SchemaFieldPathIndexMutationMetrics::new(
            entity_path,
            self.runner_report().rows_scanned(),
            self.runner_report().index_keys_written(),
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

pub(in crate::db::schema) struct SchemaFieldPathIndexRunner;

impl SchemaFieldPathIndexRunner {
    #[expect(
        clippy::too_many_arguments,
        reason = "runner boundary keeps schema input, target, row stream, physical store, and publication sinks explicit"
    )]
    pub(in crate::db::schema) fn run<'a>(
        input: &SchemaMutationRunnerInput<'_>,
        entity_tag: EntityTag,
        target: SchemaFieldPathIndexRebuildTarget,
        predicate_program: Option<&PredicateProgram>,
        rows: impl IntoIterator<Item = SchemaFieldPathIndexRebuildRow<'a>>,
        index_store: &mut IndexStore,
        invalidation_sink: &mut impl SchemaMutationRuntimeInvalidationSink,
        publication_sink: &mut impl SchemaMutationAcceptedSnapshotPublicationSink,
    ) -> Result<SchemaFieldPathIndexRunnerReport, SchemaFieldPathIndexRunnerFailure> {
        Self::validate_mutation_plan(input.mutation_plan(), &target)
            .map_err(SchemaFieldPathIndexRunnerFailure::without_rollback)?;
        let target_index_id = IndexId::new(entity_tag, target.ordinal());

        let staged = SchemaFieldPathIndexStagedRebuild::from_rows(
            input.accepted_after().entity_path(),
            entity_tag,
            target,
            predicate_program,
            rows,
        )
        .map_err(|_| {
            SchemaFieldPathIndexRunnerFailure::without_rollback(
                SchemaFieldPathIndexRunnerError::StageRowsFailed,
            )
        })?;
        let staged_store =
            SchemaFieldPathIndexStagedStore::from_rebuild(&staged).map_err(|_| {
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

        #[cfg(not(test))]
        let _ = write_report;

        Ok(SchemaFieldPathIndexRunnerReport {
            #[cfg(test)]
            store,
            #[cfg(test)]
            write_report,
            #[cfg(test)]
            validation,
            #[cfg(test)]
            invalidation_report,
            #[cfg(test)]
            publication_report,
            published_store_report,
        })
    }

    fn validate_mutation_plan(
        mutation_plan: &MutationPlan,
        target: &SchemaFieldPathIndexRebuildTarget,
    ) -> Result<(), SchemaFieldPathIndexRunnerError> {
        match mutation_plan {
            MutationPlan::FieldPathIndexRebuild {
                target: planned_target,
            } if planned_target == target => Ok(()),
            MutationPlan::FieldPathIndexRebuild { .. } => {
                Err(SchemaFieldPathIndexRunnerError::TargetMismatch)
            }
            _ => Err(SchemaFieldPathIndexRunnerError::UnsupportedMutationPlan),
        }
    }
}
