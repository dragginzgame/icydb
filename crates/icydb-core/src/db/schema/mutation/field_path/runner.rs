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
    ReadyStoreRejected,
}

impl SchemaFieldPathIndexRunnerError {
    /// Collapse the runner's typed phase failure into the shared runtime
    /// taxonomy without treating runner-internal contradictions as supported
    /// schema rejections.
    pub(in crate::db::schema) fn into_internal_error(self) -> InternalError {
        match self {
            Self::UnsupportedMutationPlan | Self::StageRowsFailed | Self::StagedStoreRejected => {
                InternalError::store_unsupported()
            }
            Self::TargetMismatch
            | Self::IsolatedStoreValidationFailed
            | Self::ReadyStoreRejected => InternalError::store_invariant(),
        }
    }
}

/// SchemaFieldPathIndexRunnerReport
///
/// Physical runner report for one accepted field-path index rebuild. Accepted
/// snapshot publication remains outside this report and is owned by the
/// reconciliation publication gate.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRunnerReport {
    #[cfg(test)]
    store: String,
    #[cfg(test)]
    validation: SchemaFieldPathIndexIsolatedIndexStoreValidation,
    rollback_plan: SchemaFieldPathIndexStagedStoreRollbackPlan,
    ready_store_report: SchemaFieldPathIndexReadyStoreReport,
}

impl SchemaFieldPathIndexRunnerReport {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
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
    pub(in crate::db::schema) const fn ready_store_report(
        &self,
    ) -> &SchemaFieldPathIndexReadyStoreReport {
        &self.ready_store_report
    }

    #[must_use]
    pub(in crate::db::schema) const fn staged_validation(
        &self,
    ) -> SchemaFieldPathIndexStagedValidation {
        self.ready_store_report.staged_validation()
    }

    pub(in crate::db::schema) fn rollback_physical_work(&self, index_store: &mut IndexStore) {
        SchemaFieldPathIndexRunner::rollback_index_store(
            self.rollback_plan.store(),
            &self.rollback_plan,
            index_store,
        );
    }

    #[must_use]
    pub(in crate::db::schema) const fn mutation_metrics(
        &self,
        entity_path: &'static str,
    ) -> SchemaFieldPathIndexMutationMetrics {
        SchemaFieldPathIndexMutationMetrics::new(
            entity_path,
            self.staged_validation().source_rows(),
            self.staged_validation().entry_count(),
        )
    }
}
/// SchemaFieldPathIndexRunner
///
/// Narrow physical runner for one accepted field-path index rebuild. The
/// runner only accepts the schema-owned field-path execution shape and keeps
/// row staging, isolated store mutation, and physical validation in one fixed
/// order.
///

pub(in crate::db::schema) struct SchemaFieldPathIndexRunner;

impl SchemaFieldPathIndexRunner {
    pub(in crate::db::schema) fn run<'a>(
        input: &SchemaMutationRunnerInput<'_>,
        entity_tag: EntityTag,
        target: SchemaFieldPathIndexRebuildTarget,
        predicate_program: Option<&PredicateProgram>,
        rows: impl IntoIterator<Item = SchemaFieldPathIndexRebuildRow<'a>>,
        index_store: &mut IndexStore,
    ) -> Result<SchemaFieldPathIndexRunnerReport, SchemaFieldPathIndexRunnerError> {
        Self::validate_mutation_plan(input.mutation_plan(), &target)?;
        let target_index_id = IndexId::new(entity_tag, target.ordinal());

        let staged = SchemaFieldPathIndexStagedRebuild::from_rows(
            input.accepted_after().entity_path(),
            entity_tag,
            target,
            predicate_program,
            rows,
        )
        .map_err(|_| SchemaFieldPathIndexRunnerError::StageRowsFailed)?;
        let staged_store = SchemaFieldPathIndexStagedStore::from_rebuild(&staged)
            .map_err(|_| SchemaFieldPathIndexRunnerError::StagedStoreRejected)?;
        let store = staged_store.store().to_string();
        let (validation, rollback_plan) = {
            let mut writer = SchemaFieldPathIndexIsolatedIndexStoreWriter::new(&store, index_store);
            let batch = staged_store.write_batch(&writer);
            let rollback_plan = batch.rollback_plan();
            batch.write_to(&mut writer);
            let Ok(validation) = writer.validate_batch_for_target_index(&target_index_id, &batch)
            else {
                rollback_plan.rollback_to(&mut writer);
                writer.index_store.mark_ready();
                return Err(SchemaFieldPathIndexRunnerError::IsolatedStoreValidationFailed);
            };

            (validation, rollback_plan)
        };
        let ready_store_plan = SchemaFieldPathIndexReadyStorePlan::from_validation(&validation);
        let Ok(ready_store_report) =
            ready_store_plan.mark_index_store_ready_for_target_index(&target_index_id, index_store)
        else {
            Self::rollback_index_store(&store, &rollback_plan, index_store);
            return Err(SchemaFieldPathIndexRunnerError::ReadyStoreRejected);
        };

        Ok(SchemaFieldPathIndexRunnerReport {
            #[cfg(test)]
            store,
            #[cfg(test)]
            validation,
            rollback_plan,
            ready_store_report,
        })
    }

    fn rollback_index_store(
        store: &str,
        rollback_plan: &SchemaFieldPathIndexStagedStoreRollbackPlan,
        index_store: &mut IndexStore,
    ) {
        let mut writer = SchemaFieldPathIndexIsolatedIndexStoreWriter::new(store, index_store);
        rollback_plan.rollback_to(&mut writer);
        writer.index_store.mark_ready();
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
