use super::MutationPlan;
use crate::db::schema::PersistedSchemaSnapshot;

///
/// SchemaFieldPathIndexMutationMetrics
///
/// Metrics produced by one completed physical field-path index mutation.
/// Startup reconciliation consumes row counts before schema publication, while
/// SQL DDL reports the same accepted mutation facts after publication.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexMutationMetrics {
    entity_path: &'static str,
    rows_scanned: usize,
    index_keys_written: usize,
}

impl SchemaFieldPathIndexMutationMetrics {
    /// Build metrics from the completed physical mutation report.
    #[must_use]
    pub(in crate::db::schema) const fn new(
        entity_path: &'static str,
        rows_scanned: usize,
        index_keys_written: usize,
    ) -> Self {
        Self {
            entity_path,
            rows_scanned,
            index_keys_written,
        }
    }

    /// Return the accepted entity path covered by the mutation.
    #[must_use]
    pub(in crate::db::schema) const fn entity_path(&self) -> &'static str {
        self.entity_path
    }

    /// Return the number of source rows inspected by physical mutation.
    #[must_use]
    pub(in crate::db::schema) const fn rows_scanned(&self) -> usize {
        self.rows_scanned
    }

    /// Return the number of physical index keys written by the mutation.
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db::schema) const fn index_keys_written(&self) -> usize {
        self.index_keys_written
    }
}

///
/// SchemaMutationRunnerInputError
///
/// Fail-closed input construction error before a physical runner can see a
/// schema mutation. These are catalog identity errors, not runner failures.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRunnerInputError {
    EntityPath,
    EntityName,
    PrimaryKeyField,
}

///
/// SchemaMutationRunnerInput
///
/// Accepted-schema-native input for physical mutation runners. Construction
/// validates the before/after identity boundary, then retains only the accepted
/// target snapshot and schema-owned mutation plan needed by execution.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationRunnerInput<'a> {
    accepted_after: &'a PersistedSchemaSnapshot,
    mutation_plan: MutationPlan,
}

impl<'a> SchemaMutationRunnerInput<'a> {
    pub(in crate::db::schema) fn new(
        accepted_before: &'a PersistedSchemaSnapshot,
        accepted_after: &'a PersistedSchemaSnapshot,
        mutation_plan: MutationPlan,
    ) -> Result<Self, SchemaMutationRunnerInputError> {
        if accepted_before.entity_path() != accepted_after.entity_path() {
            return Err(SchemaMutationRunnerInputError::EntityPath);
        }

        if accepted_before.entity_name() != accepted_after.entity_name() {
            return Err(SchemaMutationRunnerInputError::EntityName);
        }

        if accepted_before.primary_key_field_ids() != accepted_after.primary_key_field_ids() {
            return Err(SchemaMutationRunnerInputError::PrimaryKeyField);
        }

        Ok(Self {
            accepted_after,
            mutation_plan,
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn accepted_after(&self) -> &PersistedSchemaSnapshot {
        self.accepted_after
    }

    #[must_use]
    pub(in crate::db::schema) const fn mutation_plan(&self) -> &MutationPlan {
        &self.mutation_plan
    }
}
