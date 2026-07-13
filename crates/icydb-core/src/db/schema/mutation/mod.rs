//! Module: db::schema::mutation
//! Responsibility: catalog-native schema mutation contracts.
//! Does not own: SQL DDL parsing, physical rebuild execution, or schema-store writes.
//! Boundary: describes accepted snapshot changes before reconciliation persists them.

use crate::db::{
    data::CanonicalSlotReader,
    index::{
        IndexEntryValue, IndexId, IndexKey, IndexState, IndexStore, IndexStoreVisit,
        RawIndexStoreKey,
    },
    key_taxonomy::PrimaryKeyValue,
    predicate::PredicateProgram,
    schema::{FieldId, PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot},
};
use crate::error::InternalError;
use crate::types::EntityTag;

#[cfg(feature = "sql")]
mod field;
#[cfg(feature = "sql")]
pub(in crate::db) use field::{
    SchemaDdlFieldDefaultCandidateError, SchemaDdlFieldDropCandidateError,
    SchemaDdlFieldNullabilityCandidateError, SchemaDdlFieldRenameCandidateError,
    SchemaFieldAdditionTarget, SchemaFieldDefaultTarget, SchemaFieldDropTarget,
    SchemaFieldNullabilityTarget, SchemaFieldRenameTarget,
    derive_sql_ddl_field_addition_accepted_after, derive_sql_ddl_field_default_accepted_after,
    derive_sql_ddl_field_drop_accepted_after, derive_sql_ddl_field_nullability_accepted_after,
    derive_sql_ddl_field_rename_accepted_after, resolve_sql_ddl_field_drop_candidate,
    resolve_sql_ddl_field_drop_default_candidate, resolve_sql_ddl_field_nullability_candidate,
    resolve_sql_ddl_field_rename_candidate, resolve_sql_ddl_field_set_default_candidate,
    validate_sql_ddl_field_default_change_candidate,
};
#[cfg(all(test, feature = "sql"))]
pub(in crate::db) use field::{
    admit_sql_ddl_field_addition_candidate, admit_sql_ddl_field_default_candidate,
    admit_sql_ddl_field_drop_candidate, admit_sql_ddl_field_nullability_candidate,
    admit_sql_ddl_field_rename_candidate,
};

#[cfg(feature = "sql")]
mod field_allocation;
#[cfg(feature = "sql")]
pub(in crate::db) use field_allocation::{
    SchemaDdlFieldAdditionCandidateError, build_sql_ddl_field_addition_candidate,
    resolve_sql_ddl_field_addition_name_candidate,
};

#[cfg(feature = "sql")]
mod field_default_encoding;
#[cfg(feature = "sql")]
pub(in crate::db) use field_default_encoding::{
    encode_sql_ddl_add_column_default, encode_sql_ddl_alter_column_default,
};

#[cfg(feature = "sql")]
mod field_type;
#[cfg(feature = "sql")]
pub(in crate::db) use field_type::{
    SchemaDdlFieldTypeContract, resolve_sql_ddl_field_type_contract,
};

#[cfg(feature = "sql")]
mod ddl_admission;
#[cfg(feature = "sql")]
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "schema root re-exports DDL schema-version admission diagnostics"
    )
)]
pub(in crate::db) use ddl_admission::{
    SchemaDdlAcceptedSnapshotDerivation, SchemaDdlIndexDropCandidateError,
    SchemaDdlMutationAdmission, SchemaDdlMutationAdmissionError, SchemaDdlMutationTarget,
    SchemaDdlSchemaVersionAdmissionError, SchemaDdlVersionContractPreflightError,
    validate_schema_ddl_version_contract_preflight,
};

mod delta;
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "mutation planning tests consume delta classifiers through the module root"
    )
)]
pub(in crate::db::schema) use delta::{
    SchemaMutationDelta, classify_schema_mutation_delta, schema_mutation_request_for_snapshots,
};

#[cfg(feature = "sql")]
mod index_candidate;
#[cfg(feature = "sql")]
pub(in crate::db) use index_candidate::{
    SchemaDdlSecondaryIndexAdditionCandidate, SchemaDdlSecondaryIndexAdditionCandidateError,
    SchemaDdlSecondaryIndexExpressionIntent, SchemaDdlSecondaryIndexExpressionOpIntent,
    SchemaDdlSecondaryIndexFieldPathIntent, SchemaDdlSecondaryIndexKeyCandidateError,
    SchemaDdlSecondaryIndexKeyIntent, build_sql_ddl_secondary_index_candidate,
    resolve_sql_ddl_secondary_index_addition_candidate,
};

mod index;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use index::SchemaSecondaryIndexDropCleanupTarget;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use index::{
    SchemaExpressionIndexRebuildExpression, SchemaExpressionIndexRebuildKey,
};
pub(in crate::db) use index::{
    SchemaExpressionIndexRebuildTarget, SchemaFieldPathIndexRebuildKey,
    SchemaFieldPathIndexRebuildTarget,
};
#[cfg(all(test, feature = "sql"))]
pub(in crate::db) use index::{
    admit_sql_ddl_expression_index_candidate, admit_sql_ddl_field_path_index_candidate,
    admit_sql_ddl_secondary_index_drop_candidate,
};
#[cfg(feature = "sql")]
pub(in crate::db) use index::{
    derive_sql_ddl_expression_index_accepted_after, derive_sql_ddl_field_path_index_accepted_after,
    derive_sql_ddl_secondary_index_drop_accepted_after,
    resolve_sql_ddl_secondary_index_drop_candidate,
};

mod identity;
#[cfg(test)]
pub(in crate::db::schema::mutation) use identity::write_hash_bool;
pub(in crate::db::schema) use identity::{
    SchemaMutationPublicationIdentity, SchemaMutationRuntimeEpoch,
};

mod staged_index_validation;
pub(in crate::db::schema) use staged_index_validation::{
    SchemaStagedIndexValidationError, staged_index_keys_have_duplicate_unique_components,
};

///
/// SchemaMutation
///
/// SchemaMutation is the schema-owned description of one accepted catalog
/// change. It is intentionally independent of SQL syntax so parser frontends
/// must lower into this contract instead of becoming the migration authority.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutation {
    NullableField {
        field_id: FieldId,
        name: String,
        slot: SchemaFieldSlot,
    },
    DefaultedField {
        field_id: FieldId,
        name: String,
        slot: SchemaFieldSlot,
    },
    FieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    ExpressionIndex {
        target: SchemaExpressionIndexRebuildTarget,
    },
    #[cfg(any(test, feature = "sql"))]
    DropNonRequiredSecondaryIndex {
        target: SchemaSecondaryIndexDropCleanupTarget,
    },
    #[cfg(test)]
    AlterNullability { field_id: FieldId },
}

///
/// SchemaMutationRequest
///
/// Internal request vocabulary that lowers catalog-level mutation intent into
/// a deterministic `MutationPlan`. SQL DDL and generated proposal comparison
/// must route through this type instead of constructing plans ad hoc.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRequest<'a> {
    ExactMatch,
    AppendOnlyFields(&'a [PersistedFieldSnapshot]),
    AddFieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    AddExpressionIndex {
        target: SchemaExpressionIndexRebuildTarget,
    },
    #[cfg(any(test, feature = "sql"))]
    DropNonRequiredSecondaryIndex {
        target: SchemaSecondaryIndexDropCleanupTarget,
    },
    #[cfg(test)]
    AlterNullability {
        field_id: FieldId,
    },
    Incompatible,
}

///
/// AcceptedSchemaMutationError
///
/// Fail-closed reason produced while lowering accepted schema metadata into a
/// mutation request. These errors mean the mutation framework cannot describe
/// a safe catalog operation yet; callers must not compensate with generated
/// metadata.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedSchemaMutationError {
    UnsupportedIndexKeyShape,
    EmptyIndexKey,
    ExpressionIndexRequiresExpressionKey,
}

///
/// MutationCompatibility
///
/// Stable high-level compatibility bucket for one mutation plan. This is kept
/// small so unsupported schema changes fail closed instead of leaking through
/// as ad hoc snapshot rewrites.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum MutationCompatibility {
    MetadataOnlySafe,
    RequiresRebuild,
    #[cfg(test)]
    UnsupportedPreOne,
    Incompatible,
}

///
/// RebuildRequirement
///
/// Physical work required before a mutation can be considered runtime-visible.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum RebuildRequirement {
    NoRebuild,
    IndexRebuild,
    FullDataRewrite,
    #[cfg(test)]
    Unsupported,
}

///
/// SchemaRebuildAction
///
/// One physical rebuild action implied by a catalog mutation plan. These
/// actions are planning facts only; 0.152 still blocks publication until an
/// executor owns the physical work and validation boundary.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaRebuildAction {
    BuildFieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    BuildExpressionIndex {
        target: SchemaExpressionIndexRebuildTarget,
    },
    #[cfg(any(test, feature = "sql"))]
    DropSecondaryIndex {
        target: SchemaSecondaryIndexDropCleanupTarget,
    },
    RewriteAllRows,
    Unsupported {
        reason: &'static str,
    },
}

///
/// SchemaRebuildPlan
///
/// Schema-owned physical work classification derived from a mutation plan.
/// Reconciliation asks publication status before exposing a new accepted
/// snapshot; rebuild plans are the audit/debug shape that will later feed the
/// physical rebuild runner.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaRebuildPlan {
    requirement: RebuildRequirement,
    actions: Vec<SchemaRebuildAction>,
}

impl SchemaRebuildPlan {
    const fn no_rebuild() -> Self {
        Self {
            requirement: RebuildRequirement::NoRebuild,
            actions: Vec::new(),
        }
    }

    const fn new(requirement: RebuildRequirement, actions: Vec<SchemaRebuildAction>) -> Self {
        Self {
            requirement,
            actions,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn requirement(&self) -> RebuildRequirement {
        self.requirement
    }

    #[must_use]
    pub(in crate::db::schema) const fn actions(&self) -> &[SchemaRebuildAction] {
        self.actions.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn requires_physical_work(&self) -> bool {
        !matches!(self.requirement, RebuildRequirement::NoRebuild)
    }

    #[must_use]
    const fn publication_blocker(&self) -> Option<MutationPublicationBlocker> {
        if self.requires_physical_work() {
            return Some(MutationPublicationBlocker::RebuildRequired(
                self.requirement,
            ));
        }

        None
    }
}

mod runner;
pub(in crate::db::schema) use self::runner::*;

mod execution;
pub(in crate::db::schema) use self::execution::*;

mod field_path;
pub(in crate::db::schema) use self::field_path::*;

mod expression;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::schema) use self::expression::*;

///
/// MutationPublicationBlocker
///
/// Fail-closed reason preventing one mutation plan from becoming accepted
/// runtime schema. This is intentionally separate from the compatibility and
/// rebuild enums so reconciliation asks one schema-owned publication gate
/// instead of reimplementing publishability rules locally.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum MutationPublicationBlocker {
    NotMetadataSafe(MutationCompatibility),
    RebuildRequired(RebuildRequirement),
}

///
/// MutationPublicationStatus
///
/// Runtime publication decision for one mutation plan.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum MutationPublicationStatus {
    Publishable,
    Blocked(MutationPublicationBlocker),
}

///
/// MutationPlan
///
/// Deterministic schema-owned plan for moving one accepted snapshot to the
/// next. Startup reconciliation can currently execute only no-rebuild plans;
/// future DDL/rebuild work should extend this type before widening behavior.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct MutationPlan {
    mutations: Vec<SchemaMutation>,
    compatibility: MutationCompatibility,
    rebuild: RebuildRequirement,
}

impl MutationPlan {
    /// Build the no-op plan for equal accepted snapshots.
    pub(in crate::db::schema) const fn exact_match() -> Self {
        Self {
            mutations: Vec::new(),
            compatibility: MutationCompatibility::MetadataOnlySafe,
            rebuild: RebuildRequirement::NoRebuild,
        }
    }

    /// Build the currently executable append-only field plan. The caller owns
    /// validating nullable/default absence semantics before publishing it.
    pub(in crate::db::schema) fn append_only_fields(fields: &[PersistedFieldSnapshot]) -> Self {
        let mutations = fields
            .iter()
            .map(|field| {
                if field.default().is_none() {
                    SchemaMutation::NullableField {
                        field_id: field.id(),
                        name: field.name().to_string(),
                        slot: field.slot(),
                    }
                } else {
                    SchemaMutation::DefaultedField {
                        field_id: field.id(),
                        name: field.name().to_string(),
                        slot: field.slot(),
                    }
                }
            })
            .collect();

        Self {
            mutations,
            compatibility: MutationCompatibility::MetadataOnlySafe,
            rebuild: RebuildRequirement::NoRebuild,
        }
    }

    /// Stage a field-path index addition from accepted index metadata. This is
    /// a planning artifact only until rebuild orchestration can construct and
    /// validate the physical index safely.
    fn field_path_index_addition(target: SchemaFieldPathIndexRebuildTarget) -> Self {
        Self {
            mutations: vec![SchemaMutation::FieldPathIndex { target }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuild,
        }
    }

    /// Stage an accepted deterministic expression index addition. This shares
    /// the same rebuild bucket as field-path indexes but remains a separate
    /// mutation so canonical expression metadata can be audited independently.
    fn expression_index_addition(target: SchemaExpressionIndexRebuildTarget) -> Self {
        Self {
            mutations: vec![SchemaMutation::ExpressionIndex { target }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuild,
        }
    }

    /// Stage a supported index drop. Runtime execution is deferred until store
    /// cleanup and planner invalidation are wired through the mutation engine.
    #[cfg(any(test, feature = "sql"))]
    fn secondary_index_drop(target: SchemaSecondaryIndexDropCleanupTarget) -> Self {
        Self {
            mutations: vec![SchemaMutation::DropNonRequiredSecondaryIndex { target }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuild,
        }
    }

    /// Stage a nullability alteration. Pre-1.0 this remains fail-closed because
    /// existing data must be proven or rewritten before accepting it.
    #[cfg(test)]
    fn nullability_alteration(field_id: FieldId) -> Self {
        Self {
            mutations: vec![SchemaMutation::AlterNullability { field_id }],
            compatibility: MutationCompatibility::UnsupportedPreOne,
            rebuild: RebuildRequirement::Unsupported,
        }
    }

    /// Build the generic incompatible plan used by guard tests and future
    /// diagnostics for rejected snapshot changes.
    const fn incompatible() -> Self {
        Self {
            mutations: Vec::new(),
            compatibility: MutationCompatibility::Incompatible,
            rebuild: RebuildRequirement::FullDataRewrite,
        }
    }

    /// Borrow the ordered mutation list.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn mutations(&self) -> &[SchemaMutation] {
        self.mutations.as_slice()
    }

    /// Return the stable compatibility bucket.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn compatibility(&self) -> MutationCompatibility {
        self.compatibility
    }

    /// Return the physical rebuild requirement.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn rebuild_requirement(&self) -> RebuildRequirement {
        self.rebuild
    }

    /// Decide whether this mutation plan can be published as accepted runtime
    /// schema without additional physical rebuild work.
    #[must_use]
    pub(in crate::db::schema) fn publication_status(&self) -> MutationPublicationStatus {
        if !matches!(self.compatibility, MutationCompatibility::MetadataOnlySafe) {
            return MutationPublicationStatus::Blocked(
                MutationPublicationBlocker::NotMetadataSafe(self.compatibility),
            );
        }

        if let Some(blocker) = self.rebuild_plan().publication_blocker() {
            return MutationPublicationStatus::Blocked(blocker);
        }

        MutationPublicationStatus::Publishable
    }

    /// Consult runner preflight before deciding whether publication can proceed.
    /// `PhysicalWorkReady` is still not publishable in 0.152; it only means a
    /// future runner advertises the capabilities required before execution can
    /// start.
    #[must_use]
    pub(in crate::db::schema) fn publication_preflight(
        &self,
        runner: &SchemaMutationRunnerContract,
    ) -> MutationPublicationPreflight {
        match runner.preflight(&self.execution_plan()) {
            SchemaMutationRunnerPreflight::NoPhysicalWork => match self.publication_status() {
                MutationPublicationStatus::Publishable => {
                    MutationPublicationPreflight::PublishableNow
                }
                MutationPublicationStatus::Blocked(blocker) => {
                    MutationPublicationPreflight::Blocked(blocker)
                }
            },
            SchemaMutationRunnerPreflight::Ready {
                step_count,
                required,
            } => MutationPublicationPreflight::PhysicalWorkReady {
                step_count,
                required,
            },
            SchemaMutationRunnerPreflight::MissingCapabilities { missing } => {
                MutationPublicationPreflight::MissingRunnerCapabilities { missing }
            }
            SchemaMutationRunnerPreflight::Rejected { requirement } => {
                MutationPublicationPreflight::Rejected { requirement }
            }
        }
    }

    /// Derive the physical rebuild plan required before this catalog mutation
    /// can safely become accepted runtime schema.
    #[must_use]
    pub(in crate::db::schema) fn rebuild_plan(&self) -> SchemaRebuildPlan {
        if matches!(self.rebuild, RebuildRequirement::NoRebuild) {
            return SchemaRebuildPlan::no_rebuild();
        }

        let mut actions = Vec::new();
        for mutation in &self.mutations {
            match mutation {
                SchemaMutation::NullableField { .. } | SchemaMutation::DefaultedField { .. } => {}
                SchemaMutation::FieldPathIndex { target } => {
                    actions.push(SchemaRebuildAction::BuildFieldPathIndex {
                        target: target.clone(),
                    });
                }
                SchemaMutation::ExpressionIndex { target } => {
                    actions.push(SchemaRebuildAction::BuildExpressionIndex {
                        target: target.clone(),
                    });
                }
                #[cfg(any(test, feature = "sql"))]
                SchemaMutation::DropNonRequiredSecondaryIndex { target } => {
                    actions.push(SchemaRebuildAction::DropSecondaryIndex {
                        target: target.clone(),
                    });
                }
                #[cfg(test)]
                SchemaMutation::AlterNullability { .. } => {
                    actions.push(SchemaRebuildAction::Unsupported {
                        reason: "alter nullability requires data proof or rewrite",
                    });
                }
            }
        }

        if actions.is_empty() {
            actions.push(match self.rebuild {
                RebuildRequirement::FullDataRewrite => SchemaRebuildAction::RewriteAllRows,
                #[cfg(test)]
                RebuildRequirement::Unsupported => SchemaRebuildAction::Unsupported {
                    reason: "unsupported schema mutation",
                },
                RebuildRequirement::IndexRebuild => SchemaRebuildAction::Unsupported {
                    reason: "index rebuild mutation lacks an index target",
                },
                RebuildRequirement::NoRebuild => {
                    unreachable!("schema mutation invariant",)
                }
            });
        }

        SchemaRebuildPlan::new(self.rebuild, actions)
    }

    /// Derive the future physical execution contract for this mutation plan.
    /// Startup reconciliation still uses `publication_status` and remains
    /// fail-closed for every plan that requires physical work.
    #[must_use]
    pub(in crate::db::schema) fn execution_plan(&self) -> SchemaMutationExecutionPlan {
        SchemaMutationExecutionPlan::from_rebuild_plan(self.rebuild_plan())
    }

    /// Admit the developer-supported physical mutation shape: exactly one
    /// field-path secondary index addition. This plan-level guard prevents
    /// mixed catalog changes from slipping through a valid-looking execution
    /// step sequence.
    pub(in crate::db::schema) fn supported_developer_physical_path(
        &self,
    ) -> Result<SchemaMutationSupportedExecutionPath, SchemaMutationSupportedPathRejection> {
        let [SchemaMutation::FieldPathIndex { target }] = self.mutations.as_slice() else {
            return match self.rebuild {
                RebuildRequirement::NoRebuild => {
                    Err(SchemaMutationSupportedPathRejection::NoPhysicalWork)
                }
                RebuildRequirement::IndexRebuild => {
                    Err(SchemaMutationSupportedPathRejection::UnsupportedMutationKind)
                }
                RebuildRequirement::FullDataRewrite => {
                    Err(SchemaMutationSupportedPathRejection::UnsupportedRequirement(self.rebuild))
                }
                #[cfg(test)]
                RebuildRequirement::Unsupported => {
                    Err(SchemaMutationSupportedPathRejection::UnsupportedRequirement(self.rebuild))
                }
            };
        };

        let supported = self.execution_plan().supported_developer_execution_path()?;
        if supported.target() != target {
            return Err(SchemaMutationSupportedPathRejection::UnsupportedExecutionShape);
        }

        Ok(supported)
    }

    /// Return how many appended fields are represented by this plan.
    #[cfg(test)]
    pub(in crate::db::schema) fn added_field_count(&self) -> usize {
        self.mutations
            .iter()
            .filter(|mutation| {
                matches!(
                    mutation,
                    SchemaMutation::NullableField { .. } | SchemaMutation::DefaultedField { .. }
                )
            })
            .count()
    }
}

impl SchemaMutationRequest<'_> {
    /// Lower this request into the deterministic mutation plan consumed by
    /// transition, publication, and future rebuild orchestration.
    #[must_use]
    pub(in crate::db::schema) fn lower_to_plan(self) -> MutationPlan {
        match self {
            Self::ExactMatch => MutationPlan::exact_match(),
            Self::AppendOnlyFields(fields) => MutationPlan::append_only_fields(fields),
            Self::AddFieldPathIndex { target } => MutationPlan::field_path_index_addition(target),
            Self::AddExpressionIndex { target } => MutationPlan::expression_index_addition(target),
            #[cfg(any(test, feature = "sql"))]
            Self::DropNonRequiredSecondaryIndex { target } => {
                MutationPlan::secondary_index_drop(target)
            }
            #[cfg(test)]
            Self::AlterNullability { field_id } => MutationPlan::nullability_alteration(field_id),
            Self::Incompatible => MutationPlan::incompatible(),
        }
    }
}

#[cfg(test)]
mod tests;
