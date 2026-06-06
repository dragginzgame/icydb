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
use std::collections::BTreeMap;

#[cfg(any(test, feature = "sql"))]
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
};
#[cfg(test)]
pub(in crate::db) use field::{
    admit_sql_ddl_field_addition_candidate, admit_sql_ddl_field_default_candidate,
    admit_sql_ddl_field_drop_candidate, admit_sql_ddl_field_nullability_candidate,
    admit_sql_ddl_field_rename_candidate,
};

#[cfg(any(test, feature = "sql"))]
mod field_allocation;
#[cfg(feature = "sql")]
pub(in crate::db) use field_allocation::{
    SchemaDdlFieldAdditionCandidateError, build_sql_ddl_field_addition_candidate,
    resolve_sql_ddl_field_addition_name_candidate,
};

#[cfg(any(test, feature = "sql"))]
mod field_default_encoding;
#[cfg(feature = "sql")]
pub(in crate::db) use field_default_encoding::{
    encode_sql_ddl_add_column_default, encode_sql_ddl_alter_column_default,
};

#[cfg(any(test, feature = "sql"))]
mod field_type;
#[cfg(feature = "sql")]
pub(in crate::db) use field_type::{
    SchemaDdlFieldTypeContract, resolve_sql_ddl_field_type_contract,
};

#[cfg(any(test, feature = "sql"))]
mod ddl_admission;
#[cfg(any(test, feature = "sql"))]
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

#[cfg(any(test, feature = "sql"))]
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
pub(in crate::db) use index::{
    SchemaExpressionIndexRebuildExpression, SchemaExpressionIndexRebuildKey,
    SchemaExpressionIndexRebuildTarget, SchemaFieldPathIndexRebuildKey,
    SchemaFieldPathIndexRebuildTarget, SchemaSecondaryIndexDropCleanupTarget,
};
#[cfg(test)]
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
pub(in crate::db::schema::mutation) use identity::write_hash_bool;
pub(in crate::db::schema) use identity::{
    SchemaMutationPublicationIdentity, SchemaMutationRuntimeEpoch,
};

///
/// SchemaMutation
///
/// SchemaMutation is the schema-owned description of one accepted catalog
/// change. It is intentionally independent of SQL syntax so parser frontends
/// must lower into this contract instead of becoming the migration authority.
///

#[allow(
    dead_code,
    reason = "0.152 defines the first mutation vocabulary before every operation is executable"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutation {
    AddNullableField {
        field_id: FieldId,
        name: String,
        slot: SchemaFieldSlot,
    },
    AddDefaultedField {
        field_id: FieldId,
        name: String,
        slot: SchemaFieldSlot,
    },
    AddFieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    AddExpressionIndex {
        target: SchemaExpressionIndexRebuildTarget,
    },
    DropNonRequiredSecondaryIndex {
        target: SchemaSecondaryIndexDropCleanupTarget,
    },
    AlterNullability {
        field_id: FieldId,
    },
}

///
/// SchemaMutationRequest
///
/// Internal request vocabulary that lowers catalog-level mutation intent into
/// a deterministic `MutationPlan`. SQL DDL and generated proposal comparison
/// must route through this type instead of constructing plans ad hoc.
///

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "0.152 stages the internal mutation request API before every request has a live caller"
    )
)]
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
    DropNonRequiredSecondaryIndex {
        target: SchemaSecondaryIndexDropCleanupTarget,
    },
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

#[allow(
    dead_code,
    reason = "0.152 stages rebuild and unsupported buckets before every bucket has a live caller"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum MutationCompatibility {
    MetadataOnlySafe,
    RequiresRebuild,
    UnsupportedPreOne,
    Incompatible,
}

///
/// RebuildRequirement
///
/// Physical work required before a mutation can be considered runtime-visible.
///

#[allow(
    dead_code,
    reason = "0.152 exposes future rebuild buckets before orchestration consumes them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum RebuildRequirement {
    NoRebuildRequired,
    IndexRebuildRequired,
    FullDataRewriteRequired,
    Unsupported,
}

///
/// SchemaRebuildAction
///
/// One physical rebuild action implied by a catalog mutation plan. These
/// actions are planning facts only; 0.152 still blocks publication until an
/// executor owns the physical work and validation boundary.
///

#[allow(
    dead_code,
    reason = "0.152 stages rebuild orchestration contracts before execution consumes them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaRebuildAction {
    BuildFieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    BuildExpressionIndex {
        target: SchemaExpressionIndexRebuildTarget,
    },
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

#[allow(
    dead_code,
    reason = "0.152 stages rebuild orchestration contracts before execution consumes them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaRebuildPlan {
    requirement: RebuildRequirement,
    actions: Vec<SchemaRebuildAction>,
}

#[allow(
    dead_code,
    reason = "0.152 stages rebuild orchestration contracts before execution consumes them"
)]
impl SchemaRebuildPlan {
    const fn no_rebuild() -> Self {
        Self {
            requirement: RebuildRequirement::NoRebuildRequired,
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
        !matches!(self.requirement, RebuildRequirement::NoRebuildRequired)
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
#[allow(
    unused_imports,
    reason = "expression staging is consumed by tests and later physical runner wiring"
)]
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
            rebuild: RebuildRequirement::NoRebuildRequired,
        }
    }

    /// Build the currently executable append-only field plan. The caller owns
    /// validating nullable/default absence semantics before publishing it.
    pub(in crate::db::schema) fn append_only_fields(fields: &[PersistedFieldSnapshot]) -> Self {
        let mutations = fields
            .iter()
            .map(|field| {
                if field.default().is_none() {
                    SchemaMutation::AddNullableField {
                        field_id: field.id(),
                        name: field.name().to_string(),
                        slot: field.slot(),
                    }
                } else {
                    SchemaMutation::AddDefaultedField {
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
            rebuild: RebuildRequirement::NoRebuildRequired,
        }
    }

    /// Stage a field-path index addition from accepted index metadata. This is
    /// a planning artifact only until rebuild orchestration can construct and
    /// validate the physical index safely.
    fn field_path_index_addition(target: SchemaFieldPathIndexRebuildTarget) -> Self {
        Self {
            mutations: vec![SchemaMutation::AddFieldPathIndex { target }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuildRequired,
        }
    }

    /// Stage an accepted deterministic expression index addition. This shares
    /// the same rebuild bucket as field-path indexes but remains a separate
    /// mutation so canonical expression metadata can be audited independently.
    fn expression_index_addition(target: SchemaExpressionIndexRebuildTarget) -> Self {
        Self {
            mutations: vec![SchemaMutation::AddExpressionIndex { target }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuildRequired,
        }
    }

    /// Stage a supported index drop. Runtime execution is deferred until store
    /// cleanup and planner invalidation are wired through the mutation engine.
    fn secondary_index_drop(target: SchemaSecondaryIndexDropCleanupTarget) -> Self {
        Self {
            mutations: vec![SchemaMutation::DropNonRequiredSecondaryIndex { target }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuildRequired,
        }
    }

    /// Stage a nullability alteration. Pre-1.0 this remains fail-closed because
    /// existing data must be proven or rewritten before accepting it.
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
            rebuild: RebuildRequirement::FullDataRewriteRequired,
        }
    }

    /// Borrow the ordered mutation list.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "mutation diagnostics and DDL lowering will consume this in the next 0.152 slice"
        )
    )]
    #[must_use]
    pub(in crate::db::schema) const fn mutations(&self) -> &[SchemaMutation] {
        self.mutations.as_slice()
    }

    /// Return the stable compatibility bucket.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "mutation diagnostics and DDL lowering will consume this in the next 0.152 slice"
        )
    )]
    #[must_use]
    pub(in crate::db::schema) const fn compatibility(&self) -> MutationCompatibility {
        self.compatibility
    }

    /// Return the physical rebuild requirement.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "mutation diagnostics and DDL lowering will consume this in the next 0.152 slice"
        )
    )]
    #[must_use]
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
    #[allow(
        dead_code,
        reason = "0.152 stages runner preflight publication checks before physical runners consume them"
    )]
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
        if matches!(self.rebuild, RebuildRequirement::NoRebuildRequired) {
            return SchemaRebuildPlan::no_rebuild();
        }

        let mut actions = Vec::new();
        for mutation in &self.mutations {
            match mutation {
                SchemaMutation::AddNullableField { .. }
                | SchemaMutation::AddDefaultedField { .. } => {}
                SchemaMutation::AddFieldPathIndex { target } => {
                    actions.push(SchemaRebuildAction::BuildFieldPathIndex {
                        target: target.clone(),
                    });
                }
                SchemaMutation::AddExpressionIndex { target } => {
                    actions.push(SchemaRebuildAction::BuildExpressionIndex {
                        target: target.clone(),
                    });
                }
                SchemaMutation::DropNonRequiredSecondaryIndex { target } => {
                    actions.push(SchemaRebuildAction::DropSecondaryIndex {
                        target: target.clone(),
                    });
                }
                SchemaMutation::AlterNullability { .. } => {
                    actions.push(SchemaRebuildAction::Unsupported {
                        reason: "alter nullability requires data proof or rewrite",
                    });
                }
            }
        }

        if actions.is_empty() {
            actions.push(match self.rebuild {
                RebuildRequirement::FullDataRewriteRequired => SchemaRebuildAction::RewriteAllRows,
                RebuildRequirement::Unsupported => SchemaRebuildAction::Unsupported {
                    reason: "unsupported schema mutation",
                },
                RebuildRequirement::IndexRebuildRequired => SchemaRebuildAction::Unsupported {
                    reason: "index rebuild mutation lacks an index target",
                },
                RebuildRequirement::NoRebuildRequired => {
                    unreachable!("no-rebuild plans returned before rebuild action derivation",)
                }
            });
        }

        SchemaRebuildPlan::new(self.rebuild, actions)
    }

    /// Derive the future physical execution contract for this mutation plan.
    /// Startup reconciliation still uses `publication_status` and remains
    /// fail-closed for every plan that requires physical work.
    #[allow(
        dead_code,
        reason = "0.152 stages execution-boundary contracts before physical runners consume them"
    )]
    #[must_use]
    pub(in crate::db::schema) fn execution_plan(&self) -> SchemaMutationExecutionPlan {
        SchemaMutationExecutionPlan::from_rebuild_plan(self.rebuild_plan())
    }

    /// Admit the developer-supported physical mutation shape: exactly one
    /// field-path secondary index addition. This plan-level guard prevents
    /// mixed catalog changes from slipping through a valid-looking execution
    /// step sequence.
    #[allow(
        dead_code,
        reason = "0.154 starts supported-path admission before reconciliation consumes it"
    )]
    pub(in crate::db::schema) fn supported_developer_physical_path(
        &self,
    ) -> Result<SchemaMutationSupportedExecutionPath, SchemaMutationSupportedPathRejection> {
        let [SchemaMutation::AddFieldPathIndex { target }] = self.mutations.as_slice() else {
            return match self.rebuild {
                RebuildRequirement::NoRebuildRequired => {
                    Err(SchemaMutationSupportedPathRejection::NoPhysicalWork)
                }
                RebuildRequirement::IndexRebuildRequired => {
                    Err(SchemaMutationSupportedPathRejection::UnsupportedMutationKind)
                }
                RebuildRequirement::FullDataRewriteRequired | RebuildRequirement::Unsupported => {
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
                    SchemaMutation::AddNullableField { .. }
                        | SchemaMutation::AddDefaultedField { .. }
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
            Self::DropNonRequiredSecondaryIndex { target } => {
                MutationPlan::secondary_index_drop(target)
            }
            Self::AlterNullability { field_id } => MutationPlan::nullability_alteration(field_id),
            Self::Incompatible => MutationPlan::incompatible(),
        }
    }
}

#[cfg(test)]
mod tests;
