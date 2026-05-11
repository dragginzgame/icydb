//! Module: db::schema::mutation
//! Responsibility: catalog-native schema mutation contracts.
//! Does not own: SQL DDL parsing, physical rebuild execution, or schema-store writes.
//! Boundary: describes accepted snapshot changes before reconciliation persists them.

use crate::db::{
    codec::{
        finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_str_u32, write_hash_tag_u8,
        write_hash_u32,
    },
    schema::{
        FieldId, PersistedFieldKind, PersistedFieldSnapshot, PersistedIndexKeySnapshot,
        PersistedIndexSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot,
    },
};

#[allow(
    dead_code,
    reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
)]
const SCHEMA_MUTATION_FINGERPRINT_PROFILE_TAG: &[u8] = b"icydb:schema-mutation-plan:v1";

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
    AddNonUniqueFieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    AddExpressionIndex {
        name: String,
    },
    DropNonRequiredSecondaryIndex {
        name: String,
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

#[allow(
    dead_code,
    reason = "0.152 stages the internal mutation request API before every request has a live caller"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRequest<'a> {
    ExactMatch,
    AppendOnlyFields(&'a [PersistedFieldSnapshot]),
    AddNonUniqueFieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    AddExpressionIndex {
        name: String,
    },
    DropNonRequiredSecondaryIndex {
        name: String,
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

#[allow(
    dead_code,
    reason = "0.152 stages fail-closed mutation lowering before DDL diagnostics expose it"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum AcceptedSchemaMutationError {
    UniqueIndexRequiresDedicatedValidation,
    UnsupportedIndexKeyShape,
    EmptyIndexKey,
}

///
/// SchemaFieldPathIndexRebuildTarget
///
/// Accepted schema-owned rebuild target for a field-path index. It carries the
/// persisted index store identity and key-slot contract that a later physical
/// rebuild runner must consume before the index can become runtime-visible.
///

#[allow(
    dead_code,
    reason = "0.152 stages rebuild target contracts before a physical runner consumes them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRebuildTarget {
    ordinal: u16,
    name: String,
    store: String,
    unique: bool,
    predicate_sql: Option<String>,
    key_paths: Vec<SchemaFieldPathIndexRebuildKey>,
}

#[allow(
    dead_code,
    reason = "0.152 stages rebuild target contracts before a physical runner consumes them"
)]
impl SchemaFieldPathIndexRebuildTarget {
    #[must_use]
    pub(in crate::db::schema) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    #[must_use]
    pub(in crate::db::schema) const fn name(&self) -> &str {
        self.name.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn unique(&self) -> bool {
        self.unique
    }

    #[must_use]
    pub(in crate::db::schema) const fn predicate_sql(&self) -> Option<&str> {
        match &self.predicate_sql {
            Some(predicate_sql) => Some(predicate_sql.as_str()),
            None => None,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn key_paths(&self) -> &[SchemaFieldPathIndexRebuildKey] {
        self.key_paths.as_slice()
    }
}

///
/// SchemaFieldPathIndexRebuildKey
///
/// One accepted field-path key component required to rebuild a secondary index
/// from accepted row-layout slots.
///

#[allow(
    dead_code,
    reason = "0.152 stages rebuild target contracts before a physical runner consumes them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRebuildKey {
    field_id: FieldId,
    slot: SchemaFieldSlot,
    path: Vec<String>,
    kind: PersistedFieldKind,
    nullable: bool,
}

#[allow(
    dead_code,
    reason = "0.152 stages rebuild target contracts before a physical runner consumes them"
)]
impl SchemaFieldPathIndexRebuildKey {
    #[must_use]
    pub(in crate::db::schema) const fn field_id(&self) -> FieldId {
        self.field_id
    }

    #[must_use]
    pub(in crate::db::schema) const fn slot(&self) -> SchemaFieldSlot {
        self.slot
    }

    #[must_use]
    pub(in crate::db::schema) const fn path(&self) -> &[String] {
        self.path.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn kind(&self) -> &PersistedFieldKind {
        &self.kind
    }

    #[must_use]
    pub(in crate::db::schema) const fn nullable(&self) -> bool {
        self.nullable
    }
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
/// SchemaRebuildIndexKind
///
/// Index family that must be physically rebuilt before one mutation can become
/// accepted runtime schema.
///

#[allow(
    dead_code,
    reason = "0.152 stages rebuild orchestration contracts before execution consumes them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaRebuildIndexKind {
    FieldPath,
    Expression,
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
    BuildIndex {
        name: String,
        kind: SchemaRebuildIndexKind,
    },
    DropIndex {
        name: String,
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
/// SchemaMutationDelta
///
/// Snapshot-delta classification between two accepted catalog snapshots. This
/// keeps structural mutation detection inside the mutation layer while the
/// transition layer remains responsible for validation and diagnostics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationDelta<'a> {
    AppendOnlyFields(&'a [PersistedFieldSnapshot]),
    ExactMatch,
    Incompatible,
}

/// Classify the structural mutation shape between an accepted snapshot and a
/// proposed replacement. This does not decide whether the mutation is safe; it
/// only names the catalog delta shape for policy code.
pub(in crate::db::schema) fn classify_schema_mutation_delta<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> SchemaMutationDelta<'a> {
    if actual == expected {
        return SchemaMutationDelta::ExactMatch;
    }

    append_only_additive_fields(actual, expected).map_or(
        SchemaMutationDelta::Incompatible,
        SchemaMutationDelta::AppendOnlyFields,
    )
}

/// Build one mutation request from the structural delta between two accepted
/// snapshots. Policy validation remains in transition; this function only
/// classifies the catalog operation to keep lowering centralized.
pub(in crate::db::schema) fn schema_mutation_request_for_snapshots<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> SchemaMutationRequest<'a> {
    SchemaMutationRequest::from(classify_schema_mutation_delta(actual, expected))
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

    /// Stage a non-unique field-path index addition from accepted index
    /// metadata. This is a planning artifact only until rebuild orchestration
    /// can construct the physical index safely.
    fn non_unique_field_path_index_addition(target: SchemaFieldPathIndexRebuildTarget) -> Self {
        Self {
            mutations: vec![SchemaMutation::AddNonUniqueFieldPathIndex { target }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuildRequired,
        }
    }

    /// Stage an accepted deterministic expression index addition. This shares
    /// the same rebuild bucket as field-path indexes but remains a separate
    /// mutation so canonical expression metadata can be audited independently.
    fn expression_index_addition(name: String) -> Self {
        Self {
            mutations: vec![SchemaMutation::AddExpressionIndex { name }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuildRequired,
        }
    }

    /// Stage a supported index drop. Runtime execution is deferred until store
    /// cleanup and planner invalidation are wired through the mutation engine.
    fn secondary_index_drop(name: String) -> Self {
        Self {
            mutations: vec![SchemaMutation::DropNonRequiredSecondaryIndex { name }],
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
    #[allow(
        dead_code,
        reason = "mutation diagnostics and DDL lowering will consume this in the next 0.152 slice"
    )]
    #[must_use]
    pub(in crate::db::schema) const fn mutations(&self) -> &[SchemaMutation] {
        self.mutations.as_slice()
    }

    /// Return the stable compatibility bucket.
    #[allow(
        dead_code,
        reason = "mutation diagnostics and DDL lowering will consume this in the next 0.152 slice"
    )]
    #[must_use]
    pub(in crate::db::schema) const fn compatibility(&self) -> MutationCompatibility {
        self.compatibility
    }

    /// Return the physical rebuild requirement.
    #[allow(
        dead_code,
        reason = "mutation diagnostics and DDL lowering will consume this in the next 0.152 slice"
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
                SchemaMutation::AddNonUniqueFieldPathIndex { target } => {
                    actions.push(SchemaRebuildAction::BuildFieldPathIndex {
                        target: target.clone(),
                    });
                }
                SchemaMutation::AddExpressionIndex { name } => {
                    actions.push(SchemaRebuildAction::BuildIndex {
                        name: name.clone(),
                        kind: SchemaRebuildIndexKind::Expression,
                    });
                }
                SchemaMutation::DropNonRequiredSecondaryIndex { name } => {
                    actions.push(SchemaRebuildAction::DropIndex { name: name.clone() });
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

    /// Compute a deterministic plan fingerprint. This is not a cache key yet;
    /// it is a stable audit identity for mutation semantics.
    #[allow(
        dead_code,
        reason = "0.152 stages mutation audit identity before diagnostics expose it"
    )]
    pub(in crate::db::schema) fn fingerprint(&self) -> [u8; 16] {
        let mut hasher = new_hash_sha256_prefixed(SCHEMA_MUTATION_FINGERPRINT_PROFILE_TAG);
        write_hash_tag_u8(&mut hasher, self.compatibility.tag());
        write_hash_tag_u8(&mut hasher, self.rebuild.tag());
        write_hash_u32(
            &mut hasher,
            u32::try_from(self.mutations.len()).unwrap_or(u32::MAX),
        );

        for mutation in &self.mutations {
            mutation.hash_into(&mut hasher);
        }

        let digest = finalize_hash_sha256(hasher);
        let mut fingerprint = [0u8; 16];
        fingerprint.copy_from_slice(&digest[..16]);
        fingerprint
    }
}

impl SchemaMutationRequest<'_> {
    /// Lower one accepted non-unique field-path index snapshot into a mutation
    /// request. Unique and expression/mixed indexes fail closed until their
    /// rebuild validators exist.
    #[allow(
        dead_code,
        reason = "0.152 stages accepted index mutation lowering before DDL/rebuild callers use it"
    )]
    pub(in crate::db::schema) fn from_accepted_non_unique_field_path_index(
        index: &PersistedIndexSnapshot,
    ) -> Result<Self, AcceptedSchemaMutationError> {
        if index.unique() {
            return Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation);
        }

        let PersistedIndexKeySnapshot::FieldPath(paths) = index.key() else {
            return Err(AcceptedSchemaMutationError::UnsupportedIndexKeyShape);
        };

        if paths.is_empty() {
            return Err(AcceptedSchemaMutationError::EmptyIndexKey);
        }

        let key_paths = paths
            .iter()
            .map(|path| SchemaFieldPathIndexRebuildKey {
                field_id: path.field_id(),
                slot: path.slot(),
                path: path.path().to_vec(),
                kind: path.kind().clone(),
                nullable: path.nullable(),
            })
            .collect();

        Ok(Self::AddNonUniqueFieldPathIndex {
            target: SchemaFieldPathIndexRebuildTarget {
                ordinal: index.ordinal(),
                name: index.name().to_string(),
                store: index.store().to_string(),
                unique: index.unique(),
                predicate_sql: index.predicate_sql().map(str::to_string),
                key_paths,
            },
        })
    }

    /// Lower this request into the deterministic mutation plan consumed by
    /// transition, publication, and future rebuild orchestration.
    #[must_use]
    pub(in crate::db::schema) fn lower_to_plan(self) -> MutationPlan {
        match self {
            Self::ExactMatch => MutationPlan::exact_match(),
            Self::AppendOnlyFields(fields) => MutationPlan::append_only_fields(fields),
            Self::AddNonUniqueFieldPathIndex { target } => {
                MutationPlan::non_unique_field_path_index_addition(target)
            }
            Self::AddExpressionIndex { name } => MutationPlan::expression_index_addition(name),
            Self::DropNonRequiredSecondaryIndex { name } => {
                MutationPlan::secondary_index_drop(name)
            }
            Self::AlterNullability { field_id } => MutationPlan::nullability_alteration(field_id),
            Self::Incompatible => MutationPlan::incompatible(),
        }
    }
}

impl<'a> From<SchemaMutationDelta<'a>> for SchemaMutationRequest<'a> {
    fn from(delta: SchemaMutationDelta<'a>) -> Self {
        match delta {
            SchemaMutationDelta::AppendOnlyFields(fields) => Self::AppendOnlyFields(fields),
            SchemaMutationDelta::ExactMatch => Self::ExactMatch,
            SchemaMutationDelta::Incompatible => Self::Incompatible,
        }
    }
}

impl SchemaMutation {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        match self {
            Self::AddNullableField {
                field_id,
                name,
                slot,
            } => {
                write_hash_tag_u8(hasher, 1);
                hash_field_identity(hasher, *field_id, name, *slot);
            }
            Self::AddDefaultedField {
                field_id,
                name,
                slot,
            } => {
                write_hash_tag_u8(hasher, 2);
                hash_field_identity(hasher, *field_id, name, *slot);
            }
            Self::AddNonUniqueFieldPathIndex { target } => {
                write_hash_tag_u8(hasher, 3);
                target.hash_into(hasher);
            }
            Self::AddExpressionIndex { name } => {
                write_hash_tag_u8(hasher, 4);
                write_hash_str_u32(hasher, name);
            }
            Self::DropNonRequiredSecondaryIndex { name } => {
                write_hash_tag_u8(hasher, 5);
                write_hash_str_u32(hasher, name);
            }
            Self::AlterNullability { field_id } => {
                write_hash_tag_u8(hasher, 6);
                write_hash_u32(hasher, field_id.get());
            }
        }
    }
}

impl MutationCompatibility {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    const fn tag(self) -> u8 {
        match self {
            Self::MetadataOnlySafe => 1,
            Self::RequiresRebuild => 2,
            Self::UnsupportedPreOne => 3,
            Self::Incompatible => 4,
        }
    }
}

impl RebuildRequirement {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    const fn tag(self) -> u8 {
        match self {
            Self::NoRebuildRequired => 1,
            Self::IndexRebuildRequired => 2,
            Self::FullDataRewriteRequired => 3,
            Self::Unsupported => 4,
        }
    }
}

impl SchemaFieldPathIndexRebuildTarget {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        write_hash_u32(hasher, u32::from(self.ordinal));
        write_hash_str_u32(hasher, &self.name);
        write_hash_str_u32(hasher, &self.store);
        write_hash_bool(hasher, self.unique);
        match &self.predicate_sql {
            Some(predicate_sql) => {
                write_hash_tag_u8(hasher, 1);
                write_hash_str_u32(hasher, predicate_sql);
            }
            None => write_hash_tag_u8(hasher, 0),
        }
        write_hash_u32(
            hasher,
            u32::try_from(self.key_paths.len()).unwrap_or(u32::MAX),
        );
        for key_path in &self.key_paths {
            key_path.hash_into(hasher);
        }
    }
}

impl SchemaFieldPathIndexRebuildKey {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        write_hash_u32(hasher, self.field_id.get());
        write_hash_u32(hasher, u32::from(self.slot.get()));
        write_hash_u32(hasher, u32::try_from(self.path.len()).unwrap_or(u32::MAX));
        for segment in &self.path {
            write_hash_str_u32(hasher, segment);
        }
        write_hash_str_u32(hasher, &format!("{:?}", self.kind));
        write_hash_bool(hasher, self.nullable);
    }
}

#[allow(
    dead_code,
    reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
)]
fn hash_field_identity(
    hasher: &mut sha2::Sha256,
    field_id: FieldId,
    name: &str,
    slot: SchemaFieldSlot,
) {
    write_hash_u32(hasher, field_id.get());
    write_hash_str_u32(hasher, name);
    write_hash_u32(hasher, u32::from(slot.get()));
}

#[allow(
    dead_code,
    reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
)]
fn write_hash_bool(hasher: &mut sha2::Sha256, value: bool) {
    write_hash_tag_u8(hasher, u8::from(value));
}

// Return generated fields for the additive shape that can become an accepted
// mutation plan: stored fields and row-layout entries must be exact prefixes of
// the generated proposal. Absence/default policy is validated by transition.
fn append_only_additive_fields<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> Option<&'a [PersistedFieldSnapshot]> {
    if actual.fields().len() >= expected.fields().len()
        || actual.row_layout().field_to_slot().len() >= expected.row_layout().field_to_slot().len()
    {
        return None;
    }

    if !actual
        .fields()
        .iter()
        .zip(expected.fields())
        .all(|(actual_field, expected_field)| actual_field == expected_field)
    {
        return None;
    }

    if !actual
        .row_layout()
        .field_to_slot()
        .iter()
        .zip(expected.row_layout().field_to_slot())
        .all(|(actual_pair, expected_pair)| actual_pair == expected_pair)
    {
        return None;
    }

    Some(&expected.fields()[actual.fields().len()..])
}

#[cfg(test)]
mod tests {
    use crate::{
        db::schema::{
            AcceptedSchemaMutationError, FieldId, MutationCompatibility, MutationPlan,
            PersistedFieldKind, PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot,
            PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
            PersistedSchemaSnapshot, RebuildRequirement, SchemaFieldDefault, SchemaFieldSlot,
            SchemaMutation, SchemaMutationDelta, SchemaMutationRequest, SchemaRebuildAction,
            SchemaRebuildIndexKind, SchemaRowLayout, SchemaVersion, classify_schema_mutation_delta,
            mutation::{MutationPublicationBlocker, MutationPublicationStatus},
            schema_mutation_request_for_snapshots,
        },
        model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
    };

    fn nullable_text_field(name: &str, id: u32, slot: u16) -> PersistedFieldSnapshot {
        PersistedFieldSnapshot::new(
            FieldId::new(id),
            name.to_string(),
            SchemaFieldSlot::new(slot),
            PersistedFieldKind::Text { max_len: None },
            Vec::new(),
            true,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        )
    }

    fn non_unique_name_index() -> PersistedIndexSnapshot {
        PersistedIndexSnapshot::new(
            1,
            "by_name".to_string(),
            "test::mutation::by_name".to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["name".to_string()],
                PersistedFieldKind::Text { max_len: None },
                false,
            )]),
            Some("name IS NOT NULL".to_string()),
        )
    }

    fn name_key_path() -> PersistedIndexFieldPathSnapshot {
        PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["name".to_string()],
            PersistedFieldKind::Text { max_len: None },
            false,
        )
    }

    #[test]
    fn append_only_field_mutation_plan_is_no_rebuild() {
        let field = nullable_text_field("nickname", 3, 2);
        let plan = MutationPlan::append_only_fields(&[field]);

        assert_eq!(
            plan.compatibility(),
            MutationCompatibility::MetadataOnlySafe
        );
        assert_eq!(
            plan.rebuild_requirement(),
            RebuildRequirement::NoRebuildRequired
        );
        assert_eq!(plan.added_field_count(), 1);
        assert_eq!(
            plan.mutations(),
            &[SchemaMutation::AddNullableField {
                field_id: FieldId::new(3),
                name: "nickname".to_string(),
                slot: SchemaFieldSlot::new(2),
            }]
        );
    }

    #[test]
    fn mutation_plan_fingerprint_is_deterministic_and_semantic() {
        let nickname = nullable_text_field("nickname", 3, 2);
        let handle = nullable_text_field("handle", 3, 2);
        let first = MutationPlan::append_only_fields(std::slice::from_ref(&nickname));
        let second = MutationPlan::append_only_fields(&[nickname]);
        let changed = MutationPlan::append_only_fields(&[handle]);

        assert_eq!(first.fingerprint(), second.fingerprint());
        assert_ne!(first.fingerprint(), changed.fingerprint());
    }

    #[test]
    fn index_mutation_plans_are_rebuild_gated() {
        let field_path = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower")
        .lower_to_plan();
        let expression = SchemaMutationRequest::AddExpressionIndex {
            name: "by_lower".to_string(),
        }
        .lower_to_plan();
        let drop = SchemaMutationRequest::DropNonRequiredSecondaryIndex {
            name: "by_name".to_string(),
        }
        .lower_to_plan();

        for plan in [&field_path, &expression, &drop] {
            assert_eq!(plan.compatibility(), MutationCompatibility::RequiresRebuild);
            assert_eq!(
                plan.rebuild_requirement(),
                RebuildRequirement::IndexRebuildRequired
            );
            assert_eq!(
                plan.publication_status(),
                MutationPublicationStatus::Blocked(MutationPublicationBlocker::NotMetadataSafe(
                    MutationCompatibility::RequiresRebuild,
                )),
            );
        }
    }

    #[test]
    fn rebuild_plan_derives_physical_index_actions() {
        let field_path = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower")
        .lower_to_plan();
        let expression = SchemaMutationRequest::AddExpressionIndex {
            name: "by_lower".to_string(),
        }
        .lower_to_plan();
        let drop = SchemaMutationRequest::DropNonRequiredSecondaryIndex {
            name: "by_name".to_string(),
        }
        .lower_to_plan();

        let field_path_rebuild = field_path.rebuild_plan();
        let [SchemaRebuildAction::BuildFieldPathIndex { target }] = field_path_rebuild.actions()
        else {
            panic!("field-path index addition should derive one field-path rebuild target");
        };
        assert_eq!(target.ordinal(), 1);
        assert_eq!(target.name(), "by_name");
        assert_eq!(target.store(), "test::mutation::by_name");
        assert!(!target.unique());
        assert_eq!(target.predicate_sql(), Some("name IS NOT NULL"));
        let [key_path] = target.key_paths() else {
            panic!("field-path rebuild target should carry one accepted key path");
        };
        assert_eq!(key_path.field_id(), FieldId::new(2));
        assert_eq!(key_path.slot(), SchemaFieldSlot::new(1));
        assert_eq!(key_path.path(), &["name".to_string()]);
        assert_eq!(key_path.kind(), &PersistedFieldKind::Text { max_len: None });
        assert!(!key_path.nullable());
        assert_eq!(
            expression.rebuild_plan().actions(),
            &[SchemaRebuildAction::BuildIndex {
                name: "by_lower".to_string(),
                kind: SchemaRebuildIndexKind::Expression,
            }],
        );
        assert_eq!(
            drop.rebuild_plan().actions(),
            &[SchemaRebuildAction::DropIndex {
                name: "by_name".to_string(),
            }],
        );
    }

    #[test]
    fn field_path_index_request_lowering_fails_closed_for_unsupported_indexes() {
        let unique = PersistedIndexSnapshot::new(
            1,
            "unique_name".to_string(),
            "test::mutation::unique_name".to_string(),
            true,
            PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
            None,
        );
        let explicit_items = PersistedIndexSnapshot::new(
            2,
            "items_name".to_string(),
            "test::mutation::items_name".to_string(),
            false,
            PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::FieldPath(
                name_key_path(),
            )]),
            None,
        );
        let empty = PersistedIndexSnapshot::new(
            3,
            "empty_name".to_string(),
            "test::mutation::empty_name".to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(Vec::new()),
            None,
        );

        assert_eq!(
            SchemaMutationRequest::from_accepted_non_unique_field_path_index(&unique),
            Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation),
        );
        assert_eq!(
            SchemaMutationRequest::from_accepted_non_unique_field_path_index(&explicit_items),
            Err(AcceptedSchemaMutationError::UnsupportedIndexKeyShape),
        );
        assert_eq!(
            SchemaMutationRequest::from_accepted_non_unique_field_path_index(&empty),
            Err(AcceptedSchemaMutationError::EmptyIndexKey),
        );
    }

    #[test]
    fn rebuild_plan_keeps_unsupported_and_full_rewrite_shapes_explicit() {
        let nullability = SchemaMutationRequest::AlterNullability {
            field_id: FieldId::new(2),
        }
        .lower_to_plan();
        let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

        assert_eq!(
            nullability.rebuild_plan().actions(),
            &[SchemaRebuildAction::Unsupported {
                reason: "alter nullability requires data proof or rewrite",
            }],
        );
        assert_eq!(
            incompatible.rebuild_plan().actions(),
            &[SchemaRebuildAction::RewriteAllRows],
        );
    }

    #[test]
    fn unsupported_mutation_plans_fail_closed() {
        let alteration = SchemaMutationRequest::AlterNullability {
            field_id: FieldId::new(2),
        }
        .lower_to_plan();
        let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

        assert_eq!(
            alteration.compatibility(),
            MutationCompatibility::UnsupportedPreOne
        );
        assert_eq!(
            alteration.rebuild_requirement(),
            RebuildRequirement::Unsupported
        );
        assert_eq!(
            alteration.publication_status(),
            MutationPublicationStatus::Blocked(MutationPublicationBlocker::NotMetadataSafe(
                MutationCompatibility::UnsupportedPreOne,
            )),
        );
        assert_eq!(
            incompatible.compatibility(),
            MutationCompatibility::Incompatible
        );
        assert_eq!(
            incompatible.rebuild_requirement(),
            RebuildRequirement::FullDataRewriteRequired
        );
    }

    #[test]
    fn publication_gate_allows_only_metadata_safe_no_rebuild_plans() {
        let field = nullable_text_field("nickname", 3, 2);
        let append_only = MutationPlan::append_only_fields(&[field]);
        let metadata_safe_but_rebuild_required = MutationPlan {
            mutations: Vec::new(),
            compatibility: MutationCompatibility::MetadataOnlySafe,
            rebuild: RebuildRequirement::IndexRebuildRequired,
        };
        let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

        assert_eq!(
            append_only.publication_status(),
            MutationPublicationStatus::Publishable
        );
        assert_eq!(
            metadata_safe_but_rebuild_required.publication_status(),
            MutationPublicationStatus::Blocked(MutationPublicationBlocker::RebuildRequired(
                RebuildRequirement::IndexRebuildRequired,
            )),
        );
        assert_eq!(
            incompatible.publication_status(),
            MutationPublicationStatus::Blocked(MutationPublicationBlocker::NotMetadataSafe(
                MutationCompatibility::Incompatible,
            )),
        );
    }

    fn base_snapshot() -> PersistedSchemaSnapshot {
        PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "test::MutationEntity".to_string(),
            "MutationEntity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                ],
            ),
            vec![
                PersistedFieldSnapshot::new(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "name".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Text { max_len: None },
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Text),
                ),
            ],
        )
    }

    #[test]
    fn snapshot_delta_classifier_names_append_only_fields() {
        let stored = base_snapshot();
        let mut fields = stored.fields().to_vec();
        fields.push(nullable_text_field("nickname", 3, 2));
        let generated = PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.primary_key_field_id(),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                    (FieldId::new(3), SchemaFieldSlot::new(2)),
                ],
            ),
            fields,
        );

        let SchemaMutationDelta::AppendOnlyFields(added_fields) =
            classify_schema_mutation_delta(&stored, &generated)
        else {
            panic!("append-only snapshot change should classify as appended fields");
        };

        assert_eq!(added_fields.len(), 1);
        assert_eq!(added_fields[0].name(), "nickname");
    }

    #[test]
    fn snapshot_delta_request_lowers_append_only_fields_to_mutation_plan() {
        let stored = base_snapshot();
        let mut fields = stored.fields().to_vec();
        fields.push(nullable_text_field("nickname", 3, 2));
        let generated = PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.primary_key_field_id(),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                    (FieldId::new(3), SchemaFieldSlot::new(2)),
                ],
            ),
            fields,
        );

        let SchemaMutationRequest::AppendOnlyFields(added_fields) =
            schema_mutation_request_for_snapshots(&stored, &generated)
        else {
            panic!("append-only snapshot change should lower into append-only request");
        };

        let plan = SchemaMutationRequest::AppendOnlyFields(added_fields).lower_to_plan();
        assert_eq!(plan.added_field_count(), 1);
        assert_eq!(
            plan.publication_status(),
            MutationPublicationStatus::Publishable
        );
    }

    #[test]
    fn snapshot_delta_classifier_rejects_non_prefix_field_changes() {
        let stored = base_snapshot();
        let mut generated_fields = stored.fields().to_vec();
        generated_fields[1] = nullable_text_field("renamed", 2, 1);
        let generated = PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.primary_key_field_id(),
            stored.row_layout().clone(),
            generated_fields,
        );

        assert_eq!(
            classify_schema_mutation_delta(&stored, &generated),
            SchemaMutationDelta::Incompatible
        );
    }
}
