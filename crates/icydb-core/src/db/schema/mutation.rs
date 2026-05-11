//! Module: db::schema::mutation
//! Responsibility: catalog-native schema mutation contracts.
//! Does not own: SQL DDL parsing, rebuild orchestration, or schema-store writes.
//! Boundary: describes accepted snapshot changes before reconciliation persists them.

use crate::db::{
    codec::{
        finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_str_u32, write_hash_tag_u8,
        write_hash_u32,
    },
    schema::{FieldId, PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot},
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
    AddNonUniqueIndex {
        name: String,
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

    /// Stage a non-unique index addition. This is a planning artifact only
    /// until rebuild orchestration can construct the physical index safely.
    #[cfg(test)]
    pub(in crate::db::schema) fn planned_non_unique_index_addition(name: String) -> Self {
        Self {
            mutations: vec![SchemaMutation::AddNonUniqueIndex { name }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuildRequired,
        }
    }

    /// Stage an accepted deterministic expression index addition. This shares
    /// the same rebuild bucket as field-path indexes but remains a separate
    /// mutation so canonical expression metadata can be audited independently.
    #[cfg(test)]
    pub(in crate::db::schema) fn planned_expression_index_addition(name: String) -> Self {
        Self {
            mutations: vec![SchemaMutation::AddExpressionIndex { name }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuildRequired,
        }
    }

    /// Stage a supported index drop. Runtime execution is deferred until store
    /// cleanup and planner invalidation are wired through the mutation engine.
    #[cfg(test)]
    pub(in crate::db::schema) fn planned_secondary_index_drop(name: String) -> Self {
        Self {
            mutations: vec![SchemaMutation::DropNonRequiredSecondaryIndex { name }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuildRequired,
        }
    }

    /// Stage a nullability alteration. Pre-1.0 this remains fail-closed because
    /// existing data must be proven or rewritten before accepting it.
    #[cfg(test)]
    pub(in crate::db::schema) fn unsupported_nullability_alteration(field_id: FieldId) -> Self {
        Self {
            mutations: vec![SchemaMutation::AlterNullability { field_id }],
            compatibility: MutationCompatibility::UnsupportedPreOne,
            rebuild: RebuildRequirement::Unsupported,
        }
    }

    /// Build the generic incompatible plan used by guard tests and future
    /// diagnostics for rejected snapshot changes.
    #[cfg(test)]
    pub(in crate::db::schema) const fn incompatible() -> Self {
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
            Self::AddNonUniqueIndex { name } => {
                write_hash_tag_u8(hasher, 3);
                write_hash_str_u32(hasher, name);
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
            FieldId, MutationCompatibility, MutationPlan, PersistedFieldKind,
            PersistedFieldSnapshot, PersistedSchemaSnapshot, RebuildRequirement,
            SchemaFieldDefault, SchemaFieldSlot, SchemaMutation, SchemaMutationDelta,
            SchemaRowLayout, SchemaVersion, classify_schema_mutation_delta,
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
        let field_path = MutationPlan::planned_non_unique_index_addition("by_name".to_string());
        let expression = MutationPlan::planned_expression_index_addition("by_lower".to_string());
        let drop = MutationPlan::planned_secondary_index_drop("by_name".to_string());

        for plan in [&field_path, &expression, &drop] {
            assert_eq!(plan.compatibility(), MutationCompatibility::RequiresRebuild);
            assert_eq!(
                plan.rebuild_requirement(),
                RebuildRequirement::IndexRebuildRequired
            );
        }
    }

    #[test]
    fn unsupported_mutation_plans_fail_closed() {
        let alteration = MutationPlan::unsupported_nullability_alteration(FieldId::new(2));
        let incompatible = MutationPlan::incompatible();

        assert_eq!(
            alteration.compatibility(),
            MutationCompatibility::UnsupportedPreOne
        );
        assert_eq!(
            alteration.rebuild_requirement(),
            RebuildRequirement::Unsupported
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
