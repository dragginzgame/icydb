//! Module: db::schema::integrity
//! Responsibility: persisted schema metadata integrity checks.
//! Does not own: reconciliation policy, schema transition decisions, or raw codec parsing.
//! Boundary: reports local metadata inconsistencies before snapshots become accepted authority.

mod constraint;
mod index;
mod nullable_unique;
mod relation;

use crate::db::schema::{
    AcceptedFieldKind, FieldId, FieldInsertGeneration, PersistedFieldSnapshot, RowLayoutVersion,
    SchemaHistoricalFill, SchemaRowLayout, SchemaVersion,
};

pub(in crate::db::schema) use constraint::schema_snapshot_constraint_integrity_detail;
pub(in crate::db::schema) use index::schema_snapshot_index_integrity_detail;
pub(in crate::db) use nullable_unique::NullableUniqueIndexContractError;
pub(in crate::db) use nullable_unique::validate_nullable_unique_index_contract;
pub(in crate::db) use relation::accepted_relation_path_terminal;
pub(in crate::db::schema) use relation::accepted_relation_sources_match_catalogs;
pub(in crate::db::schema) use relation::schema_snapshot_relation_integrity_detail;

/// The smallest acceptance taxonomy required beyond historical structure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaSnapshotAcceptanceError {
    Structural,
    Predicate,
    NullableUnique(NullableUniqueIndexContractError),
}

/// Validate one complete persisted snapshot before it becomes authority.
pub(in crate::db) fn validate_schema_snapshot_acceptance(
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
) -> Result<(), SchemaSnapshotAcceptanceError> {
    let all_indexes = snapshot
        .indexes()
        .iter()
        .chain(snapshot.candidate_indexes())
        .cloned()
        .collect::<Vec<_>>();
    let all_relations = snapshot
        .relations()
        .iter()
        .chain(snapshot.candidate_relations())
        .cloned()
        .collect::<Vec<_>>();
    let relation_id_high_water = snapshot.relation_id_allocator().high_water();
    if schema_snapshot_integrity_detail(
        "persisted schema snapshot",
        snapshot.version(),
        snapshot.primary_key_field_ids(),
        snapshot.row_layout(),
        snapshot.fields(),
    )
    .is_some()
        || schema_snapshot_index_integrity_detail(
            "persisted schema snapshot",
            snapshot.row_layout(),
            snapshot.fields(),
            all_indexes.as_slice(),
        )
        .is_some()
        || schema_snapshot_relation_integrity_detail(
            "persisted schema snapshot",
            snapshot.row_layout(),
            snapshot.fields(),
            all_relations.as_slice(),
        )
        .is_some()
        || all_relations
            .iter()
            .any(|relation| relation.id().get() > relation_id_high_water)
        || schema_snapshot_constraint_integrity_detail(
            snapshot.primary_key_field_ids(),
            snapshot.fields(),
            snapshot.indexes(),
            snapshot.relations(),
            snapshot.candidate_indexes(),
            snapshot.candidate_relations(),
            snapshot.constraint_catalog(),
        )
        .is_some()
    {
        return Err(SchemaSnapshotAcceptanceError::Structural);
    }

    for index in snapshot
        .indexes()
        .iter()
        .chain(snapshot.candidate_indexes())
    {
        validate_nullable_unique_index_contract(snapshot.row_layout(), snapshot.fields(), index)?;
    }
    Ok(())
}

// Build the first deterministic persisted-schema integrity diagnostic. Callers
// decide whether the detail represents a typed caller invariant or raw payload
// corruption, but the schema module owns the actual metadata consistency rules.
pub(in crate::db::schema) fn schema_snapshot_integrity_detail(
    subject: &str,
    version: SchemaVersion,
    primary_key_field_ids: &[FieldId],
    row_layout: &SchemaRowLayout,
    fields: &[PersistedFieldSnapshot],
) -> Option<()> {
    if version.get() == 0 {
        return Some(());
    }

    if row_layout.history_floor() > row_layout.current_version() {
        return Some(());
    }

    if let Some(detail) = duplicate_row_layout_detail(subject, row_layout) {
        return Some(detail);
    }

    if let Some(detail) = duplicate_field_detail(subject, fields) {
        return Some(detail);
    }

    if primary_key_field_ids.is_empty() {
        return Some(());
    }

    for (index, primary_key_field_id) in primary_key_field_ids.iter().enumerate() {
        if primary_key_field_ids[..index].contains(primary_key_field_id) {
            return Some(());
        }

        if row_layout.slot_for_field(*primary_key_field_id).is_none() {
            return Some(());
        }
    }

    if row_layout.field_to_slot().len() != fields.len() {
        return Some(());
    }

    let mut prior_introduction = RowLayoutVersion::INITIAL;
    for (index, ((field_id, slot), field)) in
        row_layout.field_to_slot().iter().zip(fields).enumerate()
    {
        let expected_id = u32::try_from(index).ok()?.checked_add(1)?;
        let expected_slot = u16::try_from(index).ok()?;
        if field_id.get() != expected_id
            || slot.get() != expected_slot
            || field.id() != *field_id
            || field.slot() != *slot
        {
            return Some(());
        }

        let introduced = field.introduced_in_layout();
        if introduced > row_layout.current_version() || introduced < prior_introduction {
            return Some(());
        }
        prior_introduction = introduced;

        match field.historical_fill() {
            SchemaHistoricalFill::Reject if introduced > row_layout.history_floor() => {
                return Some(());
            }
            SchemaHistoricalFill::Null if !field.nullable() => return Some(()),
            SchemaHistoricalFill::Null | SchemaHistoricalFill::SlotPayload(_)
                if introduced <= row_layout.history_floor() =>
            {
                return Some(());
            }
            SchemaHistoricalFill::Reject
            | SchemaHistoricalFill::Null
            | SchemaHistoricalFill::SlotPayload(_) => {}
        }
    }

    let mut matched_primary_key_fields = 0usize;
    for field in fields {
        if !field.kind().has_valid_local_shape()
            || field.leaf_codec() != field.kind().leaf_codec_for_storage(field.storage_decode())
        {
            return Some(());
        }

        if primary_key_field_ids.contains(&field.id()) {
            matched_primary_key_fields += 1;
            if !matches!(field.historical_fill(), SchemaHistoricalFill::Reject) {
                return Some(());
            }
        }

        let Some(row_layout_slot) = row_layout.slot_for_field(field.id()) else {
            return Some(());
        };

        if row_layout_slot != field.slot() {
            return Some(());
        }
    }

    if matched_primary_key_fields != primary_key_field_ids.len() {
        return Some(());
    }

    if insert_generation_detail(primary_key_field_ids, fields).is_some() {
        return Some(());
    }

    None
}

// Keep database-owned insert synthesis exact in accepted authority. Identity
// is narrower than the existing stateless generators: it belongs only to one
// generated, non-null, exact unsigned scalar sole primary key.
fn insert_generation_detail(
    primary_key_field_ids: &[FieldId],
    fields: &[PersistedFieldSnapshot],
) -> Option<()> {
    for field in fields {
        let Some(generation) = field.write_policy().insert_generation() else {
            continue;
        };
        if !field.generated()
            || field.nullable()
            || field.insert_default().slot_payload().is_some()
            || field.write_policy().write_management().is_some()
        {
            return Some(());
        }

        match (generation, field.kind()) {
            (
                FieldInsertGeneration::Identity,
                AcceptedFieldKind::Nat8
                | AcceptedFieldKind::Nat16
                | AcceptedFieldKind::Nat32
                | AcceptedFieldKind::Nat64
                | AcceptedFieldKind::Nat128,
            ) if primary_key_field_ids == [field.id()] => {}
            (FieldInsertGeneration::Ulid, AcceptedFieldKind::Ulid)
            | (FieldInsertGeneration::Timestamp, AcceptedFieldKind::Timestamp) => {}
            (
                FieldInsertGeneration::Identity
                | FieldInsertGeneration::Ulid
                | FieldInsertGeneration::Timestamp,
                _,
            ) => return Some(()),
        }
    }

    None
}

// Find duplicate row-layout entries before slot lookup can hide the ambiguity
// by returning only the first matching field ID.
fn duplicate_row_layout_detail(_subject: &str, row_layout: &SchemaRowLayout) -> Option<()> {
    let entries = row_layout.field_to_slot();
    for (index, (field_id, slot)) in entries.iter().enumerate() {
        for (other_field_id, other_slot) in &entries[index + 1..] {
            if field_id == other_field_id {
                return Some(());
            }

            if slot == other_slot {
                return Some(());
            }
        }
    }

    None
}

// Find duplicate persisted field entries before name or field-ID lookup can
// become order-dependent. Accepted schema metadata must be unambiguous.
fn duplicate_field_detail(subject: &str, fields: &[PersistedFieldSnapshot]) -> Option<()> {
    for (index, field) in fields.iter().enumerate() {
        for other in &fields[index + 1..] {
            if field.id() == other.id() {
                return Some(());
            }

            if field.name() == other.name() {
                return Some(());
            }
        }

        if let Some(detail) = nested_leaf_detail(subject, field) {
            return Some(detail);
        }
    }

    None
}

// Find ambiguous nested leaf descriptors before accepted field-path inference
// can become first-match dependent. Nested paths are local to their owning
// top-level field, so uniqueness is enforced per field.
fn nested_leaf_detail(_subject: &str, field: &PersistedFieldSnapshot) -> Option<()> {
    for (index, leaf) in field.nested_leaves().iter().enumerate() {
        if leaf.path().is_empty() || !leaf.kind().has_valid_local_shape() {
            return Some(());
        }

        for other in &field.nested_leaves()[index + 1..] {
            if leaf.path() == other.path() {
                return Some(());
            }
        }
    }

    None
}
