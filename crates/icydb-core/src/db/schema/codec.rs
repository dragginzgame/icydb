//! Module: db::schema::codec
//! Responsibility: direct bounded persisted-schema snapshot encoding.
//! Does not own: reconciliation policy, schema proposal construction, or row decoding.
//! Boundary: accepted domain types <-> current `SchemaStore` payload bytes.

mod constraint;
mod field;
mod index;
mod mapping;

use crate::{
    db::schema::{
        AcceptedConstraintCatalog, ConstraintIdAllocator, FieldId, PersistedSchemaSnapshot,
        RowLayoutVersion, SchemaFieldSlot, SchemaRowLayout, SchemaSnapshotAcceptanceError,
        SchemaVersion,
        enum_catalog::MAX_SCHEMA_STORE_PATH_BYTES,
        validate_schema_snapshot_acceptance,
        wire::{SchemaWireReader, SchemaWireWriter},
    },
    error::InternalError,
};
use constraint::{decode_activation, decode_constraint, encode_activation, encode_constraint};
use field::{decode_field, encode_field};
use index::{decode_index, decode_relation, encode_index, encode_relation};
use mapping::{decode_sequence, encode_sequence};
#[cfg(test)]
use std::cell::Cell;

const SCHEMA_SNAPSHOT_MAGIC: [u8; 8] = *b"ICYUSNP\0";
const SCHEMA_SNAPSHOT_FORMAT_VERSION: u8 = 1;
/// Maximum canonical bytes for one persisted entity-schema snapshot.
pub(in crate::db) const MAX_SCHEMA_SNAPSHOT_BYTES: u32 = 512 * 1024;

type SnapshotWriter = SchemaWireWriter<{ MAX_SCHEMA_SNAPSHOT_BYTES as usize }>;
type SnapshotReader<'a> = SchemaWireReader<'a>;

const MAX_NAME_BYTES: usize = icydb_schema::MAX_SCHEMA_NAME_BYTES;
const MAX_SLOT_PAYLOAD_BYTES: usize = MAX_SCHEMA_SNAPSHOT_BYTES as usize;
const MAX_SQL_TEXT_BYTES: usize = MAX_SCHEMA_SNAPSHOT_BYTES as usize;
const MAX_CONSTRAINT_ACTIVATIONS: usize = 512;

#[cfg(test)]
thread_local! {
    static PERSISTED_SCHEMA_SNAPSHOT_DECODE_CALLS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
pub(in crate::db) fn reset_persisted_schema_snapshot_decode_count_for_tests() {
    PERSISTED_SCHEMA_SNAPSHOT_DECODE_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(in crate::db) fn persisted_schema_snapshot_decode_count_for_tests() -> u64 {
    PERSISTED_SCHEMA_SNAPSHOT_DECODE_CALLS.with(Cell::get)
}

/// Encode one accepted persisted-schema snapshot into its sole current format.
pub(in crate::db) fn encode_persisted_schema_snapshot(
    snapshot: &PersistedSchemaSnapshot,
) -> Result<Vec<u8>, InternalError> {
    if validate_schema_snapshot_acceptance(snapshot).is_err() {
        return Err(InternalError::store_invariant());
    }
    encode_snapshot(snapshot)
}

/// Encode an intentionally malformed domain fixture for decode-boundary tests.
#[cfg(test)]
pub(in crate::db) fn encode_unchecked_persisted_schema_snapshot_for_tests(
    snapshot: &PersistedSchemaSnapshot,
) -> Result<Vec<u8>, InternalError> {
    encode_snapshot(snapshot)
}

fn encode_snapshot(snapshot: &PersistedSchemaSnapshot) -> Result<Vec<u8>, InternalError> {
    let mut writer = SnapshotWriter::new();
    writer.push_bytes(&SCHEMA_SNAPSHOT_MAGIC);
    writer.push_u8(SCHEMA_SNAPSHOT_FORMAT_VERSION);
    writer.push_u32(snapshot.version().get());
    writer.push_bounded_string(snapshot.entity_path(), MAX_SCHEMA_STORE_PATH_BYTES)?;
    writer.push_bounded_string(snapshot.entity_name(), MAX_NAME_BYTES)?;
    encode_sequence!(
        writer,
        snapshot.primary_key_field_ids(),
        icydb_schema::MAX_FRAGMENT_FIELDS,
        |field_id| {
            writer.push_u32(field_id.get());
        }
    );
    encode_row_layout(&mut writer, snapshot.row_layout())?;
    writer.push_u32(snapshot.constraint_id_allocator().high_water());
    encode_sequence!(
        writer,
        snapshot.constraints(),
        icydb_schema::MAX_FRAGMENT_CONSTRAINTS,
        |constraint| {
            encode_constraint(&mut writer, constraint)?;
        }
    );
    encode_sequence!(
        writer,
        snapshot.constraint_activations(),
        MAX_CONSTRAINT_ACTIVATIONS,
        |activation| {
            encode_activation(&mut writer, activation)?;
        }
    );
    encode_sequence!(
        writer,
        snapshot.fields(),
        icydb_schema::MAX_FRAGMENT_FIELDS,
        |field| {
            encode_field(&mut writer, field)?;
        }
    );
    encode_sequence!(
        writer,
        snapshot.indexes(),
        icydb_schema::MAX_FRAGMENT_INDEXES,
        |index| {
            encode_index(&mut writer, index)?;
        }
    );
    encode_sequence!(
        writer,
        snapshot.relations(),
        icydb_schema::MAX_FRAGMENT_RELATIONS,
        |relation| {
            encode_relation(&mut writer, relation)?;
        }
    );
    encode_sequence!(
        writer,
        snapshot.candidate_indexes(),
        icydb_schema::MAX_FRAGMENT_INDEXES,
        |index| {
            encode_index(&mut writer, index)?;
        }
    );
    encode_sequence!(
        writer,
        snapshot.candidate_relations(),
        icydb_schema::MAX_FRAGMENT_RELATIONS,
        |relation| {
            encode_relation(&mut writer, relation)?;
        }
    );
    writer.finish()
}

/// Decode one accepted persisted-schema snapshot from its sole current format.
pub(in crate::db) fn decode_persisted_schema_snapshot(
    bytes: &[u8],
) -> Result<PersistedSchemaSnapshot, InternalError> {
    if bytes.len() > MAX_SCHEMA_SNAPSHOT_BYTES as usize {
        return Err(InternalError::store_corruption());
    }

    #[cfg(test)]
    PERSISTED_SCHEMA_SNAPSHOT_DECODE_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));

    let mut reader = SnapshotReader::new(bytes);
    if reader.read_array::<8>()? != SCHEMA_SNAPSHOT_MAGIC {
        return Err(InternalError::store_corruption());
    }
    if reader.read_u8()? != SCHEMA_SNAPSHOT_FORMAT_VERSION {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }

    let version = SchemaVersion::new(reader.read_u32()?);
    let entity_path = reader.read_bounded_string(MAX_SCHEMA_STORE_PATH_BYTES)?;
    let entity_name = reader.read_bounded_string(MAX_NAME_BYTES)?;
    let primary_key_field_ids = decode_sequence!(reader, icydb_schema::MAX_FRAGMENT_FIELDS, {
        FieldId::new(reader.read_u32()?)
    });
    let row_layout = decode_row_layout(&mut reader)?;
    let constraint_id_allocator = ConstraintIdAllocator::new(reader.read_u32()?);
    let constraints = decode_sequence!(reader, icydb_schema::MAX_FRAGMENT_CONSTRAINTS, {
        decode_constraint(&mut reader)?
    });
    let activations = decode_sequence!(reader, MAX_CONSTRAINT_ACTIVATIONS, {
        decode_activation(&mut reader)?
    });
    let fields = decode_sequence!(reader, icydb_schema::MAX_FRAGMENT_FIELDS, {
        decode_field(&mut reader)?
    });
    let indexes = decode_sequence!(reader, icydb_schema::MAX_FRAGMENT_INDEXES, {
        decode_index(&mut reader)?
    });
    let relations = decode_sequence!(reader, icydb_schema::MAX_FRAGMENT_RELATIONS, {
        decode_relation(&mut reader)?
    });
    let candidate_indexes = decode_sequence!(reader, icydb_schema::MAX_FRAGMENT_INDEXES, {
        decode_index(&mut reader)?
    });
    let candidate_relations = decode_sequence!(reader, icydb_schema::MAX_FRAGMENT_RELATIONS, {
        decode_relation(&mut reader)?
    });
    reader.finish()?;

    let constraint_catalog = AcceptedConstraintCatalog::from_persisted_parts(
        constraint_id_allocator,
        constraints,
        activations,
    );
    let snapshot = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        version,
        entity_path,
        entity_name,
        primary_key_field_ids,
        row_layout,
        fields,
        indexes,
    )
    .with_constraint_catalog(constraint_catalog)
    .with_relations(relations)
    .with_constraint_candidates(candidate_indexes, candidate_relations);
    match validate_schema_snapshot_acceptance(&snapshot) {
        Ok(()) => {}
        Err(SchemaSnapshotAcceptanceError::NullableUnique(_)) => {
            return Err(InternalError::serialize_incompatible_persisted_format());
        }
        Err(
            SchemaSnapshotAcceptanceError::Structural | SchemaSnapshotAcceptanceError::Predicate,
        ) => {
            return Err(InternalError::store_corruption());
        }
    }
    Ok(snapshot)
}

fn encode_row_layout(
    writer: &mut SnapshotWriter,
    layout: &SchemaRowLayout,
) -> Result<(), InternalError> {
    writer.push_u32(layout.current_version().get());
    writer.push_u32(layout.history_floor().get());
    encode_sequence!(
        writer,
        layout.field_to_slot(),
        icydb_schema::MAX_FRAGMENT_FIELDS,
        |entry| {
            writer.push_u32(entry.0.get());
            writer.push_u16(entry.1.get());
        }
    );
    Ok(())
}

fn decode_row_layout(reader: &mut SnapshotReader<'_>) -> Result<SchemaRowLayout, InternalError> {
    let current_version =
        RowLayoutVersion::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
    let history_floor =
        RowLayoutVersion::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
    let field_to_slot = decode_sequence!(reader, icydb_schema::MAX_FRAGMENT_FIELDS, {
        (
            FieldId::new(reader.read_u32()?),
            SchemaFieldSlot::new(reader.read_u16()?),
        )
    });
    Ok(SchemaRowLayout::new(
        current_version,
        history_floor,
        field_to_slot,
    ))
}

#[cfg(test)]
mod tests;
