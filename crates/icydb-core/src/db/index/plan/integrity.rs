//! Module: db::index::plan::integrity
//! Responsibility: accepted-native forward-index witnesses for integrity inspection.
//! Does not own: physical traversal, finding classification, or index publication.
//! Boundary: accepted snapshot + decoded row -> optional exact active index witness.

use crate::{
    db::{
        data::{CanonicalSlotReader, StructuralRowContract},
        index::{
            IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey,
            plan::{
                accepted_expression_index_key_for_slot_reader_with_membership_structural,
                accepted_field_path_index_key_for_slot_reader_with_membership_structural,
            },
            raw_keys_for_component_prefix_with_kind,
        },
        key_taxonomy::PrimaryKeyValue,
        predicate::{PredicateProgram, normalize, parse_sql_predicate},
        schema::{
            AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, PersistedIndexKeySnapshot,
            SchemaExpressionIndexInfo, SchemaIndexId, SchemaIndexInfo, SchemaInfo,
        },
    },
    error::InternalError,
    types::EntityTag,
};
use std::ops::Bound;

/// Exact active forward-index witness expected for one accepted row.

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedIndexInspectionWitness {
    schema_index_id: SchemaIndexId,
    store_path: String,
    raw_key: RawIndexStoreKey,
}

impl AcceptedIndexInspectionWitness {
    /// Return the stable logical index identity.
    #[must_use]
    pub(in crate::db) const fn schema_index_id(&self) -> SchemaIndexId {
        self.schema_index_id
    }

    /// Borrow the backing index-store path.
    #[must_use]
    pub(in crate::db) const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }

    /// Borrow the exact active physical key.
    #[must_use]
    pub(in crate::db) const fn raw_key(&self) -> &RawIndexStoreKey {
        &self.raw_key
    }
}

/// Exact active physical domain for one accepted forward index.
///
/// The logical schema ID remains diagnostic identity. The dense ordinal and
/// generation are used only to delimit the accepted physical keyspace.
#[derive(Clone, Debug)]
pub(in crate::db) struct AcceptedIndexInspectionDomain {
    schema_index_id: SchemaIndexId,
    store_path: String,
    physical_index_id: IndexId,
    key_component_count: usize,
    unique: bool,
}

impl AcceptedIndexInspectionDomain {
    /// Return the stable logical accepted index identity.
    #[must_use]
    pub(in crate::db) const fn schema_index_id(&self) -> SchemaIndexId {
        self.schema_index_id
    }

    /// Borrow the registry-owned index store path.
    #[must_use]
    pub(in crate::db) const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }

    /// Return whether this accepted physical domain enforces uniqueness.
    #[must_use]
    pub(in crate::db) const fn unique(&self) -> bool {
        self.unique
    }

    /// Return the accepted physical generation encoded in this domain.
    #[must_use]
    pub(in crate::db) const fn physical_generation(&self) -> u64 {
        self.physical_index_id.generation()
    }

    /// Build canonical inclusive bounds for this active generation only.
    pub(in crate::db) fn raw_bounds(
        &self,
    ) -> Result<(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>), InternalError> {
        let (lower, upper) = raw_keys_for_component_prefix_with_kind::<Vec<u8>>(
            &self.physical_index_id,
            IndexKeyKind::User,
            self.key_component_count,
            &[],
        )
        .map_err(|_| InternalError::store_corruption())?;

        Ok((Bound::Included(lower), Bound::Included(upper)))
    }

    /// Prove that one decoded key names this exact accepted physical domain.
    #[must_use]
    pub(in crate::db) fn contains_decoded_key(&self, key: &IndexKey) -> bool {
        key.key_kind() == IndexKeyKind::User && key.index_id() == &self.physical_index_id
    }
}

/// Accepted forward-index contracts compiled once with one inspection plan.

#[derive(Clone, Debug)]
pub(in crate::db) struct AcceptedIndexInspectionPlan {
    indexes: Vec<AcceptedIndexInspectionEntry>,
}

impl AcceptedIndexInspectionPlan {
    /// Compile every active accepted index in persisted catalog order.
    pub(in crate::db) fn compile(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: AcceptedValueCatalogHandle,
        row_contract: &StructuralRowContract,
    ) -> Result<Self, InternalError> {
        let schema_info =
            SchemaInfo::from_accepted_snapshot_and_catalog(schema, value_catalog, true);
        let snapshot = schema.persisted_snapshot();
        let mut indexes = Vec::with_capacity(snapshot.indexes().len());

        for accepted in snapshot.indexes() {
            let predicate = compile_predicate(accepted.predicate_sql(), row_contract)?;
            let entry = match accepted.key() {
                PersistedIndexKeySnapshot::FieldPath(_) => {
                    let info = schema_info
                        .field_path_indexes()
                        .iter()
                        .find(|info| info.ordinal() == accepted.ordinal())
                        .cloned()
                        .ok_or_else(InternalError::store_corruption)?;
                    validate_projection_identity(
                        info.ordinal(),
                        info.physical_generation(),
                        info.name(),
                        info.store(),
                        info.unique(),
                        accepted,
                    )?;
                    AcceptedIndexInspectionEntry::FieldPath {
                        schema_index_id: accepted.schema_id(),
                        info,
                        predicate,
                    }
                }
                PersistedIndexKeySnapshot::Items(_) => {
                    let info = schema_info
                        .expression_indexes()
                        .iter()
                        .find(|info| info.ordinal() == accepted.ordinal())
                        .cloned()
                        .ok_or_else(InternalError::store_corruption)?;
                    validate_projection_identity(
                        info.ordinal(),
                        info.physical_generation(),
                        info.name(),
                        info.store(),
                        info.unique(),
                        accepted,
                    )?;
                    AcceptedIndexInspectionEntry::Expression {
                        schema_index_id: accepted.schema_id(),
                        info,
                        predicate,
                    }
                }
            };
            indexes.push(entry);
        }

        Ok(Self { indexes })
    }

    /// Return the number of accepted active forward-index contracts.
    #[must_use]
    pub(in crate::db) const fn len(&self) -> usize {
        self.indexes.len()
    }

    /// Resolve one accepted active physical domain by persisted catalog order.
    pub(in crate::db) fn domain(
        &self,
        ordinal: usize,
        entity_tag: EntityTag,
    ) -> Result<AcceptedIndexInspectionDomain, InternalError> {
        let entry = self
            .indexes
            .get(ordinal)
            .ok_or_else(InternalError::store_invariant)?;
        let (schema_index_id, store_path, physical_index_id, key_component_count, unique) =
            match entry {
                AcceptedIndexInspectionEntry::FieldPath {
                    schema_index_id,
                    info,
                    ..
                } => (
                    *schema_index_id,
                    info.store(),
                    IndexId::new_with_generation(
                        entity_tag,
                        info.ordinal(),
                        info.physical_generation(),
                    ),
                    info.fields().len(),
                    info.unique(),
                ),
                AcceptedIndexInspectionEntry::Expression {
                    schema_index_id,
                    info,
                    ..
                } => (
                    *schema_index_id,
                    info.store(),
                    IndexId::new_with_generation(
                        entity_tag,
                        info.ordinal(),
                        info.physical_generation(),
                    ),
                    info.key_items().len(),
                    info.unique(),
                ),
            };

        Ok(AcceptedIndexInspectionDomain {
            schema_index_id,
            store_path: store_path.to_string(),
            physical_index_id,
            key_component_count,
            unique,
        })
    }

    /// Derive one exact optional witness for a decoded accepted row.
    pub(in crate::db) fn project(
        &self,
        ordinal: usize,
        entity_tag: EntityTag,
        primary_key: &PrimaryKeyValue,
        row: &dyn CanonicalSlotReader,
    ) -> Result<Option<AcceptedIndexInspectionWitness>, InternalError> {
        let entry = self
            .indexes
            .get(ordinal)
            .ok_or_else(InternalError::store_invariant)?;
        let (schema_index_id, store_path, key) = match entry {
            AcceptedIndexInspectionEntry::FieldPath {
                schema_index_id,
                info,
                predicate,
            } => (
                *schema_index_id,
                info.store(),
                accepted_field_path_index_key_for_slot_reader_with_membership_structural(
                    entity_tag,
                    info,
                    predicate.as_ref(),
                    primary_key,
                    row,
                )?,
            ),
            AcceptedIndexInspectionEntry::Expression {
                schema_index_id,
                info,
                predicate,
            } => (
                *schema_index_id,
                info.store(),
                accepted_expression_index_key_for_slot_reader_with_membership_structural(
                    entity_tag,
                    info,
                    predicate.as_ref(),
                    primary_key,
                    row,
                )?,
            ),
        };

        let raw_key = key
            .as_ref()
            .map(IndexKey::to_raw)
            .transpose()
            .map_err(InternalError::from)?;
        Ok(raw_key.map(|raw_key| AcceptedIndexInspectionWitness {
            schema_index_id,
            store_path: store_path.to_string(),
            raw_key,
        }))
    }
}

#[derive(Clone, Debug)]
enum AcceptedIndexInspectionEntry {
    FieldPath {
        schema_index_id: SchemaIndexId,
        info: SchemaIndexInfo,
        predicate: Option<PredicateProgram>,
    },
    Expression {
        schema_index_id: SchemaIndexId,
        info: SchemaExpressionIndexInfo,
        predicate: Option<PredicateProgram>,
    },
}

fn compile_predicate(
    sql: Option<&str>,
    row_contract: &StructuralRowContract,
) -> Result<Option<PredicateProgram>, InternalError> {
    sql.map(|sql| {
        let predicate = parse_sql_predicate(sql).map_err(|_| InternalError::store_corruption())?;
        Ok(PredicateProgram::compile_with_row_contract(
            row_contract,
            &normalize(&predicate),
        ))
    })
    .transpose()
}

fn validate_projection_identity(
    ordinal: u16,
    physical_generation: u64,
    name: &str,
    store: &str,
    unique: bool,
    accepted: &crate::db::schema::PersistedIndexSnapshot,
) -> Result<(), InternalError> {
    if ordinal != accepted.ordinal()
        || physical_generation != accepted.physical_generation()
        || name != accepted.name()
        || store != accepted.store()
        || unique != accepted.unique()
    {
        return Err(InternalError::store_corruption());
    }

    Ok(())
}
