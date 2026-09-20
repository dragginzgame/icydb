//! Fixed controls for the populated entity-rename deployment rehearsal.

use crate::{typed_fixture_invariant_error, typed_operation_fixture_error};
use ic_cdk::{query, update};
use icydb::{
    db::{StructuralPatch, WriteCell, with_request_execution},
    traits::EntitySource,
    types::Id,
    value::InputValue,
};
#[cfg(feature = "entity-rename-successor")]
use icydb_testing_test_sql_fixtures::entity_rename::CatalogItem as RenameItem;
use icydb_testing_test_sql_fixtures::entity_rename::Holder;
#[cfg(not(feature = "entity-rename-successor"))]
use icydb_testing_test_sql_fixtures::entity_rename::Item as RenameItem;

/// Seed the three fixed rows through ordinary accepted structural writes.
#[update]
fn seed_entity_rename() -> Result<(), icydb::Error> {
    with_request_execution(|| {
        let session = icydb::db!()?;
        for (id, parent) in [(1, InputValue::null()), (2, InputValue::nat64(1))] {
            let patch = StructuralPatch::new()
                .field("id", WriteCell::Value(InputValue::nat64(id)))
                .field("key", WriteCell::Value(InputValue::nat64(100 + id)))
                .field("label", WriteCell::Value(InputValue::nat64(200 + id)))
                .field("parent_id", WriteCell::Value(parent));
            session.execute_trusted_structural_insert_batch(RenameItem::ENTITY, vec![patch])?;
        }
        let holder = StructuralPatch::new()
            .field("id", WriteCell::Value(InputValue::nat64(10)))
            .field("item_id", WriteCell::Value(InputValue::nat64(2)));
        session.execute_trusted_structural_insert_batch(Holder::ENTITY, vec![holder])?;
        Ok(())
    })
}

/// Exercise both reverse-relation delete restrictions without caller-owned SQL.
#[update]
fn check_entity_rename_deletes() -> Result<Vec<icydb::Error>, icydb::Error> {
    with_request_execution(|| {
        let session = icydb::db!()?;
        let mut errors = Vec::new();
        for id in [1, 2] {
            let sql = format!("DELETE FROM {} WHERE id = {id}", RenameItem::ENTITY);
            let Err(error) = session.execute_trusted_sql_mutation(&sql) else {
                return Err(typed_fixture_invariant_error());
            };
            errors.push(error);
        }
        Ok(errors)
    })
}

/// Resolve and decode both generated entities through accepted bindings.
#[query]
fn check_entity_rename_bindings() -> Result<(), icydb::Error> {
    with_request_execution(|| {
        let session = icydb::db!()?;
        // Exact keys obey ordinary typed-read admission without a full scan.
        for expected in [(1, 101, 201, None), (2, 102, 202, Some(1))] {
            let row = session
                .get::<RenameItem>(Id::from_key(expected.0))
                .map_err(typed_operation_fixture_error)?
                .ok_or_else(typed_fixture_invariant_error)?;
            if (row.id, row.key, row.label, row.parent_id) != expected {
                return Err(typed_fixture_invariant_error());
            }
        }
        let holder = session
            .get::<Holder>(Id::from_key(10))
            .map_err(typed_operation_fixture_error)?
            .ok_or_else(typed_fixture_invariant_error)?;
        if (holder.id, holder.item_id) != (10, 2) {
            return Err(typed_fixture_invariant_error());
        }
        Ok(())
    })
}
