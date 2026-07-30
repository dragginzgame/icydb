//!
//! One-entity typed-query canister used for wasm-footprint auditing.
//!

use icydb::db::query::FieldRef;
use icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleEntity01;

icydb::start!();

#[ic_cdk::query]
fn query_one_entity_typed_rows() -> Result<u32, String> {
    let rows = db()
        .map_err(|error| error.to_string())?
        .query::<OneSimpleEntity01>()
        .map_err(|error| error.to_string())?
        .filter(FieldRef::new("id").eq(icydb::types::Ulid::MIN))
        .limit(1)
        .execute_rows()
        .map_err(|error| error.to_string())?;

    u32::try_from(rows.len()).map_err(|_| "typed query row count exceeds u32".to_string())
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();

#[cfg(test)]
mod tests {
    use crate::db;
    use icydb::{
        db::{StructuralPatch, WriteCell},
        value::InputValue,
    };

    fn insert_one_native_row(name: &str) {
        let patch = StructuralPatch::new()
            .field("name", WriteCell::Value(InputValue::Text(name.to_string())));
        db().expect("native database should initialize")
            .execute_trusted_structural_insert_batch("OneSimpleEntity01", vec![patch])
            .expect("native insert should succeed");
    }

    #[test]
    fn first_libtest_thread_initializes_its_native_database() {
        insert_one_native_row("first");
    }

    #[test]
    fn second_libtest_thread_initializes_its_native_database() {
        insert_one_native_row("second");
    }
}
