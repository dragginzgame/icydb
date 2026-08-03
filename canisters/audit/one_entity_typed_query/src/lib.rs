//!
//! One-entity typed-query canister used for wasm-footprint auditing.
//!

use icydb::db::query::FieldRef;
use icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleEntity01;

icydb::start!();

#[ic_cdk::query]
fn query_one_entity_typed_rows() -> u32 {
    let Ok(database) = db() else {
        return 0;
    };
    let Ok(query) = database.query::<OneSimpleEntity01>() else {
        return 0;
    };
    let Ok(rows) = query
        .filter(FieldRef::new("id").eq(icydb::types::Ulid::MIN))
        .execute_rows()
    else {
        return 0;
    };

    u32::try_from(rows.len()).unwrap_or(u32::MAX)
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();

#[cfg(test)]
mod tests {
    use crate::db;
    use icydb::{
        db::{
            StructuralPatch, WriteCell,
            query::{FieldRef, TypedQueryError, count},
        },
        diagnostic::{DiagnosticCode, DiagnosticDetail, ErrorOrigin, QueryReadAdmissionCode},
        types::Ulid,
        value::{InputValue, OutputValue},
    };
    use icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleEntity01;

    fn insert_one_native_row(name: &str) -> Ulid {
        let patch = StructuralPatch::new()
            .field("name", WriteCell::Value(InputValue::Text(name.to_string())));
        let result = db()
            .expect("native database should initialize")
            .execute_trusted_structural_insert_batch("OneSimpleEntity01", vec![patch])
            .expect("native insert should succeed");
        let id_slot = result
            .columns
            .iter()
            .position(|column| column == "id")
            .expect("insert result should include the accepted identity field");
        match result.rows.first().and_then(|row| row.get(id_slot)) {
            Some(OutputValue::Ulid(id)) => *id,
            _ => panic!("generated identity should be returned as an Ulid"),
        }
    }

    #[test]
    fn first_libtest_thread_initializes_its_native_database() {
        insert_one_native_row("first");
    }

    #[test]
    fn second_libtest_thread_initializes_its_native_database() {
        insert_one_native_row("second");
    }

    #[test]
    fn generated_typed_grouped_terminal_executes_without_sql() {
        let id = insert_one_native_row("grouped");
        let grouped = db()
            .expect("native database should initialize")
            .query::<OneSimpleEntity01>()
            .expect("generated typed adapter should bind")
            .filter(FieldRef::new("id").eq(id))
            .group_by("name")
            .aggregate(count())
            .grouped_limits(1, 1024)
            .limit(1)
            .execute_grouped()
            .expect("generated typed grouped query should execute");

        assert_eq!(grouped.row_count, 1);
        assert_eq!(
            grouped.rows[0].group_key(),
            &[OutputValue::Text("grouped".to_string())]
        );
        assert_eq!(grouped.rows[0].aggregate_values(), &[OutputValue::Nat64(1)]);
        assert_eq!(grouped.next_cursor, None);

        let error = db()
            .expect("native database should initialize")
            .query::<OneSimpleEntity01>()
            .expect("generated typed adapter should bind")
            .group_by("name")
            .aggregate(count())
            .execute_grouped()
            .expect_err("generated typed grouped query must require explicit limits");
        let TypedQueryError::Database(error) = error else {
            panic!("grouped limit rejection should cross the typed database boundary");
        };
        let diagnostic = error.diagnostic();
        assert_eq!(diagnostic.code(), DiagnosticCode::QueryReadAdmission);
        assert_eq!(diagnostic.origin(), ErrorOrigin::Query);
        assert_eq!(
            diagnostic.detail(),
            Some(&DiagnosticDetail::QueryReadAdmission {
                reason: QueryReadAdmissionCode::GroupedQueryRequiresLimits,
            })
        );
    }
}
