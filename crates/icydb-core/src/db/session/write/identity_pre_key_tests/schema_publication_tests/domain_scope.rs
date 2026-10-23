//! SQL index replacement observes only its entity's rows and physical indexes.

use super::*;
use crate::db::data::RawRow;

fn assert_entity_scoped_ddl<C: CanisterKind>(session: &DbSession<C>, store_path: &'static str) {
    let store = session.db.store_handle(store_path).unwrap();
    // A key with a foreign entity prefix is outside this DDL's authority,
    // even when decoding that other domain would fail. Keep its bytes intact.
    let foreign = RawDataStoreKey::from_persisted_bytes(u64::MAX.to_be_bytes().to_vec());
    assert!(DecodedDataStoreKey::try_from_raw(&foreign).is_err());
    let raw_row = RawRow::try_new(vec![0xff]).unwrap();
    store.with_data_mut(|data| data.insert_raw_for_test(foreign.clone(), raw_row.clone()));
    let before = rows(store);
    for (version, sql) in [
        (
            1,
            "CREATE INDEX scoped_payload ON IdentityRow (id, payload)",
        ),
        (2, "DROP INDEX scoped_payload ON IdentityRow"),
    ] {
        session
            .execute_admin_sql_ddl(&format!(
                "{sql} EXPECT SCHEMA VERSION {version} SET SCHEMA VERSION {}",
                version + 1
            ))
            .unwrap();
        assert_eq!(rows(store), before);
        assert_eq!(
            store.with_data(|data| data.get(&foreign)),
            Some(raw_row.clone())
        );
    }

    // The same malformed shape inside the selected entity still refuses
    // staging before publication or any row/index mutation.
    let malformed =
        RawDataStoreKey::from_persisted_bytes(ENTITY_TAG.value().to_be_bytes().to_vec());
    store.with_data_mut(|data| data.insert_raw_for_test(malformed, raw_row));
    let before_rows = rows(store);
    let before_indexes = indexes(store, false);
    let before_schema =
        store.with_schema(|schema| schema.current_accepted_schema_bundle().unwrap().unwrap());
    let error = session.execute_admin_sql_ddl("CREATE INDEX refused_payload ON IdentityRow (id, payload) EXPECT SCHEMA VERSION 3 SET SCHEMA VERSION 4").unwrap_err();
    assert_eq!(
        error.diagnostic().code(),
        InternalError::store_corruption().diagnostic_code()
    );
    assert_eq!(rows(store), before_rows);
    assert_eq!(indexes(store, false), before_indexes);
    assert_eq!(
        store.with_schema(|schema| schema.current_accepted_schema_bundle().unwrap().unwrap()),
        before_schema
    );
}

#[test]
fn heap_index_ddl_is_entity_scoped_and_keeps_local_corruption_refusal() {
    let session = initialize();
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: ENTITY_NAME.into(),
            patch: dynamic_payload_patch(7),
        })
        .unwrap();
    assert_entity_scoped_ddl(&session, STORE_PATH);
}

#[test]
fn journaled_index_ddl_is_entity_scoped_and_keeps_local_corruption_refusal() {
    assert_entity_scoped_ddl(&populated_session(), JOURNALED_STORE_PATH);
}
