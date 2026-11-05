//! The ICRC default account has one key across writes, text and typed SQL.

use super::*;
use crate::types::{Account, Principal, Subaccount};
use icydb_diagnostic_code::{DiagnosticDetail, RuntimeBoundaryCode};

#[test]
fn default_account_identity_preserves_unique_index_refusal_and_sql_lookup() {
    let account_index = PersistedIndexSnapshot::new(
        SchemaIndexId::new(1).unwrap(),
        1,
        "account_unique".into(),
        STORE_PATH.into(),
        true,
        PersistedIndexKeySnapshot::from_field_paths(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["account".into()],
            AcceptedFieldKind::Account,
            false,
        )]),
        None,
    );
    initialize_field_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            field(2, "account", 1, AcceptedFieldKind::Account),
        ],
        vec![account_index],
    );
    let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
    let owner = Principal::anonymous();
    let absent = Account::from(owner);
    let explicit_zero = Account::from_owner_and_subaccount(owner, Some(Subaccount::MIN));
    let nonzero = Account::from_owner_and_subaccount(owner, Some(Subaccount::MAX));
    let insert = |id, account| {
        session.execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![DynamicStructuralPatch::new(vec![
                ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                (
                    "account".into(),
                    DynamicWriteCell::Value(InputValue::account(account)),
                ),
            ])],
        )
    };
    insert(1, absent).unwrap();
    insert(4, nonzero).unwrap();
    for (id, duplicate) in [(2, absent), (3, explicit_zero)] {
        let error = insert(id, duplicate).unwrap_err();
        assert_eq!(
            error.diagnostic().detail(),
            Some(&DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ConstraintViolation
            })
        );
        assert_eq!(DATA_STORE.with_borrow(DataStore::len), 2);
        assert_eq!(INDEX_STORE.with_borrow(IndexStore::len).unwrap(), 2);
    }
    // Extended scalar SQL inputs use typed bindings. Parse the displayed text
    // through the Account owner before binding, preserving the maintained lane.
    for _ in 0..2 {
        for (account, id) in [(absent, 1), (explicit_zero, 1), (nonzero, 4)] {
            let parsed = account.to_string().parse::<Account>().unwrap();
            for condition in ["account = ?", "account = ? AND id + 0 = id"] {
                let sql = format!("SELECT id FROM PlannerRow WHERE {condition} ORDER BY id");
                let dispatch = crate::db::sql_statement_dispatch(&sql).unwrap();
                let (result, _) = session
                    .execute_trusted_sql_query_with_entity_name(
                        &dispatch,
                        &[InputValue::account(parsed)],
                    )
                    .unwrap();
                let SqlStatementResult::Projection { rows, .. } = result else {
                    panic!("projection")
                };
                assert_eq!(rows, vec![vec![OutputValue::nat64(id)]]);
            }
        }
    }
}
