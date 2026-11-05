//! Compile actual actor output with caller-defined standard names and macros.

// No endpoint adapter is needed to compile private actor wiring. Most generated
// helpers are deliberately unused by this native compile contract.
#[expect(
    dead_code,
    reason = "qualify private actor wiring without an endpoint host"
)]
mod generated {
    use std::{
        cmp::Ordering::{Equal as None, Less as Ok},
        collections::VecDeque as Vec,
        ffi::OsString as String,
        io::Result,
        ops::ControlFlow::{Break as Err, Continue as Some},
    };

    const _: std::cmp::Ordering = None;
    const _: std::cmp::Ordering = Ok;
    const _: std::ops::ControlFlow<(), ()> = Err(());
    const _: std::ops::ControlFlow<(), ()> = Some(());
    const _: Result<()> = std::result::Result::Ok(());
    const _: std::option::Option<Vec<String>> = std::option::Option::None;

    macro_rules! thread_local {
        ($($input:tt)*) => {
            const _: () = ();
        };
    }
    macro_rules! vec {
        ($($input:tt)*) => {
            0_u8
        };
    }
    macro_rules! concat {
        ($($input:tt)*) => {
            0_u8
        };
    }
    thread_local!();
    const _: u8 = vec![];
    const _: u8 = concat!();

    ::std::include!(::std::concat!(::std::env!("OUT_DIR"), "/actor.rs"));
}

pub(super) fn icydb_storage_grants()
-> std::result::Result<runtime_api::db::AllocationGrants, runtime_api::db::AdmissionError> {
    runtime_api::db::AllocationGrants::new(vec![runtime_api::db::MemoryAuthority::new(
        "icydb_actor_hygiene",
        "icydb.actor_hygiene.",
    )?])
}

#[test]
fn actor_requests_keep_the_declared_memory_authority() {
    let requests = generated::icydb_memory_requests().expect("generated requests should validate");
    assert_eq!(requests.len(), 7);
    assert!(requests.iter().all(|request| {
        request.authority() == "icydb_actor_hygiene"
            && request
                .stable_key()
                .as_str()
                .starts_with("icydb.actor_hygiene.")
    }));
    let _ = icydb_storage_grants().expect("fixture authority should validate");
}

#[test]
fn whole_rows_use_checked_writes_and_preserve_assigned_identity() {
    use crate::actor_schema::{
        ActorHygieneEntity as Row, ActorHygieneEntityInsert as Insert,
        ActorHygieneEntityPatch as Patch, ActorHygieneEntityReplace as Replace,
    };
    use runtime_api::db::{TypedOperationError, TypedRowAdapter, TypedWriteAdapter, WriteCell};

    generated::__drive_native_database_for_tests().unwrap();
    runtime_api::db::with_request_execution(|| {
        let session = generated::db().unwrap();
        let binding = Row::typed_binding(&session).unwrap();
        let invalid = Insert::from(Row {
            id: 999,
            label: "x".repeat(33),
            nickname: None,
        })
        .encode_write(&binding)
        .map_err(TypedOperationError::Adapter)
        .and_then(|write| session.execute_trusted_typed_write(write));
        assert!(invalid.is_err());
        let input = Insert::from(Row {
            id: 999,
            label: "authored".into(),
            nickname: None,
        });
        assert!(matches!(input.nickname, WriteCell::Null));
        let output = session
            .execute_trusted_typed_write_row(input.encode_write(&binding).unwrap())
            .unwrap();
        let row = Row::decode_row(&binding, output).unwrap();
        assert_eq!(row.id, 1);
        assert_eq!(row.nickname, None);
        let key = row.clone().into_typed_key();

        let mut update = Patch::from(Row {
            label: "updated".into(),
            nickname: Some("nick".into()),
            ..row
        });
        assert_eq!(update.id, key);
        update.label = WriteCell::Omitted;
        session
            .execute_trusted_typed_write(update.encode_write(&binding).unwrap())
            .unwrap();
        let row = session.get::<Row>(key).unwrap().unwrap();
        assert_eq!(row.label, "authored");
        assert_eq!(row.nickname.as_deref(), Some("nick"));

        let replacement = Replace::from(Row {
            nickname: None,
            ..row
        });
        assert_eq!(replacement.id, key);
        session
            .execute_trusted_typed_write(replacement.encode_write(&binding).unwrap())
            .unwrap();
        assert_eq!(session.get::<Row>(key).unwrap().unwrap().nickname, None);
        let delete = runtime_api::db::TypedWrite::delete(&binding, key.into());
        session.execute_trusted_typed_write(delete).unwrap();
        assert!(session.get::<Row>(key).unwrap().is_none());
    });
}

#[test]
fn whole_row_conversions_keep_composite_keys() {
    use crate::actor_schema::{
        CompositeRow as Row, CompositeRowInsert as Insert, CompositeRowPatch as Patch,
    };
    use runtime_api::db::TypedWriteAdapter;

    generated::__drive_native_database_for_tests().unwrap();
    runtime_api::db::with_request_execution(|| {
        let session = generated::db().unwrap();
        let binding = Row::typed_binding(&session).unwrap();
        let row = Row {
            tenant: u64::MAX,
            local: u32::MAX,
            label: Some("first".into()),
        };
        let key = row.clone().into_typed_key();
        session
            .execute_trusted_typed_write(Insert::from(row).encode_write(&binding).unwrap())
            .unwrap();
        let mut row = session.get::<Row>(key).unwrap().unwrap();
        row.label = None;
        session
            .execute_trusted_typed_write(Patch::from(row).encode_write(&binding).unwrap())
            .unwrap();
        assert_eq!(session.get::<Row>(key).unwrap().unwrap().label, None);
        session
            .execute_trusted_typed_write(runtime_api::db::TypedWrite::delete(&binding, key.into()))
            .unwrap();
        assert!(session.get::<Row>(key).unwrap().is_none());
    });
}

#[test]
fn whole_row_writes_rebind_after_accepted_schema_changes() {
    use crate::actor_schema::{
        ActorHygieneEntity as Row, ActorHygieneEntityInsert as Insert,
        ActorHygieneEntityPatch as Patch,
    };
    use runtime_api::db::{
        TypedAdapterError, TypedOperationError, TypedRowAdapter, TypedWriteAdapter,
    };

    generated::__drive_native_database_for_tests().unwrap();
    runtime_api::db::with_request_execution(|| {
        let session = generated::db().unwrap();
        let binding = Row::typed_binding(&session).unwrap();
        let output = session
            .execute_trusted_typed_write_row(
                Insert::from(Row {
                    id: 0,
                    label: "before".into(),
                    nickname: None,
                })
                .encode_write(&binding)
                .unwrap(),
            )
            .unwrap();
        let row = Row::decode_row(&binding, output).unwrap();
        let key = row.clone().into_typed_key();
        // SQL owns this additional field; generated fields keep their declared
        // identities. A metadata-only rename must still expire issued writes.
        session
            .execute_admin_sql_ddl(
                "ALTER TABLE ActorHygieneEntity ADD COLUMN extra text \
                 EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2",
            )
            .unwrap();
        let binding = Row::typed_binding(&session).unwrap();
        let stale = Patch::from(row.clone()).encode_write(&binding).unwrap();
        session
            .execute_admin_sql_ddl(
                "ALTER TABLE ActorHygieneEntity RENAME COLUMN extra TO renamed \
                 EXPECT SCHEMA VERSION 2 SET SCHEMA VERSION 3",
            )
            .unwrap();
        assert!(matches!(
            session.execute_trusted_typed_write(stale),
            Err(TypedOperationError::Adapter(
                TypedAdapterError::StaleBinding
            ))
        ));
        let current = Row::typed_binding(&session).unwrap();
        session
            .execute_trusted_typed_write(
                Patch::from(Row {
                    label: "after".into(),
                    ..row
                })
                .encode_write(&current)
                .unwrap(),
            )
            .unwrap();
        assert_eq!(session.get::<Row>(key).unwrap().unwrap().label, "after");
    });
}
