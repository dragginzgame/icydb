use icydb::{
    db::{
        DbSession, DynamicQuery, StructuralMutation, StructuralPatch, TypedWrite, WriteCell,
    },
    traits::{CanisterKind, EntityFor},
    value::InputValue,
};

fn trusted_sql_query_compiles<C>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
{
    let _ = db.execute_trusted_sql_query(sql);
}

fn trusted_dynamic_query_compiles<C>(db: &DbSession<C>)
where
    C: CanisterKind,
{
    let request = DynamicQuery::new("app::User")
        .select(["name", "age"])
        .limit(25);
    let _ = db.execute_trusted_dynamic_query(&request);
}

fn trusted_sql_mutation_compiles<C, E>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
    E: EntityFor<C>,
{
    let _ = db.execute_trusted_sql_mutation(sql);
}

fn trusted_structural_mutation_compiles<C>(db: &DbSession<C>)
where
    C: CanisterKind,
{
    let patch = StructuralPatch::new()
        .field("name", WriteCell::Value(InputValue::Text("Ada".to_string())))
        .field("nickname", WriteCell::Null)
        .field("status", WriteCell::Default)
        .field("unchanged", WriteCell::Omitted);
    let _ = db.execute_trusted_structural_mutation(StructuralMutation::Insert {
        entity: "app::User".to_string(),
        patch,
    });
    if let Ok(binding) = db.bind_typed_entity(
        "app:entity:user",
        ["app:field:user-id", "app:field:user-name"],
    ) {
        let _ = TypedWrite::insert(
            &binding,
            [(
                "app:field:user-name",
                WriteCell::Value(InputValue::Text("Ada".to_string())),
            )],
        );
    }
}

fn trusted_sql_update_contracts_compile<C, E>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
    E: EntityFor<C>,
{
    let _ = db.execute_trusted_sql_exact_update(sql, 10);
    let _ = db.execute_trusted_sql_prefix_update(sql);
    let operation_id = icydb::types::Ulid::MIN;
    if let Ok(continuation) = db.prepare_trusted_sql_resumable_update(operation_id, sql) {
        let _ = icydb::db::TrustedResumableUpdateContinuation::try_from_bytes(
            continuation.as_bytes().to_vec(),
        );
        let _ = db.resume_trusted_sql_resumable_update(operation_id, sql, &continuation);
    }
}

fn admin_sql_ddl_compiles<C, E>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
    E: EntityFor<C>,
{
    let _ = db.execute_admin_sql_ddl::<E>(sql);
}

fn admin_integrity_sql_compiles<C>(db: &DbSession<C>, sql: &str)
where
    C: CanisterKind,
{
    let owner =
        icydb::db::IntegrityJobOwner::new("compile-test").expect("static owner should admit");
    let _ = db.execute_admin_integrity_sql(sql, owner);
}

fn main() {}
