use super::*;
use crate::db::{
    schema::{
        AcceptedCompositeCatalog, AcceptedConstraintCatalog, AcceptedFieldKind,
        AcceptedSchemaFingerprint, AcceptedSchemaRevision, AcceptedValueCatalogHandle, FieldId,
        FieldStorageDecode, LeafCodec, PersistedFieldSnapshot, PersistedSchemaSnapshot,
        ScalarCodec, SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
        empty_accepted_enum_catalog_for_tests,
    },
    sql::parser::{SqlDdlStatement, SqlStatement, parse_sql},
};

fn nullable_email_snapshot() -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::Account".to_string(),
        "Account".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial_test_field(
                1,
                "id",
                0,
                AcceptedFieldKind::Ulid,
                false,
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "email".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                true,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
    )
}

fn schema_view(snapshot: PersistedSchemaSnapshot) -> (AcceptedSchemaSnapshot, SchemaInfo) {
    let accepted =
        AcceptedSchemaSnapshot::try_new(snapshot).expect("fixture accepted schema should validate");
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::new(1),
    );
    let schema = SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog);
    (accepted, schema)
}

fn nullable_email_schema() -> (AcceptedSchemaSnapshot, SchemaInfo) {
    schema_view(nullable_email_snapshot())
}

fn create_index_statement(sql: &str) -> SqlCreateIndexStatement {
    let SqlStatement::Ddl(SqlDdlStatement::CreateIndex(statement)) =
        parse_sql(sql).expect("fixture SQL should parse")
    else {
        panic!("fixture SQL should be CREATE INDEX");
    };
    statement
}

fn drop_index_statement(sql: &str) -> SqlDropIndexStatement {
    let SqlStatement::Ddl(SqlDdlStatement::DropIndex(statement)) =
        parse_sql(sql).expect("fixture SQL should parse")
    else {
        panic!("fixture SQL should be DROP INDEX");
    };
    statement
}

fn nullable_unique_index_states() -> (
    (AcceptedSchemaSnapshot, SchemaInfo),
    (AcceptedSchemaSnapshot, SchemaInfo),
) {
    let (accepted, schema) = nullable_email_schema();
    let create = create_index_statement(
        "CREATE UNIQUE INDEX account_email ON Account (email) WHERE email IS NOT NULL",
    );
    let candidate = match bind_create_index_statement(
        &create,
        &accepted,
        &schema,
        "entities::Account::account_email",
        &crate::db::resource::budget::MaintenanceConstructionBudget::new(),
    )
    .expect("guarded nullable unique index should bind")
    .statement()
    {
        BoundSqlDdlStatement::CreateIndex(create) => create.candidate_index().clone(),
        other => panic!("expected CREATE INDEX candidate, got {other:?}"),
    };
    let active_snapshot = PersistedSchemaSnapshot::new_with_indexes(
        accepted.persisted_snapshot().version(),
        accepted.persisted_snapshot().entity_path().to_string(),
        accepted.persisted_snapshot().entity_name().to_string(),
        accepted
            .persisted_snapshot()
            .primary_key_field_ids()
            .to_vec(),
        accepted.persisted_snapshot().row_layout().clone(),
        accepted.persisted_snapshot().fields().to_vec(),
        vec![candidate.clone()],
    );
    let active_catalog = AcceptedConstraintCatalog::initial(
        active_snapshot.fields(),
        active_snapshot.indexes(),
        active_snapshot.relations(),
    )
    .expect("active constraint catalog should build");
    let active = schema_view(active_snapshot.with_constraint_catalog(active_catalog));
    let pending_candidate =
        candidate.clone_with_schema_identity(candidate.schema_id(), candidate.ordinal(), 9);
    let pending_snapshot = nullable_email_snapshot()
        .with_added_unique_activation(
            pending_candidate,
            AcceptedSchemaFingerprint::new([9; 32]),
            9,
        )
        .expect("exact guarded candidate should admit");

    (active, schema_view(pending_snapshot))
}

#[test]
fn authored_names_cannot_claim_engine_constraint_namespace() {
    let (accepted, schema) = nullable_email_schema();
    for name in [
        "__icydb_primary_key",
        "__icydb_not_null_2",
        "__icydb_custom",
    ] {
        for unique in ["", "UNIQUE "] {
            let statement = create_index_statement(&format!(
                "CREATE {unique}INDEX {name} ON Account (email) WHERE email IS NOT NULL"
            ));
            assert!(matches!(
                bind_create_index_statement(&statement, &accepted, &schema, "index-store", &crate::db::resource::budget::MaintenanceConstructionBudget::new()),
                Err(SqlDdlBindError::InvalidConstraintName { constraint_name }) if constraint_name == name
            ));
        }
        let SqlStatement::Ddl(SqlDdlStatement::AlterTableAddCheckConstraint(statement)) =
            parse_sql(&format!(
                "ALTER TABLE Account ADD CONSTRAINT {name} CHECK (email IS NOT NULL) NOT VALID"
            ))
            .unwrap()
        else {
            panic!("expected check statement")
        };
        assert!(matches!(
            crate::db::sql::ddl::constraint::bind_alter_table_add_check_constraint_statement(
                &statement, &accepted, &schema,
            ),
            Err(SqlDdlBindError::InvalidConstraintName { constraint_name }) if constraint_name == name
        ));
    }
    for name in ["__icydb", "_icydb_user", "__ICYDB_user", "user__icydb_name"] {
        let statement = create_index_statement(&format!("CREATE INDEX {name} ON Account (email)"));
        bind_create_index_statement(
            &statement,
            &accepted,
            &schema,
            "index-store",
            &crate::db::resource::budget::MaintenanceConstructionBudget::new(),
        )
        .unwrap();
    }
}

#[test]
fn sql_unique_index_binding_projects_shared_nullable_contract() {
    let (accepted, schema) = nullable_email_schema();
    let invalid = create_index_statement(
        "CREATE UNIQUE INDEX account_email ON Account (email) WHERE id IS NOT NULL",
    );
    let error = bind_create_index_statement(
        &invalid,
        &accepted,
        &schema,
        "entities::Account::account_email",
        &crate::db::resource::budget::MaintenanceConstructionBudget::new(),
    )
    .expect_err("unmatched nullable guard must reject during binding");
    assert!(matches!(
        error,
        SqlDdlBindError::NullableUniqueIndexContract(
            crate::db::schema::NullableUniqueIndexContractError::MissingGuards {
                sources,
                ..
            }
        ) if sources == vec![vec!["email".to_string()]]
    ));

    let valid = create_index_statement(
        "CREATE UNIQUE INDEX account_email ON Account (email) WHERE email IS NOT NULL",
    );
    let bound = bind_create_index_statement(
        &valid,
        &accepted,
        &schema,
        "entities::Account::account_email",
        &crate::db::resource::budget::MaintenanceConstructionBudget::new(),
    )
    .expect("exact nullable guard should bind");
    assert!(matches!(
        bound.statement(),
        BoundSqlDdlStatement::CreateIndex(create)
            if create.candidate_index().predicate() == Some(&crate::db::schema::AcceptedIndexPredicate::test_non_null(2))
    ));
}

#[test]
fn required_create_and_optional_drop_entity_bindings_stay_distinct() {
    let (accepted, schema) = nullable_email_schema();
    let create = create_index_statement("CREATE INDEX account_email ON Other (email)");
    assert!(matches!(
        bind_create_index_statement(
            &create,
            &accepted,
            &schema,
            "entities::Account::account_email", &crate::db::resource::budget::MaintenanceConstructionBudget::new()),
        Err(SqlDdlBindError::EntityMismatch { sql_entity, expected_entity })
            if sql_entity == "Other" && expected_entity == "Account"
    ));

    let targetless = drop_index_statement("DROP INDEX IF EXISTS missing");
    assert!(matches!(
        bind_drop_index_statement(&targetless, &accepted, &schema)
            .expect("targetless DROP INDEX IF EXISTS should use the selected catalog")
            .statement(),
        BoundSqlDdlStatement::NoOp(no_op) if no_op.entity_name == "Account"
    ));

    let mismatched = drop_index_statement("DROP INDEX IF EXISTS missing ON Other");
    assert!(matches!(
        bind_drop_index_statement(&mismatched, &accepted, &schema),
        Err(SqlDdlBindError::EntityMismatch { sql_entity, expected_entity })
            if sql_entity == "Other" && expected_entity == "Account"
    ));
}

#[test]
fn filtered_index_normalization_shares_one_allowance_and_preserves_budget_facts() {
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag as Fact,
    };

    use crate::db::{
        resource::budget::MaintenanceConstructionBudget, sql::ddl::SqlDdlPrepareError,
    };

    let (accepted, schema) = nullable_email_schema();
    let sql = "(email != 'a' AND email != 'b' AND email != 'c') OR \
               (email != 'd' AND email != 'e' AND email != 'f')";
    let expected = crate::db::schema::AcceptedIndexPredicate::bind_test_sql(
        sql,
        accepted.persisted_snapshot().fields(),
    );
    // Both conjunctions and their enclosing sort share one statement's
    // complete allowance, including key encoding across disjunction branches.
    let baseline = MaintenanceConstructionBudget::with_limit_for_tests(
        Resource::PredicateExpressionSteps,
        u64::MAX,
    );
    let canonical = baseline
        .run(
            |work| {
                bind_create_index_predicate(Some(sql), &accepted, &schema, work)
                    .map_err(SqlDdlPrepareError::from)
            },
            SqlDdlPrepareError::Construction,
        )
        .unwrap();
    assert_eq!(canonical.as_ref(), Some(&expected));
    let steps = baseline.observed_for_tests(Resource::PredicateExpressionSteps);
    assert!(steps > 6, "sort work supplements the six conjunction pairs");
    for limit in [steps - 1, steps, steps + 1] {
        let work = MaintenanceConstructionBudget::with_limit_for_tests(
            Resource::PredicateExpressionSteps,
            limit,
        );
        let result = work.run(
            |work| {
                bind_create_index_predicate(Some(sql), &accepted, &schema, work)
                    .map_err(SqlDdlPrepareError::from)
            },
            SqlDdlPrepareError::Construction,
        );
        if limit < steps {
            let error = crate::db::QueryError::from_sql_ddl_prepare_error(result.unwrap_err());
            for fact in [
                (
                    Fact::BudgetResource,
                    Resource::PredicateExpressionSteps.raw(),
                ),
                (Fact::Limit, limit),
                (Fact::Actual, steps),
            ] {
                assert!(error.diagnostic_facts().contains(&fact));
            }
        } else {
            assert_eq!(result.unwrap().as_ref(), Some(&expected));
        }
    }
}

#[test]
fn sql_index_binding_rejects_unbound_eliminated_branches() {
    let (accepted, schema) = nullable_email_schema();
    for sql in [
        "CREATE UNIQUE INDEX account_email ON Account (email) WHERE email IS NOT NULL AND (email = 'active' OR (email = 'a' AND email = 'b' AND missing IS NOT NULL))",
        "CREATE INDEX account_email ON Account (email) WHERE email = 'a' AND email = 'b' AND missing IS NOT NULL",
        "CREATE UNIQUE INDEX account_email ON Account (id) WHERE missing = email",
    ] {
        let invalid = create_index_statement(sql);
        let error = bind_create_index_statement(
            &invalid,
            &accepted,
            &schema,
            "entities::Account::account_email",
            &crate::db::resource::budget::MaintenanceConstructionBudget::new(),
        )
        .unwrap_err();
        assert!(
            matches!(error, SqlDdlBindError::InvalidFilteredIndexPredicate),
            "{sql}: {error:?}"
        );
    }
}

#[test]
fn sql_unique_index_if_not_exists_classifies_active_candidate_and_conflicts() {
    let ((active, active_schema), (pending, pending_schema)) = nullable_unique_index_states();
    let exact = create_index_statement(
        "CREATE UNIQUE INDEX IF NOT EXISTS account_email ON Account (email) WHERE email IS NOT NULL",
    );
    assert!(matches!(
        bind_create_index_statement(
            &exact,
            &active,
            &active_schema,
            "entities::Account::account_email",
            &crate::db::resource::budget::MaintenanceConstructionBudget::new()
        )
        .expect("exact active IF NOT EXISTS should bind")
        .statement(),
        BoundSqlDdlStatement::NoOp(_)
    ));

    assert!(matches!(
        bind_create_index_statement(
            &exact,
            &pending,
            &pending_schema,
            "entities::Account::account_email",
            &crate::db::resource::budget::MaintenanceConstructionBudget::new()
        )
        .expect("exact candidate IF NOT EXISTS should bind")
        .statement(),
        BoundSqlDdlStatement::NoOp(_)
    ));

    for sql in [
        "CREATE UNIQUE INDEX IF NOT EXISTS account_email ON Account(email) WHERE (email IS NOT NULL)",
        "CREATE UNIQUE INDEX IF NOT EXISTS account_email ON Account (email) WHERE email is not null AND email IS NOT NULL",
    ] {
        let equivalent = create_index_statement(sql);
        for (accepted, schema) in [(&active, &active_schema), (&pending, &pending_schema)] {
            assert!(matches!(
                bind_create_index_statement(
                    &equivalent,
                    accepted,
                    schema,
                    "entities::Account::account_email",
                    &crate::db::resource::budget::MaintenanceConstructionBudget::new()
                )
                .unwrap()
                .statement(),
                BoundSqlDdlStatement::NoOp(_)
            ));
        }
    }

    let same_name_conflict = create_index_statement(
        "CREATE UNIQUE INDEX IF NOT EXISTS account_email ON Account (email) WHERE email IS NOT NULL AND id IS NOT NULL",
    );
    assert!(matches!(
        bind_create_index_statement(
            &same_name_conflict,
            &active,
            &active_schema,
            "entities::Account::account_email", &crate::db::resource::budget::MaintenanceConstructionBudget::new()),
        Err(SqlDdlBindError::DuplicateIndexName { index_name })
            if index_name == "account_email"
    ));
    assert!(matches!(
        bind_create_index_statement(
            &same_name_conflict,
            &pending,
            &pending_schema,
            "entities::Account::account_email", &crate::db::resource::budget::MaintenanceConstructionBudget::new()),
        Err(SqlDdlBindError::DuplicateIndexName { index_name })
            if index_name == "account_email"
    ));

    let new_name_same_contract = create_index_statement(
        "CREATE UNIQUE INDEX IF NOT EXISTS account_email_copy ON Account (email) WHERE email IS NOT NULL",
    );
    assert!(matches!(
        bind_create_index_statement(
            &new_name_same_contract,
            &active,
            &active_schema,
            "entities::Account::account_email_copy", &crate::db::resource::budget::MaintenanceConstructionBudget::new()),
        Err(SqlDdlBindError::DuplicateFieldPathIndex { existing_index, .. })
            if existing_index == "account_email"
    ));
    assert!(matches!(
        bind_create_index_statement(
            &new_name_same_contract,
            &pending,
            &pending_schema,
            "entities::Account::account_email_copy", &crate::db::resource::budget::MaintenanceConstructionBudget::new()),
        Err(SqlDdlBindError::DuplicateFieldPathIndex { existing_index, .. })
            if existing_index == "account_email"
    ));
}
