use super::{prepare_bound_sql_statement, validate_sql_bindings};
use crate::{
    db::{
        QueryError,
        schema::{
            AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision,
            AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId, FieldStorageDecode,
            PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot, SchemaInfo,
            SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
            empty_accepted_enum_catalog_for_tests,
        },
        sql::parser::{SqlExpr, SqlStatement, parse_sql},
    },
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, IntBig, NatBig, Principal, Subaccount,
        Timestamp, U256, Ulid,
    },
    value::{InputValue, PublicValue, Value},
};
use icydb_diagnostic_code::SqlLoweringCode;

fn schema(kind: AcceptedFieldKind) -> SchemaInfo {
    let fields = [AcceptedFieldKind::Nat64, kind]
        .into_iter()
        .enumerate()
        .map(|(index, kind)| {
            let decode = FieldStorageDecode::ByKind;
            let codec = kind.leaf_codec_for_storage(decode);
            PersistedFieldSnapshot::new_initial(
                FieldId::new(u32::try_from(index).expect("two fields") + 1),
                ["id", "operand"][index].to_string(),
                SchemaFieldSlot::new(u16::try_from(index).expect("two fields")),
                kind,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                decode,
                codec,
            )
        })
        .collect::<Vec<_>>();
    let snapshot = AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::Bindings".to_string(),
        "Bindings".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ))
    .expect("accepted schema");
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    SchemaInfo::from_accepted_snapshot_and_catalog(&snapshot, catalog, true)
}

#[test]
fn typed_scalar_families_keep_runtime_values_and_reusable_syntax() {
    let cases = [
        (
            AcceptedFieldKind::Account,
            InputValue::account(Account::from_owner_and_subaccount(
                Principal::anonymous(),
                Some(Subaccount::from_array([7; 32])),
            )),
        ),
        (
            AcceptedFieldKind::Blob { max_len: None },
            InputValue::blob(vec![0, 1, 255]),
        ),
        (AcceptedFieldKind::Bool, InputValue::boolean(true)),
        (AcceptedFieldKind::Date, InputValue::date(Date::EPOCH)),
        (
            AcceptedFieldKind::Decimal { scale: 2 },
            InputValue::decimal(Decimal::from(2_u64)),
        ),
        (
            AcceptedFieldKind::Duration,
            InputValue::duration(Duration::from_millis(7)),
        ),
        (
            AcceptedFieldKind::Float32,
            InputValue::float32(Float32::default()),
        ),
        (
            AcceptedFieldKind::Float64,
            InputValue::float64(Float64::default()),
        ),
        (AcceptedFieldKind::Int64, InputValue::int64(-3)),
        (AcceptedFieldKind::Int128, InputValue::int128(i128::MAX)),
        (
            AcceptedFieldKind::IntBig { max_bytes: 64 },
            InputValue::int_big(
                "170141183460469231731687303715884105727"
                    .parse::<IntBig>()
                    .expect("wide integer"),
            ),
        ),
        (AcceptedFieldKind::Nat64, InputValue::nat64(3)),
        (AcceptedFieldKind::Nat128, InputValue::nat128(u128::MAX)),
        (
            AcceptedFieldKind::NatBig { max_bytes: 64 },
            InputValue::nat_big(
                "340282366920938463463374607431768211455"
                    .parse::<NatBig>()
                    .expect("wide natural"),
            ),
        ),
        (AcceptedFieldKind::U256, InputValue::u256(U256::MAX)),
        (
            AcceptedFieldKind::Principal,
            InputValue::principal(Principal::anonymous()),
        ),
        (
            AcceptedFieldKind::Subaccount,
            InputValue::subaccount(Subaccount::from_array([5; 32])),
        ),
        (
            AcceptedFieldKind::Text { max_len: None },
            InputValue::text("'?; -- payload".to_string()),
        ),
        (
            AcceptedFieldKind::Timestamp,
            InputValue::timestamp(Timestamp::from_millis(5)),
        ),
        (
            AcceptedFieldKind::Ulid,
            InputValue::ulid(Ulid::from_u128(7)),
        ),
        (AcceptedFieldKind::Unit, InputValue::unit()),
    ];
    let syntax = parse_sql("SELECT id FROM Bindings WHERE operand = ?").expect("syntax");
    let original = syntax.clone();
    for (kind, input) in cases {
        let schema = schema(kind);
        let expected = input.clone().try_into_runtime_non_enum().expect("scalar");
        let prepared = prepare_bound_sql_statement(&syntax, "Bindings", &schema, &[input])
            .expect("typed scalar admission");
        let SqlStatement::Select(select) = prepared.into_statement() else {
            panic!("select");
        };
        let Some(SqlExpr::Binary { right, .. }) = select.predicate else {
            panic!("comparison");
        };
        assert_eq!(*right, SqlExpr::Literal(expected));
        assert_eq!(syntax, original);
        assert!(
            prepare_bound_sql_statement(
                &syntax,
                "Bindings",
                &schema,
                &[InputValue::map(Vec::new())]
            )
            .is_err()
        );
    }
}

#[test]
fn binding_limits_and_placement_are_typed_and_payload_independent() {
    let syntax = parse_sql("SELECT id FROM Bindings WHERE operand = ?").expect("syntax");
    for (inputs, code) in [
        (vec![], SqlLoweringCode::BindingCount),
        (
            vec![InputValue::null(), InputValue::null()],
            SqlLoweringCode::BindingCount,
        ),
        (vec![InputValue::null(); 65], SqlLoweringCode::BindingLimit),
        (
            vec![InputValue::list(Vec::new())],
            SqlLoweringCode::BindingFamily,
        ),
        (
            vec![InputValue::map(Vec::new())],
            SqlLoweringCode::BindingFamily,
        ),
        (
            vec![InputValue::text("x".repeat(65537))],
            SqlLoweringCode::BindingLimit,
        ),
    ] {
        let error = validate_sql_bindings(&syntax, &inputs).expect_err("reject input");
        assert_eq!(
            error.diagnostic(),
            QueryError::sql_lowering(code).diagnostic()
        );
    }
    validate_sql_bindings(&syntax, &[InputValue::text("x".repeat(65536))])
        .expect("exact byte bound");
    for sql in [
        "SELECT ? FROM Bindings",
        "SELECT id FROM Bindings ORDER BY ?",
        "SELECT SUM(operand) FROM Bindings HAVING SUM(operand) >= ?",
        "EXPLAIN SELECT id FROM Bindings WHERE id = ?",
        "SHOW STORES",
        "DELETE FROM Bindings WHERE id = ?",
    ] {
        let Ok(syntax) = parse_sql(sql) else {
            // Some forbidden positions are rejected by the static grammar.
            continue;
        };
        let error = validate_sql_bindings(&syntax, &[InputValue::nat64(1)]).expect_err("placement");
        assert_eq!(
            error.diagnostic(),
            QueryError::sql_lowering(SqlLoweringCode::ParameterPlacement).diagnostic()
        );
    }
    let syntax =
        parse_sql("SELECT id FROM Bindings WHERE operand = '?'").expect("quoted question mark");
    validate_sql_bindings(&syntax, &[]).expect("no lexical slots");
    let sixty_four = format!(
        "SELECT id FROM Bindings WHERE id IN ({})",
        vec!["?"; 64].join(",")
    );
    validate_sql_bindings(
        &parse_sql(&sixty_four).expect("64 slots"),
        &vec![InputValue::nat64(1); 64],
    )
    .expect("exact count bound");
    let two = parse_sql("SELECT id FROM Bindings WHERE operand IN (?, ?)").expect("two slots");
    validate_sql_bindings(
        &two,
        &[
            InputValue::blob(vec![0; 32768]),
            InputValue::blob(vec![0; 32768]),
        ],
    )
    .expect("aggregate exact byte bound");
    assert!(
        validate_sql_bindings(
            &two,
            &[
                InputValue::blob(vec![0; 32768]),
                InputValue::blob(vec![0; 32769])
            ]
        )
        .is_err()
    );
    let huge = NatBig::from_biguint(num_bigint::BigUint::from(1_u8) << (65536 * 8));
    let one = parse_sql("SELECT id FROM Bindings WHERE operand = ?").expect("one slot");
    let error =
        validate_sql_bindings(&one, &[InputValue::nat_big(huge)]).expect_err("large magnitude");
    assert_eq!(
        error.diagnostic(),
        QueryError::sql_lowering(SqlLoweringCode::BindingLimit).diagnostic()
    );
}

#[test]
fn binding_uses_query_rules_without_sql_text_identity_coercion() {
    let syntax = parse_sql("SELECT id FROM Bindings WHERE operand >= ?").expect("syntax");
    prepare_bound_sql_statement(
        &syntax,
        "Bindings",
        &schema(AcceptedFieldKind::Nat64),
        &[InputValue::int64(-1)],
    )
    .expect("unsigned query boundary, not stored field input");
    let syntax = parse_sql("SELECT id FROM Bindings WHERE operand = ?").expect("syntax");
    assert!(
        prepare_bound_sql_statement(
            &syntax,
            "Bindings",
            &schema(AcceptedFieldKind::Ulid),
            &[InputValue::text(Ulid::from_u128(7).to_string())]
        )
        .is_err()
    );
    assert!(
        prepare_bound_sql_statement(
            &syntax,
            "Bindings",
            &schema(AcceptedFieldKind::Nat64),
            &[InputValue::from_public(PublicValue::Text("42".to_string()))]
        )
        .is_err()
    );
    let prepared = prepare_bound_sql_statement(
        &syntax,
        "Bindings",
        &schema(AcceptedFieldKind::Nat64),
        &[InputValue::null()],
    )
    .expect("SQL null, not write-nullability admission");
    let SqlStatement::Select(select) = prepared.into_statement() else {
        panic!("select");
    };
    assert!(
        matches!(select.predicate, Some(SqlExpr::Binary { right, .. }) if matches!(*right, SqlExpr::Literal(Value::Null)))
    );
}

#[test]
fn enclosing_type_checks_preserve_nested_operand_admission() {
    let nested = format!(
        "SELECT id FROM Bindings WHERE {}?{} = 'payload'",
        "LOWER(".repeat(32),
        ")".repeat(32)
    );
    let syntax = parse_sql(&nested).expect("nested wrappers");
    let original = syntax.clone();
    let input = [InputValue::text("PAYLOAD".into())];
    validate_sql_bindings(&syntax, &input).expect("ingress");
    prepare_bound_sql_statement(
        &syntax,
        "Bindings",
        &schema(AcceptedFieldKind::Text { max_len: None }),
        &input,
    )
    .expect("nested text remains admitted");
    assert_eq!(syntax, original);

    for sql in [
        "SELECT id FROM Bindings WHERE FALSE AND LOWER(ABS(?)) = 'x'",
        "SELECT id FROM Bindings WHERE TRUE OR COALESCE(?, ABS('bad')) = 'x'",
        "SELECT id FROM Bindings WHERE FALSE AND LOWER(CASE WHEN TRUE THEN ? ELSE ABS('bad') END) = 'x'",
    ] {
        let syntax = parse_sql(sql).expect("nested rejection syntax");
        validate_sql_bindings(&syntax, &input).expect("scalar preflight");
        assert!(
            prepare_bound_sql_statement(
                &syntax,
                "Bindings",
                &schema(AcceptedFieldKind::Text { max_len: None }),
                &input
            )
            .is_err()
        );
    }

    let syntax = parse_sql("SELECT id FROM Bindings WHERE COALESCE(operand = ?, FALSE)")
        .expect("nested comparison");
    let input = [InputValue::principal(Principal::anonymous())];
    validate_sql_bindings(&syntax, &input).expect("identity preflight");
    assert!(
        prepare_bound_sql_statement(
            &syntax,
            "Bindings",
            &schema(AcceptedFieldKind::Ulid),
            &input
        )
        .is_err()
    );

    let syntax = parse_sql("SELECT id FROM Bindings WHERE COALESCE(operand >= ?, FALSE)")
        .expect("query comparison");
    let input = [InputValue::int64(-1)];
    validate_sql_bindings(&syntax, &input).expect("numeric preflight");
    prepare_bound_sql_statement(
        &syntax,
        "Bindings",
        &schema(AcceptedFieldKind::Nat64),
        &input,
    )
    .expect("query boundary is not a write constraint");
}
