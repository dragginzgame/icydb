use super::*;
use crate::value::Value;

fn fixed_patch() -> AcceptedFixedUpdatePatch {
    AcceptedFixedUpdatePatch::from_canonical_fields(vec![
        (FieldSlot::from_validated_index(1), vec![1, 2, 3]),
        (FieldSlot::from_validated_index(4), vec![4, 5]),
    ])
    .expect("strict canonical fixed fields should admit")
}

fn scope(value: u64) -> Expr {
    Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("collection_id"))),
            right: Box::new(Expr::Literal(Value::Nat64(value))),
        }),
        right: Box::new(Expr::FunctionCall {
            function: Function::IsNotNull,
            args: vec![Expr::Field(FieldId::new("tier"))],
        }),
    }
}

fn intent(value: u64, timestamp: i64) -> CanonicalMutationIntent {
    CanonicalMutationIntent::new(
        [1; 16],
        [2; 32],
        "journaled".to_string(),
        "schema::Token".to_string(),
        7,
        11,
        1,
        [3; 16],
        &scope(value),
        &fixed_patch(),
        Timestamp::from_millis(timestamp),
        17,
    )
    .expect("bounded canonical intent should admit")
}

#[test]
fn intent_identity_failure_is_not_corruption_and_can_be_retried() {
    use crate::{
        db::QueryError,
        value::{test_hash_budget_error, with_test_hash_override},
    };
    let current = intent(7, 100);
    let bytes = current.encode().unwrap();
    let expected = MutationJobError::ExecutionBudgetExceeded {
        resource: icydb_diagnostic_code::DiagnosticExecutionBudgetResource::TemporaryBytes.raw(),
        limit: 10,
        observed: 11,
        scope: icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution.raw(),
        lane: icydb_diagnostic_code::DiagnosticExecutionLane::Mutation.raw(),
        normalized_shape_fingerprint_prefix: 7,
    };
    with_test_hash_override(Err(test_hash_budget_error), || {
        for _ in 0..2 {
            assert_eq!(
                CanonicalMutationIntent::decode(&bytes),
                Err(expected.clone())
            );
        }
        assert_eq!(
            MutationJobError::from_query_error(QueryError::execute(test_hash_budget_error())),
            expected
        );
    });
    assert_eq!(CanonicalMutationIntent::decode(&bytes).unwrap(), current);
    assert_eq!(
        MutationJobError::from_query_error(QueryError::sql_write_boundary(
            icydb_diagnostic_code::SqlWriteBoundaryCode::ResumableUpdateScopeDependsOnAssignedField,
        )),
        MutationJobError::IneligibleIntent,
    );
}

#[test]
fn current_intent_round_trip_preserves_scope_patch_and_request_identity() {
    let current = intent(7, 100);
    let bytes = current.encode().expect("current intent should encode");
    let decoded = CanonicalMutationIntent::decode(&bytes).expect("current intent should decode");

    assert_eq!(decoded, current);
    assert_eq!(decoded.decode_scope(), Ok(scope(7)));
    assert_eq!(decoded.decode_fixed_patch(), Ok(fixed_patch()));
    assert!(decoded.same_start_request(&intent(7, 200)));
    assert!(!decoded.same_start_request(&intent(8, 200)));
    assert_eq!(decoded.operation_timestamp(), Timestamp::from_millis(100));
}

#[test]
fn scope_expression_limits_admit_the_boundary_and_reject_one_more() {
    let mut nested = Expr::Literal(Value::Bool(true));
    for _ in 0..MAX_CANONICAL_EXPR_DEPTH {
        nested = Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(nested),
        };
    }
    let accepted = encode_scope(&nested).expect("maximum expression depth should encode");
    assert_eq!(decode_scope(&accepted).unwrap(), nested);
    let too_deep = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(nested),
    };
    assert_eq!(
        encode_scope(&too_deep),
        Err(MutationJobError::IneligibleIntent)
    );

    // Wrap the admitted wire expression once to exercise decoder admission
    // independently of the encoder's rejection.
    let mut writer = Writer::new();
    writer.raw(SCOPE_MAGIC);
    writer.u8(SCOPE_FORMAT_VERSION);
    writer.u8(4);
    writer.u8(unary_tag(UnaryOp::Not));
    writer.raw(&accepted[SCOPE_MAGIC.len() + 1..]);
    assert_eq!(
        decode_scope(&writer.finish()),
        Err(MutationJobError::CorruptProgressStore)
    );

    let literal = Expr::Literal(Value::Bool(true));
    let mut wide = Expr::FunctionCall {
        function: Function::Coalesce,
        args: vec![literal.clone(); MAX_CANONICAL_EXPR_NODES - 1],
    };
    let accepted = encode_scope(&wide).expect("maximum expression node count should encode");
    assert_eq!(decode_scope(&accepted).unwrap(), wide);
    let Expr::FunctionCall { args, .. } = &mut wide else {
        unreachable!("fixture is a function call");
    };
    args.push(literal.clone());
    assert_eq!(encode_scope(&wide), Err(MutationJobError::IneligibleIntent));

    // Each child is valid; the parent plus all children exceeds the total
    // node budget while remaining inside the wire argument-count bound.
    let leaf = encode_scope(&literal).unwrap();
    let mut writer = Writer::new();
    writer.raw(SCOPE_MAGIC);
    writer.u8(SCOPE_FORMAT_VERSION);
    writer.u8(3);
    writer.u8(function_tag(Function::Coalesce));
    writer.count(MAX_CANONICAL_EXPR_NODES).unwrap();
    for _ in 0..MAX_CANONICAL_EXPR_NODES {
        writer.raw(&leaf[SCOPE_MAGIC.len() + 1..]);
    }
    assert_eq!(
        decode_scope(&writer.finish()),
        Err(MutationJobError::CorruptProgressStore)
    );
}

#[test]
fn scope_literal_codec_rejects_deep_values_with_existing_intent_errors() {
    // One expression node containing a ~5-KiB value must still obey the
    // value codec's nesting bound, independently of the expression limit.
    let mut payload = Vec::new();
    for _ in 0..1000 {
        payload.extend_from_slice(&[11, 0, 0, 0, 1]); // singleton value list
    }
    payload.push(21); // unit leaf
    let mut scope_bytes = Vec::from(*SCOPE_MAGIC);
    scope_bytes.extend_from_slice(&[SCOPE_FORMAT_VERSION, 2]); // literal
    scope_bytes.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    scope_bytes.extend_from_slice(&payload);
    assert!(scope_bytes.len() < MAX_CANONICAL_SCOPE_BYTES);
    assert_eq!(
        decode_scope(&scope_bytes),
        Err(MutationJobError::CorruptProgressStore)
    );

    let mut value = Value::Bool(false);
    for _ in 0..129 {
        value = Value::List(vec![value]);
    }
    assert_eq!(
        encode_scope(&Expr::Literal(value)),
        Err(MutationJobError::IneligibleIntent)
    );
    let wide = Expr::Literal(Value::List(vec![Value::Bool(true); 1000]));
    let bytes = encode_scope(&wide).unwrap();
    assert_eq!(decode_scope(&bytes).unwrap(), wide);
}

#[test]
fn current_intent_rejects_corruption_future_format_and_noncanonical_patch() {
    let current = intent(7, 100);
    let mut corrupt = current.encode().expect("current intent should encode");
    corrupt[40] ^= 1;
    assert_eq!(
        CanonicalMutationIntent::decode(&corrupt),
        Err(MutationJobError::CorruptProgressStore),
    );

    let mut future = current.encode().expect("current intent should encode");
    future[INTENT_MAGIC.len()] = INTENT_FORMAT_VERSION + 1;
    let checksum_offset = future.len() - size_of::<u32>();
    let checksum = crc32c(&future[..checksum_offset]);
    future[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    assert_eq!(
        CanonicalMutationIntent::decode(&future),
        Err(MutationJobError::IncompatibleProgressFormat),
    );

    assert!(
        AcceptedFixedUpdatePatch::from_canonical_fields(vec![
            (FieldSlot::from_validated_index(2), vec![1]),
            (FieldSlot::from_validated_index(2), vec![2]),
        ])
        .is_err()
    );
}

#[test]
fn current_intent_preserves_fixed_integer_decimal_and_account_literals() {
    use crate::types::{Account, Decimal, Principal, Subaccount, U256};
    let values = [
        Value::Int128(i128::MIN),
        Value::U256(U256::MAX),
        Value::Decimal(Decimal::from_i128_with_scale(i128::MIN, 28)),
        Value::Account(Account::new(Principal::MAX, Some(Subaccount::MAX))),
    ];
    for value in values {
        let scope = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("value"))),
            right: Box::new(Expr::Literal(value)),
        };
        let current = CanonicalMutationIntent::new(
            [1; 16],
            [2; 32],
            "journaled".to_string(),
            "schema::Token".to_string(),
            7,
            11,
            1,
            [3; 16],
            &scope,
            &fixed_patch(),
            Timestamp::from_millis(100),
            17,
        )
        .unwrap();
        let bytes = current.encode().unwrap();
        let decoded = CanonicalMutationIntent::decode(&bytes).unwrap();
        assert_eq!(decoded.decode_scope().unwrap(), scope);
        assert_eq!(decoded.encode().unwrap(), bytes);
    }
}
