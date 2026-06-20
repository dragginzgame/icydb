//! Module: db::index::predicate::tests
//! Covers index-predicate derivation and normalization behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        index::{
            IndexCompareOp, IndexId, IndexKey, IndexKeyKind, IndexLiteral, IndexPredicateProgram,
            predicate::literal_index_component_bytes,
        },
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        predicate::{
            CoercionId, CoercionSpec, CompareOp, ComparePredicate, ExecutableComparePredicate,
            ExecutablePredicate, IndexCompileTarget, Predicate, PredicateProgram, compare_eq,
            compare_order,
        },
    },
    error::{ErrorClass, ErrorOrigin},
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel},
        index::{IndexExpression, IndexKeyItem, IndexModel, IndexPredicateMetadata},
    },
    types::Decimal,
    types::EntityTag,
    value::Value,
};
use std::{
    cmp::Ordering,
    hint::black_box,
    sync::LazyLock,
    time::{Duration, Instant},
};

use super::{
    IndexCompilePolicy, canonical_index_predicate, compile_index_program,
    compile_index_program_for_targets, eval_index_compare, eval_index_program_on_decoded_key,
    eval_index_program_on_prefix_components,
};

static ACTIVE_TRUE_PREDICATE: LazyLock<Predicate> =
    LazyLock::new(|| Predicate::eq("active".to_string(), true.into()));

static INDEX_PREDICATE_FIELDS: [FieldModel; 5] = [
    FieldModel::generated("id", FieldKind::Nat64),
    FieldModel::generated("collection_id", FieldKind::Text { max_len: None }),
    FieldModel::generated("stage", FieldKind::Text { max_len: None }),
    FieldModel::generated("rank", FieldKind::Nat64),
    FieldModel::generated("title", FieldKind::Text { max_len: None }),
];
static INDEX_PREDICATE_MODEL: EntityModel = EntityModel::generated(
    "IndexPredicateTestEntity",
    "IndexPredicateTestEntity",
    1,
    &INDEX_PREDICATE_FIELDS[0],
    0,
    &INDEX_PREDICATE_FIELDS,
    &[],
);
const INDEX_PREDICATE_SLOTS: [usize; 4] = [1, 2, 3, 4];
const INDEX_MEMBERSHIP_BENCH_ITERATIONS: usize = 20_000;
const INDEX_MEMBERSHIP_BENCH_COUNTS: [usize; 8] = [4, 8, 16, 24, 32, 48, 64, 128];

fn active_true_predicate() -> &'static Predicate {
    &ACTIVE_TRUE_PREDICATE
}

const fn active_true_predicate_metadata() -> IndexPredicateMetadata {
    IndexPredicateMetadata::generated("active = true", active_true_predicate)
}

fn strict_predicate(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        value,
        CoercionId::Strict,
    ))
}

fn strict_text(field: &str, op: CompareOp, value: &str) -> Predicate {
    strict_predicate(field, op, Value::Text(value.to_string()))
}

fn index_predicate_row(collection_id: &str, stage: &str, rank: u64, title: &str) -> Vec<Value> {
    vec![
        Value::Nat64(rank),
        Value::Text(collection_id.to_string()),
        Value::Text(stage.to_string()),
        Value::Nat64(rank),
        Value::Text(title.to_string()),
    ]
}

fn index_key_for_predicate_row(row: &[Value], primary_key: u64) -> IndexKey {
    let components = INDEX_PREDICATE_SLOTS
        .iter()
        .map(|slot| literal_index_component_bytes(&row[*slot]).expect("index value should encode"))
        .collect::<Vec<_>>();

    IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new(EntityTag::new(0x184), 1),
        IndexKeyKind::User,
        &components,
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(primary_key)),
    )
}

fn assert_index_program_matches_runtime(predicate: Predicate, rows: Vec<Vec<Value>>) {
    let runtime_program =
        PredicateProgram::compile_for_model_only(&INDEX_PREDICATE_MODEL, &predicate);
    let index_program = compile_index_program(
        runtime_program.executable(),
        &INDEX_PREDICATE_SLOTS,
        IndexCompilePolicy::StrictAllOrNone,
    )
    .expect("strict predicate should compile into an index predicate");

    for (row_index, row) in rows.iter().enumerate() {
        let key = index_key_for_predicate_row(row, row_index as u64);
        let mut read_slot = |slot| row.get(slot);
        let runtime_result = runtime_program.eval_with_slot_value_ref_reader(&mut read_slot);
        let index_result = eval_index_program_on_decoded_key(&key, &index_program)
            .expect("index predicate should evaluate");

        assert_eq!(
            index_result, runtime_result,
            "index predicate result diverged from runtime predicate for row_index={row_index} row={row:?}",
        );
    }
}

// Run explicitly when assessing whether the small linear membership threshold
// should move. Timings are informational; correctness stays covered by the
// non-ignored equivalence tests above.
#[test]
#[ignore = "native microbenchmark: run explicitly with --ignored --nocapture"]
fn index_predicate_membership_microbenchmark_report() {
    println!();
    println!("Encoded index predicate membership microbenchmark");
    println!("iterations={INDEX_MEMBERSHIP_BENCH_ITERATIONS}");
    println!();

    for candidate_count in INDEX_MEMBERSHIP_BENCH_COUNTS {
        report_membership_benchmark(candidate_count, false);
        report_membership_benchmark(candidate_count, true);
        println!();
    }
}

fn report_membership_benchmark(candidate_count: usize, sorted: bool) {
    let candidates = membership_benchmark_candidates(candidate_count);
    let probes = membership_benchmark_probes(candidate_count);
    let literal = if sorted {
        IndexLiteral::ManySorted(candidates)
    } else {
        IndexLiteral::Many(candidates)
    };
    let label = if sorted {
        "sorted binary"
    } else {
        "linear scan"
    };
    let (elapsed, checksum) = measure_membership_benchmark(&literal, probes.as_slice());
    let iterations =
        u128::try_from(INDEX_MEMBERSHIP_BENCH_ITERATIONS).expect("iterations should fit u128");

    println!(
        "candidates={candidate_count:<4} {label:<13} total_ns={:<14} avg_ns_per_iteration={} checksum={checksum}",
        elapsed.as_nanos(),
        elapsed.as_nanos() / iterations,
    );
}

fn measure_membership_benchmark(literal: &IndexLiteral, probes: &[Vec<u8>]) -> (Duration, usize) {
    let warm = black_box(membership_checksum(literal, probes));
    assert!(warm > 0, "membership benchmark should exercise probes");

    let started_at = Instant::now();
    let mut measured = 0usize;
    for _ in 0..INDEX_MEMBERSHIP_BENCH_ITERATIONS {
        measured = measured.saturating_add(black_box(membership_checksum(literal, probes)));
    }

    (started_at.elapsed(), measured)
}

fn membership_checksum(literal: &IndexLiteral, probes: &[Vec<u8>]) -> usize {
    probes
        .iter()
        .enumerate()
        .map(|(index, probe)| {
            let passed = eval_index_compare(probe.as_slice(), IndexCompareOp::In, literal);
            usize::from(passed).saturating_mul(index + 1)
        })
        .sum()
}

fn membership_benchmark_candidates(candidate_count: usize) -> Vec<Vec<u8>> {
    (0..candidate_count)
        .map(membership_benchmark_component)
        .collect()
}

fn membership_benchmark_probes(candidate_count: usize) -> Vec<Vec<u8>> {
    [
        0,
        candidate_count / 2,
        candidate_count.saturating_sub(1),
        candidate_count,
        candidate_count + 1,
        candidate_count + 2,
    ]
    .into_iter()
    .map(membership_benchmark_component)
    .collect()
}

fn membership_benchmark_component(index: usize) -> Vec<u8> {
    literal_index_component_bytes(&Value::Text(format!("member-{index:04}")))
        .expect("benchmark literal should encode")
}

// Match index compare operations to strict predicate semantics for expected results.
fn expected_strict_compare(
    op: IndexCompareOp,
    left: &Value,
    right: &Value,
    strict: &CoercionSpec,
) -> bool {
    match op {
        IndexCompareOp::Eq => compare_eq(left, right, strict).unwrap_or(false),
        IndexCompareOp::Ne => compare_eq(left, right, strict).is_some_and(|equal| !equal),
        IndexCompareOp::Lt => compare_order(left, right, strict).is_some_and(Ordering::is_lt),
        IndexCompareOp::Lte => compare_order(left, right, strict).is_some_and(Ordering::is_le),
        IndexCompareOp::Gt => compare_order(left, right, strict).is_some_and(Ordering::is_gt),
        IndexCompareOp::Gte => compare_order(left, right, strict).is_some_and(Ordering::is_ge),
        IndexCompareOp::In | IndexCompareOp::NotIn => {
            unreachable!("expected_strict_compare only handles one-literal compare operators")
        }
    }
}

#[test]
fn canonical_index_predicate_reuses_parsed_predicate_for_equivalent_sql_text() {
    static INDEX_A: IndexModel = IndexModel::generated_with_predicate(
        "idx_entity__active",
        "entity::index",
        &["active"],
        false,
        Some(active_true_predicate_metadata()),
    );
    static INDEX_B: IndexModel = IndexModel::generated_with_predicate(
        "idx_entity__active_alt",
        "entity::index",
        &["active"],
        false,
        Some(active_true_predicate_metadata()),
    );

    let first = canonical_index_predicate(&INDEX_A).expect("predicate should exist");
    let second = canonical_index_predicate(&INDEX_A).expect("predicate should exist");
    let third = canonical_index_predicate(&INDEX_B).expect("predicate should exist");

    assert!(
        std::ptr::eq(first, second),
        "same index predicate should return the same canonical predicate instance",
    );
    assert!(
        std::ptr::eq(first, third),
        "equivalent predicate SQL should resolve to the same canonical predicate instance",
    );
}

#[test]
fn canonical_index_predicate_is_absent_for_unfiltered_index() {
    static INDEX: IndexModel =
        IndexModel::generated("idx_entity__active", "entity::index", &["active"], false);

    assert!(canonical_index_predicate(&INDEX).is_none());
}

#[test]
fn compile_index_program_maps_field_slot_to_component_index() {
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(7),
        CompareOp::Eq,
        Value::Nat64(11),
        CoercionSpec::new(CoercionId::Strict),
    ));

    let program = compile_index_program(
        &predicate,
        &[3, 7, 9],
        IndexCompilePolicy::ConservativeSubset,
    )
    .expect("strict EQ over indexed slot should compile");
    let expected =
        literal_index_component_bytes(&Value::Nat64(11)).expect("nat literal should convert");

    assert_eq!(
        program,
        IndexPredicateProgram::Compare {
            component_index: 1,
            op: IndexCompareOp::Eq,
            literal: IndexLiteral::One(expected),
        }
    );
}

#[test]
fn prefix_component_predicate_evaluation_rejects_known_false_prefixes() {
    let collection = literal_index_component_bytes(&Value::Text("collection-a".to_string()))
        .expect("collection literal should encode");
    let draft =
        literal_index_component_bytes(&Value::Text("Draft".to_string())).expect("draft literal");
    let review =
        literal_index_component_bytes(&Value::Text("Review".to_string())).expect("review literal");
    let program = IndexPredicateProgram::Compare {
        component_index: 1,
        op: IndexCompareOp::Ne,
        literal: IndexLiteral::One(review.clone()),
    };

    assert_eq!(
        eval_index_program_on_prefix_components(&[collection.clone(), draft], &program,),
        Some(true),
        "known Draft branch prefix should pass stage != Review",
    );
    assert_eq!(
        eval_index_program_on_prefix_components(&[collection.clone(), review], &program),
        Some(false),
        "known Review branch prefix should be pruned before scanning",
    );
    assert_eq!(
        eval_index_program_on_prefix_components(&[collection], &program),
        None,
        "predicates on unbound suffix components must remain unknown",
    );
}

#[test]
fn compile_index_program_rejects_non_strict_coercion() {
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(1),
        CompareOp::Eq,
        Value::Nat64(11),
        CoercionSpec::new(CoercionId::NumericWiden),
    ));

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);
    assert!(program.is_none());
}

#[test]
fn compile_index_program_operator_matrix_matches_strict_subset() {
    let eligible = [
        (CompareOp::Eq, Value::Nat64(11)),
        (CompareOp::Ne, Value::Nat64(11)),
        (CompareOp::Lt, Value::Nat64(11)),
        (CompareOp::Lte, Value::Nat64(11)),
        (CompareOp::Gt, Value::Nat64(11)),
        (CompareOp::Gte, Value::Nat64(11)),
        (
            CompareOp::In,
            Value::List(vec![Value::Nat64(11), Value::Nat64(12)]),
        ),
        (
            CompareOp::NotIn,
            Value::List(vec![Value::Nat64(11), Value::Nat64(12)]),
        ),
        (CompareOp::StartsWith, Value::Text("x".to_string())),
    ];
    for (op, value) in eligible {
        let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            Some(1),
            op,
            value,
            CoercionSpec::new(CoercionId::Strict),
        ));
        let program =
            compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);

        assert!(
            program.is_some(),
            "strict compare op {op:?} should compile into an index predicate program",
        );
    }

    let ineligible = [
        (CompareOp::Contains, Value::Text("x".to_string())),
        (CompareOp::EndsWith, Value::Text("x".to_string())),
    ];
    for (op, value) in ineligible {
        let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            Some(1),
            op,
            value,
            CoercionSpec::new(CoercionId::Strict),
        ));
        let program =
            compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);

        assert!(
            program.is_none(),
            "op {op:?} should stay on fallback execution",
        );
    }
}

#[test]
fn compile_index_program_starts_with_compiles_to_bounded_range_compare_pair() {
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(1),
        CompareOp::StartsWith,
        Value::Text("foo".to_string()),
        CoercionSpec::new(CoercionId::Strict),
    ));

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset)
        .expect("strict starts-with should compile for index prefilter");
    let expected_lower =
        literal_index_component_bytes(&Value::Text("foo".to_string())).expect("lower bytes");
    let expected_upper =
        literal_index_component_bytes(&Value::Text("fop".to_string())).expect("upper bytes");

    assert_eq!(
        program,
        IndexPredicateProgram::And(vec![
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Gte,
                literal: IndexLiteral::One(expected_lower),
            },
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Lt,
                literal: IndexLiteral::One(expected_upper),
            },
        ]),
    );
}

#[test]
fn compile_index_program_starts_with_rejects_empty_prefix() {
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(1),
        CompareOp::StartsWith,
        Value::Text(String::new()),
        CoercionSpec::new(CoercionId::Strict),
    ));

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);
    assert!(program.is_none());
}

#[test]
fn compile_index_program_starts_with_high_unicode_skips_surrogate_gap_upper_bound() {
    let prefix = format!("foo{}", char::from_u32(0xD7FF).expect("valid scalar"));
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(1),
        CompareOp::StartsWith,
        Value::Text(prefix.clone()),
        CoercionSpec::new(CoercionId::Strict),
    ));

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset)
        .expect("strict starts-with should compile for high-unicode prefix");
    let expected_lower =
        literal_index_component_bytes(&Value::Text(prefix)).expect("lower bytes should convert");
    let expected_upper = literal_index_component_bytes(&Value::Text(format!(
        "foo{}",
        char::from_u32(0xE000).expect("valid scalar")
    )))
    .expect("upper bytes should skip surrogate gap");

    assert_eq!(
        program,
        IndexPredicateProgram::And(vec![
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Gte,
                literal: IndexLiteral::One(expected_lower),
            },
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Lt,
                literal: IndexLiteral::One(expected_upper),
            },
        ]),
    );
}

#[test]
fn compile_index_program_starts_with_max_unicode_compiles_to_lower_bound_only() {
    let prefix = char::from_u32(0x10_FFFF).expect("valid scalar").to_string();
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(1),
        CompareOp::StartsWith,
        Value::Text(prefix.clone()),
        CoercionSpec::new(CoercionId::Strict),
    ));

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset)
        .expect("max-unicode starts-with should compile to one lower-bound compare");
    let expected_lower =
        literal_index_component_bytes(&Value::Text(prefix)).expect("lower bytes should convert");

    assert_eq!(
        program,
        IndexPredicateProgram::Compare {
            component_index: 0,
            op: IndexCompareOp::Gte,
            literal: IndexLiteral::One(expected_lower),
        },
    );
}

#[test]
fn compile_index_program_strict_mode_accepts_starts_with_bounded_prefix() {
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(1),
        CompareOp::StartsWith,
        Value::Text("foo".to_string()),
        CoercionSpec::new(CoercionId::Strict),
    ));

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::StrictAllOrNone)
        .expect("strict-all-or-none should compile starts-with when fully index-expressible");
    let expected_lower =
        literal_index_component_bytes(&Value::Text("foo".to_string())).expect("lower bytes");
    let expected_upper =
        literal_index_component_bytes(&Value::Text("fop".to_string())).expect("upper bytes");

    assert_eq!(
        program,
        IndexPredicateProgram::And(vec![
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Gte,
                literal: IndexLiteral::One(expected_lower),
            },
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Lt,
                literal: IndexLiteral::One(expected_upper),
            },
        ]),
    );
}

#[test]
fn compile_index_program_strict_mode_accepts_starts_with_max_unicode_prefix() {
    let prefix = char::from_u32(0x10_FFFF).expect("valid scalar").to_string();
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(1),
        CompareOp::StartsWith,
        Value::Text(prefix.clone()),
        CoercionSpec::new(CoercionId::Strict),
    ));

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::StrictAllOrNone)
        .expect("strict-all-or-none should compile max-unicode starts-with lower-bound form");
    let expected_lower =
        literal_index_component_bytes(&Value::Text(prefix)).expect("lower bytes should convert");

    assert_eq!(
        program,
        IndexPredicateProgram::Compare {
            component_index: 0,
            op: IndexCompareOp::Gte,
            literal: IndexLiteral::One(expected_lower),
        },
    );
}

#[test]
fn compile_index_program_targets_accept_text_casefold_strict_range() {
    let predicate = ExecutablePredicate::And(vec![
        ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            Some(1),
            CompareOp::Gte,
            Value::Text("BR".to_string()),
            CoercionSpec::new(CoercionId::TextCasefold),
        )),
        ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            Some(1),
            CompareOp::Lt,
            Value::Text("BS".to_string()),
            CoercionSpec::new(CoercionId::TextCasefold),
        )),
    ]);
    let compile_targets = [IndexCompileTarget {
        component_index: 0,
        field_slot: 1,
        key_item: IndexKeyItem::Expression(IndexExpression::Lower("name")),
    }];

    let program = compile_index_program_for_targets(
        &predicate,
        &compile_targets,
        IndexCompilePolicy::StrictAllOrNone,
    )
    .expect("strict-all-or-none should compile text-casefold range for expression target");
    let expected_lower =
        literal_index_component_bytes(&Value::Text("br".to_string())).expect("lower bytes");
    let expected_upper =
        literal_index_component_bytes(&Value::Text("bs".to_string())).expect("upper bytes");

    assert_eq!(
        program,
        IndexPredicateProgram::And(vec![
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Gte,
                literal: IndexLiteral::One(expected_lower),
            },
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Lt,
                literal: IndexLiteral::One(expected_upper),
            },
        ]),
    );
}

#[test]
fn compile_index_program_rejects_non_strict_coercion_across_operator_subset() {
    let operators = [
        (CompareOp::Eq, Value::Nat64(11)),
        (CompareOp::Ne, Value::Nat64(11)),
        (CompareOp::Lt, Value::Nat64(11)),
        (CompareOp::Lte, Value::Nat64(11)),
        (CompareOp::Gt, Value::Nat64(11)),
        (CompareOp::Gte, Value::Nat64(11)),
        (
            CompareOp::In,
            Value::List(vec![Value::Nat64(11), Value::Nat64(12)]),
        ),
        (
            CompareOp::NotIn,
            Value::List(vec![Value::Nat64(11), Value::Nat64(12)]),
        ),
    ];

    for (op, value) in operators {
        let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            Some(1),
            op,
            value,
            CoercionSpec::new(CoercionId::NumericWiden),
        ));
        let program =
            compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);

        assert!(
            program.is_none(),
            "non-strict coercion for op {op:?} must remain unsupported in index subset",
        );
    }
}

#[test]
fn compile_index_program_rejects_in_with_non_list_literal() {
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(1),
        CompareOp::In,
        Value::Nat64(11),
        CoercionSpec::new(CoercionId::Strict),
    ));

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);
    assert!(program.is_none());
}

#[test]
fn compile_index_program_rejects_in_with_empty_list_literal() {
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(1),
        CompareOp::In,
        Value::List(Vec::new()),
        CoercionSpec::new(CoercionId::Strict),
    ));

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);
    assert!(program.is_none());
}

#[test]
fn compile_index_program_keeps_small_in_literals_linear() {
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(1),
        CompareOp::In,
        Value::List(vec![
            Value::Text("gamma".to_string()),
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
            Value::Text("alpha".to_string()),
        ]),
        CoercionSpec::new(CoercionId::Strict),
    ));

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset)
        .expect("strict IN should compile to an index predicate");
    let alpha = literal_index_component_bytes(&Value::Text("alpha".to_string()))
        .expect("alpha literal should encode");
    let beta = literal_index_component_bytes(&Value::Text("beta".to_string()))
        .expect("beta literal should encode");
    let gamma = literal_index_component_bytes(&Value::Text("gamma".to_string()))
        .expect("gamma literal should encode");

    assert_eq!(
        program,
        IndexPredicateProgram::Compare {
            component_index: 0,
            op: IndexCompareOp::In,
            literal: IndexLiteral::Many(vec![gamma, alpha.clone(), beta, alpha]),
        },
        "small index membership literals should keep linear caller-order evaluation",
    );
}

#[test]
fn compile_index_program_canonicalizes_threshold_in_literals_for_binary_membership() {
    let values = (0..16)
        .rev()
        .map(|idx| Value::Text(format!("v{idx:02}")))
        .collect::<Vec<_>>();
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(1),
        CompareOp::In,
        Value::List(values),
        CoercionSpec::new(CoercionId::Strict),
    ));

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset)
        .expect("threshold-size strict IN should compile to an index predicate");
    let mut expected = (0..16)
        .map(|idx| {
            literal_index_component_bytes(&Value::Text(format!("v{idx:02}")))
                .expect("expected literal should encode")
        })
        .collect::<Vec<_>>();
    expected.sort();

    assert_eq!(
        program,
        IndexPredicateProgram::Compare {
            component_index: 0,
            op: IndexCompareOp::In,
            literal: IndexLiteral::ManySorted(expected),
        },
        "threshold-size membership literals should use binary search",
    );
}

#[test]
fn compile_index_program_canonicalizes_large_in_literals_for_binary_membership() {
    let mut values = (0..40)
        .rev()
        .map(|idx| Value::Text(format!("v{idx:02}")))
        .collect::<Vec<_>>();
    values.push(Value::Text("v07".to_string()));
    let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(1),
        CompareOp::In,
        Value::List(values),
        CoercionSpec::new(CoercionId::Strict),
    ));

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset)
        .expect("strict IN should compile to an index predicate");
    let mut expected = (0..40)
        .map(|idx| {
            literal_index_component_bytes(&Value::Text(format!("v{idx:02}")))
                .expect("expected literal should encode")
        })
        .collect::<Vec<_>>();
    expected.sort();
    expected.dedup();
    let hit = literal_index_component_bytes(&Value::Text("v07".to_string()))
        .expect("hit literal should encode");

    let IndexPredicateProgram::Compare { literal, .. } = &program else {
        panic!("expected compare program");
    };
    assert_eq!(
        program,
        IndexPredicateProgram::Compare {
            component_index: 0,
            op: IndexCompareOp::In,
            literal: IndexLiteral::ManySorted(expected),
        },
        "large index membership literals must be sorted and deduplicated for binary search",
    );
    assert!(
        eval_index_compare(hit.as_slice(), IndexCompareOp::In, literal),
        "canonicalized compiled membership should preserve strict IN semantics",
    );
}

#[test]
fn compile_index_program_and_subset_compiles_supported_children_only() {
    let predicate = ExecutablePredicate::And(vec![
        ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            Some(1),
            CompareOp::Eq,
            Value::Nat64(11),
            CoercionSpec::new(CoercionId::Strict),
        )),
        ExecutablePredicate::TextContains {
            field_slot: Some(1),
            value: Value::Text("x".to_string()),
        },
        ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            Some(2),
            CompareOp::Gt,
            Value::Nat64(9),
            CoercionSpec::new(CoercionId::Strict),
        )),
    ]);

    let program =
        compile_index_program(&predicate, &[1, 2], IndexCompilePolicy::ConservativeSubset)
            .expect("subset mode should keep supported children");

    let expected_left =
        literal_index_component_bytes(&Value::Nat64(11)).expect("left should convert");
    let expected_right =
        literal_index_component_bytes(&Value::Nat64(9)).expect("right should convert");

    assert_eq!(
        program,
        IndexPredicateProgram::And(vec![
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Eq,
                literal: IndexLiteral::One(expected_left),
            },
            IndexPredicateProgram::Compare {
                component_index: 1,
                op: IndexCompareOp::Gt,
                literal: IndexLiteral::One(expected_right),
            },
        ]),
    );
}

#[test]
fn compile_index_program_and_subset_drops_fully_unsupported_and() {
    let predicate = ExecutablePredicate::And(vec![
        ExecutablePredicate::TextContains {
            field_slot: Some(1),
            value: Value::Text("x".to_string()),
        },
        ExecutablePredicate::IsNull {
            field_slot: Some(2),
        },
    ]);

    let program =
        compile_index_program(&predicate, &[1, 2], IndexCompilePolicy::ConservativeSubset);
    assert!(program.is_none());
}

#[test]
fn compile_index_program_strict_rejects_partial_and_support() {
    let predicate = ExecutablePredicate::And(vec![
        ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            Some(1),
            CompareOp::Eq,
            Value::Nat64(11),
            CoercionSpec::new(CoercionId::Strict),
        )),
        ExecutablePredicate::TextContains {
            field_slot: Some(1),
            value: Value::Text("x".to_string()),
        },
    ]);

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::StrictAllOrNone);
    assert!(program.is_none());
}

#[test]
fn eval_index_program_matches_runtime_for_strict_compare_tree() {
    let predicate = Predicate::And(vec![
        strict_text("collection_id", CompareOp::Eq, "collection-a"),
        Predicate::Or(vec![
            strict_text("stage", CompareOp::Eq, "Draft"),
            strict_text("stage", CompareOp::Eq, "Review"),
        ]),
        strict_predicate("rank", CompareOp::Gte, Value::Nat64(10)),
        strict_predicate("rank", CompareOp::Lt, Value::Nat64(20)),
    ]);
    let rows = vec![
        index_predicate_row("collection-a", "Draft", 10, "Alpha"),
        index_predicate_row("collection-a", "Review", 19, "Beta"),
        index_predicate_row("collection-a", "Review", 20, "Gamma"),
        index_predicate_row("collection-a", "Published", 12, "Delta"),
        index_predicate_row("collection-b", "Draft", 12, "Epsilon"),
    ];

    assert_index_program_matches_runtime(predicate, rows);
}

#[test]
fn eval_index_program_matches_runtime_for_membership_literals() {
    let predicate = Predicate::And(vec![
        strict_predicate(
            "stage",
            CompareOp::In,
            Value::List(vec![
                Value::Text("Draft".to_string()),
                Value::Text("Review".to_string()),
            ]),
        ),
        strict_predicate(
            "title",
            CompareOp::NotIn,
            Value::List(vec![
                Value::Text("Rejected".to_string()),
                Value::Text("Archived".to_string()),
            ]),
        ),
    ]);
    let rows = vec![
        index_predicate_row("collection-a", "Draft", 1, "Ready"),
        index_predicate_row("collection-a", "Review", 2, "Rejected"),
        index_predicate_row("collection-a", "Published", 3, "Ready"),
        index_predicate_row("collection-a", "Review", 4, "Archived"),
    ];

    assert_index_program_matches_runtime(predicate, rows);
}

#[test]
fn eval_index_program_matches_runtime_for_large_sorted_membership_literals() {
    let values = (0..40)
        .map(|idx| Value::Text(format!("stage-{idx:02}")))
        .collect::<Vec<_>>();
    let predicate = strict_predicate("stage", CompareOp::In, Value::List(values));
    let rows = vec![
        index_predicate_row("collection-a", "stage-00", 1, "Alpha"),
        index_predicate_row("collection-a", "stage-17", 2, "Beta"),
        index_predicate_row("collection-a", "stage-39", 3, "Gamma"),
        index_predicate_row("collection-a", "stage-99", 4, "Delta"),
    ];

    assert_index_program_matches_runtime(predicate, rows);
}

#[test]
fn eval_index_program_matches_runtime_for_text_prefix_bounds() {
    let predicate = strict_text("title", CompareOp::StartsWith, "Alp");
    let rows = vec![
        index_predicate_row("collection-a", "Draft", 1, "Alpha"),
        index_predicate_row("collection-a", "Draft", 2, "Alpine"),
        index_predicate_row("collection-a", "Draft", 3, "Alq"),
        index_predicate_row("collection-a", "Draft", 4, "Beta"),
    ];

    assert_index_program_matches_runtime(predicate, rows);
}

#[test]
fn conservative_and_subset_never_rejects_runtime_matches() {
    let predicate = Predicate::And(vec![
        strict_text("stage", CompareOp::Eq, "Draft"),
        Predicate::TextContains {
            field: "title".to_string(),
            value: Value::Text("urgent".to_string()),
        },
    ]);
    let runtime_program =
        PredicateProgram::compile_for_model_only(&INDEX_PREDICATE_MODEL, &predicate);
    let index_program = compile_index_program(
        runtime_program.executable(),
        &INDEX_PREDICATE_SLOTS,
        IndexCompilePolicy::ConservativeSubset,
    )
    .expect("strict indexed AND child should remain as a conservative subset");
    let rows = [
        index_predicate_row("collection-a", "Draft", 1, "urgent item"),
        index_predicate_row("collection-a", "Draft", 2, "ordinary item"),
        index_predicate_row("collection-a", "Review", 3, "urgent item"),
    ];

    for (row_index, row) in rows.iter().enumerate() {
        let key = index_key_for_predicate_row(row, row_index as u64);
        let mut read_slot = |slot| row.get(slot);
        let runtime_result = runtime_program.eval_with_slot_value_ref_reader(&mut read_slot);
        let index_result = eval_index_program_on_decoded_key(&key, &index_program)
            .expect("index predicate should evaluate");

        assert!(
            !runtime_result || index_result,
            "conservative index predicate must not reject a runtime match for row_index={row_index} row={row:?}",
        );
    }
}

#[test]
fn eval_index_compare_matches_strict_semantics_for_one_literal_ops() {
    let strict = CoercionSpec::new(CoercionId::Strict);
    let cases = vec![
        (Value::Int64(-2), Value::Int64(7)),
        (
            Value::Decimal(Decimal::new(10, 1)),
            Value::Decimal(Decimal::new(1, 0)),
        ),
        (
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ),
    ];
    let operators = [
        IndexCompareOp::Eq,
        IndexCompareOp::Ne,
        IndexCompareOp::Lt,
        IndexCompareOp::Lte,
        IndexCompareOp::Gt,
        IndexCompareOp::Gte,
    ];

    for (left, right) in cases {
        let component = literal_index_component_bytes(&left).expect("left value should encode");
        let literal = IndexLiteral::One(
            literal_index_component_bytes(&right).expect("right value should encode"),
        );

        for op in operators {
            let expected = expected_strict_compare(op, &left, &right, &strict);
            let actual = eval_index_compare(component.as_slice(), op, &literal);

            assert_eq!(
                actual, expected,
                "index compare drifted from strict predicate semantics for op={op:?} left={left:?} right={right:?}",
            );
        }
    }
}

#[test]
fn eval_index_compare_in_and_not_in_match_strict_membership_semantics() {
    let strict = CoercionSpec::new(CoercionId::Strict);
    let target = Value::Text("beta".to_string());
    let candidates = [
        Value::Text("alpha".to_string()),
        Value::Text("beta".to_string()),
        Value::Text("gamma".to_string()),
    ];
    let component = literal_index_component_bytes(&target).expect("target should encode");
    let literal = IndexLiteral::Many(
        candidates
            .iter()
            .map(literal_index_component_bytes)
            .collect::<Option<Vec<_>>>()
            .expect("all candidate literals should encode"),
    );

    let expected_in = candidates
        .iter()
        .any(|candidate| compare_eq(&target, candidate, &strict).unwrap_or(false));
    let expected_not_in = candidates
        .iter()
        .all(|candidate| compare_eq(&target, candidate, &strict).is_some_and(|eq| !eq));

    assert_eq!(
        eval_index_compare(component.as_slice(), IndexCompareOp::In, &literal),
        expected_in,
    );
    assert_eq!(
        eval_index_compare(component.as_slice(), IndexCompareOp::NotIn, &literal),
        expected_not_in,
    );
}

#[test]
fn eval_index_program_missing_component_is_index_invariant() {
    let (key, _) = IndexKey::bounds_for_prefix_with_kind(
        &IndexId::new(EntityTag::new(7), 0),
        IndexKeyKind::User,
        0,
        &[] as &[Vec<u8>],
    );
    let program = IndexPredicateProgram::Compare {
        component_index: 0,
        op: IndexCompareOp::Eq,
        literal: IndexLiteral::One(vec![0x01]),
    };

    let err = eval_index_program_on_decoded_key(&key, &program)
        .expect_err("missing component must fail closed");

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Index);
}
