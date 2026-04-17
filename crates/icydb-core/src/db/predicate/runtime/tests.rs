use super::{PredicateProgram, eval_compare_scalar_slot, eval_compare_values};
use crate::{
    db::{
        data::{CanonicalSlotReader, ScalarSlotValueRef, ScalarValueRef, SlotReader},
        predicate::{
            CoercionId, CoercionSpec, CompareOp, ComparePredicate, ExecutablePredicate, Predicate,
        },
    },
    error::InternalError,
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel},
    },
    types::{Float32, Principal},
    value::Value,
};
use std::borrow::Cow;

static PREDICATE_FIELDS: [FieldModel; 4] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("score", FieldKind::Int),
    FieldModel::generated("tags", FieldKind::List(&FieldKind::Text)),
    FieldModel::generated("name", FieldKind::Text),
];
static PREDICATE_MODEL: EntityModel = EntityModel::generated(
    "PredicateTestEntity",
    "PredicateTestEntity",
    &PREDICATE_FIELDS[0],
    0,
    &PREDICATE_FIELDS,
    &[],
);

struct PredicateTestSlotReader {
    score: Option<ScalarSlotValueRef<'static>>,
    name: Option<ScalarSlotValueRef<'static>>,
}

impl SlotReader for PredicateTestSlotReader {
    fn model(&self) -> &'static EntityModel {
        &PREDICATE_MODEL
    }

    fn has(&self, slot: usize) -> bool {
        match slot {
            1 => self.score.is_some(),
            3 => self.name.is_some(),
            _ => false,
        }
    }

    fn get_bytes(&self, _slot: usize) -> Option<&[u8]> {
        None
    }

    fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        Ok(match slot {
            1 => self.score,
            3 => self.name,
            _ => None,
        })
    }

    crate::db::data::impl_scalar_only_test_slot_reader_get_value!();
}

impl CanonicalSlotReader for PredicateTestSlotReader {}

#[test]
fn scalar_compare_fast_path_matches_value_semantics_for_strict_int_and_text() {
    let strict = CoercionSpec::new(CoercionId::Strict);
    let int_actual = ScalarSlotValueRef::Value(ScalarValueRef::Int(7));
    let text_actual = ScalarSlotValueRef::Value(ScalarValueRef::Text("Alpha"));

    let int_cases = [
        (CompareOp::Eq, Value::Int(7)),
        (CompareOp::Ne, Value::Int(8)),
        (CompareOp::Gt, Value::Int(3)),
        (
            CompareOp::In,
            Value::List(vec![Value::Int(1), Value::Int(7)]),
        ),
        (
            CompareOp::NotIn,
            Value::List(vec![Value::Int(1), Value::Int(2)]),
        ),
    ];
    for (op, expected) in int_cases {
        let direct = eval_compare_scalar_slot(int_actual, op, &expected, &strict);
        let generic = eval_compare_values(&Value::Int(7), op, &expected, &strict);

        assert_eq!(direct, Some(generic), "int fast path diverged for {op:?}");
    }

    let text_cases = [
        (CompareOp::Eq, Value::Text("Alpha".to_string())),
        (CompareOp::StartsWith, Value::Text("Al".to_string())),
        (
            CompareOp::In,
            Value::List(vec![
                Value::Text("Beta".to_string()),
                Value::Text("Alpha".to_string()),
            ]),
        ),
        (CompareOp::Contains, Value::Text("ph".to_string())),
    ];
    for (op, expected) in text_cases {
        let direct = eval_compare_scalar_slot(text_actual, op, &expected, &strict);
        let generic =
            eval_compare_values(&Value::Text("Alpha".to_string()), op, &expected, &strict);

        assert_eq!(direct, Some(generic), "text fast path diverged for {op:?}");
    }
}

#[test]
fn scalar_compare_fast_path_falls_back_for_numeric_widen() {
    let numeric = CoercionSpec::new(CoercionId::NumericWiden);
    let actual = ScalarSlotValueRef::Value(ScalarValueRef::Float32(
        Float32::try_new(7.0).expect("finite float should build"),
    ));

    let direct = eval_compare_scalar_slot(actual, CompareOp::Eq, &Value::Int(7), &numeric);

    assert_eq!(
        direct, None,
        "numeric widen should stay on fallback for now"
    );
}

#[test]
fn scalar_compare_fast_path_preserves_strict_variant_mismatch_false() {
    let strict = CoercionSpec::new(CoercionId::Strict);
    let actual = ScalarSlotValueRef::Value(ScalarValueRef::Principal(Principal::anonymous()));

    let eq = eval_compare_scalar_slot(
        actual,
        CompareOp::Eq,
        &Value::Text("x".to_string()),
        &strict,
    );
    let ne = eval_compare_scalar_slot(
        actual,
        CompareOp::Ne,
        &Value::Text("x".to_string()),
        &strict,
    );

    assert_eq!(eq, Some(false));
    assert_eq!(ne, Some(false));
}

#[test]
fn predicate_program_dispatches_scalar_only_predicates_once() {
    let scalar_predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate {
            field: "score".to_string(),
            op: CompareOp::Gt,
            value: Value::Int(10),
            coercion: CoercionSpec::new(CoercionId::Strict),
        }),
        Predicate::Not(Box::new(Predicate::Compare(ComparePredicate {
            field: "score".to_string(),
            op: CompareOp::In,
            value: Value::List(vec![Value::Int(1), Value::Int(2)]),
            coercion: CoercionSpec::new(CoercionId::Strict),
        }))),
    ]);
    let generic_predicate = Predicate::Compare(ComparePredicate {
        field: "tags".to_string(),
        op: CompareOp::Contains,
        value: Value::Text("x".to_string()),
        coercion: CoercionSpec::new(CoercionId::CollectionElement),
    });

    let scalar_program = PredicateProgram::compile(&PREDICATE_MODEL, &scalar_predicate);
    let generic_program = PredicateProgram::compile(&PREDICATE_MODEL, &generic_predicate);

    assert!(scalar_program.uses_scalar_program());
    assert!(!generic_program.uses_scalar_program());
}

#[test]
fn predicate_program_dispatches_scalar_field_to_field_compare_once() {
    let predicate =
        Predicate::CompareFields(crate::db::predicate::CompareFieldsPredicate::with_coercion(
            "score",
            CompareOp::Eq,
            "score",
            CoercionId::NumericWiden,
        ));
    let program = PredicateProgram::compile(&PREDICATE_MODEL, &predicate);
    let slots = PredicateTestSlotReader {
        score: Some(ScalarSlotValueRef::Value(ScalarValueRef::Int(7))),
        name: None,
    };

    assert!(
        program.uses_scalar_program(),
        "field-to-field scalar compares should stay on the scalar slot seam once both operands are scalar slots",
    );
    assert!(
        program
            .eval_with_structural_slot_reader(&slots)
            .expect("scalar field-to-field predicate should evaluate"),
        "same-slot scalar field compare should evaluate true under the scalar fast path",
    );
}

#[test]
fn scalar_predicate_program_reuses_canonical_executable_tree() {
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate {
            field: "score".to_string(),
            op: CompareOp::Eq,
            value: Value::Int(10),
            coercion: CoercionSpec::new(CoercionId::Strict),
        }),
        Predicate::Compare(ComparePredicate {
            field: "score".to_string(),
            op: CompareOp::In,
            value: Value::List(vec![Value::Int(1), Value::Int(2)]),
            coercion: CoercionSpec::new(CoercionId::Strict),
        }),
    ]);

    let program = PredicateProgram::compile(&PREDICATE_MODEL, &predicate);
    let ExecutablePredicate::And(children) = program.executable() else {
        panic!("expected executable and-predicate");
    };

    let ExecutablePredicate::Compare(eq) = &children[0] else {
        panic!("expected eq compare");
    };
    let ExecutablePredicate::Compare(in_list) = &children[1] else {
        panic!("expected in-list compare");
    };

    assert_eq!(
        eq.right_literal(),
        Some(&Value::Int(10)),
        "compiled literal compare should keep the literal on the right operand",
    );
    assert_eq!(
        in_list.right_literal(),
        Some(&Value::List(vec![Value::Int(1), Value::Int(2)])),
        "compiled list compare should keep the list literal on the right operand",
    );
}

#[test]
fn scalar_predicate_fast_path_preserves_null_and_variant_mismatch_semantics() {
    let strict = CoercionSpec::new(CoercionId::Strict);

    let null_eq = eval_compare_scalar_slot(
        ScalarSlotValueRef::Null,
        CompareOp::Eq,
        &Value::Null,
        &strict,
    );
    let null_in = eval_compare_scalar_slot(
        ScalarSlotValueRef::Null,
        CompareOp::In,
        &Value::List(vec![Value::Null]),
        &strict,
    );
    let mismatch = eval_compare_scalar_slot(
        ScalarSlotValueRef::Value(ScalarValueRef::Int(7)),
        CompareOp::Eq,
        &Value::Text("x".to_string()),
        &strict,
    );

    assert_eq!(null_eq, Some(true));
    assert_eq!(null_in, Some(true));
    assert_eq!(mismatch, Some(false));
}

#[test]
fn scalar_predicate_not_bool_equality_treats_null_and_missing_as_not_true_or_false() {
    let is_not_true = Predicate::not(Predicate::Compare(ComparePredicate::with_coercion(
        "score",
        CompareOp::Eq,
        Value::Bool(true),
        CoercionId::Strict,
    )));
    let is_not_false = Predicate::not(Predicate::Compare(ComparePredicate::with_coercion(
        "score",
        CompareOp::Eq,
        Value::Bool(false),
        CoercionId::Strict,
    )));
    let is_not_true_program = PredicateProgram::compile(&PREDICATE_MODEL, &is_not_true);
    let is_not_false_program = PredicateProgram::compile(&PREDICATE_MODEL, &is_not_false);
    let null_slots = PredicateTestSlotReader {
        score: Some(ScalarSlotValueRef::Null),
        name: None,
    };

    assert!(
        is_not_true_program
            .eval_with_structural_slot_reader(&null_slots)
            .expect("IS NOT TRUE should evaluate against null scalar slots"),
        "IS NOT TRUE should treat null as not true under the current predicate runtime",
    );
    assert!(
        is_not_false_program
            .eval_with_structural_slot_reader(&null_slots)
            .expect("IS NOT FALSE should evaluate against null scalar slots"),
        "IS NOT FALSE should treat null as not false under the current predicate runtime",
    );
    assert!(
        is_not_true_program.eval_with_slot_value_ref_reader(&mut |_| None),
        "IS NOT TRUE should treat missing values as not true under the current predicate runtime",
    );
    assert!(
        is_not_false_program.eval_with_slot_value_ref_reader(&mut |_| None),
        "IS NOT FALSE should treat missing values as not false under the current predicate runtime",
    );
}

#[test]
fn scalar_predicate_fast_path_matches_text_prefix_suffix_semantics() {
    let strict = CoercionSpec::new(CoercionId::Strict);
    let casefold = CoercionSpec::new(CoercionId::TextCasefold);
    let actual = ScalarSlotValueRef::Value(ScalarValueRef::Text("Alpha"));

    let strict_prefix = eval_compare_scalar_slot(
        actual,
        CompareOp::StartsWith,
        &Value::Text("Al".to_string()),
        &strict,
    );
    let ci_suffix = eval_compare_scalar_slot(
        actual,
        CompareOp::EndsWith,
        &Value::Text("HA".to_string()),
        &casefold,
    );

    assert_eq!(strict_prefix, Some(true));
    assert_eq!(ci_suffix, Some(true));
}

#[test]
fn scalar_predicate_program_handles_scalar_non_compare_nodes() {
    let predicate = Predicate::And(vec![
        Predicate::IsNotNull {
            field: "score".to_string(),
        },
        Predicate::IsMissing {
            field: "missing".to_string(),
        },
        Predicate::IsNotEmpty {
            field: "name".to_string(),
        },
        Predicate::TextContainsCi {
            field: "name".to_string(),
            value: Value::Text("alp".to_string()),
        },
    ]);
    let program = PredicateProgram::compile(&PREDICATE_MODEL, &predicate);
    let slots = PredicateTestSlotReader {
        score: Some(ScalarSlotValueRef::Value(ScalarValueRef::Int(7))),
        name: Some(ScalarSlotValueRef::Value(ScalarValueRef::Text("Alpha"))),
    };

    assert!(program.uses_scalar_program());
    assert!(
        program
            .eval_with_structural_slot_reader(&slots)
            .expect("scalar non-compare predicate should evaluate")
    );
}

#[test]
fn predicate_program_accepts_mixed_cow_slot_readers() {
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate {
            field: "score".to_string(),
            op: CompareOp::Gt,
            value: Value::Int(5),
            coercion: CoercionSpec::new(CoercionId::Strict),
        }),
        Predicate::TextContainsCi {
            field: "name".to_string(),
            value: Value::Text("alp".to_string()),
        },
    ]);
    let program = PredicateProgram::compile(&PREDICATE_MODEL, &predicate);
    let borrowed_score = Value::Int(7);

    let mut read_slot = |slot| match slot {
        1 => Some(Cow::Borrowed(&borrowed_score)),
        3 => Some(Cow::Owned(Value::Text("Alpha".to_string()))),
        _ => None,
    };

    assert!(program.eval_with_slot_value_cow_reader(&mut read_slot));
}

#[test]
fn scalar_predicate_program_compiles_text_prefix_suffix_compares() {
    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate {
            field: "name".to_string(),
            op: CompareOp::StartsWith,
            value: Value::Text("Al".to_string()),
            coercion: CoercionSpec::new(CoercionId::Strict),
        }),
        Predicate::Compare(ComparePredicate {
            field: "name".to_string(),
            op: CompareOp::EndsWith,
            value: Value::Text("HA".to_string()),
            coercion: CoercionSpec::new(CoercionId::TextCasefold),
        }),
    ]);
    let program = PredicateProgram::compile(&PREDICATE_MODEL, &predicate);
    let slots = PredicateTestSlotReader {
        score: None,
        name: Some(ScalarSlotValueRef::Value(ScalarValueRef::Text("Alpha"))),
    };

    assert!(program.uses_scalar_program());
    assert!(
        program
            .eval_with_structural_slot_reader(&slots)
            .expect("scalar text prefix/suffix predicate should evaluate")
    );
}

#[test]
fn scalar_predicate_program_audit_covers_expected_scalar_shapes() {
    let scalar_predicates = [
        Predicate::Compare(ComparePredicate {
            field: "score".to_string(),
            op: CompareOp::Eq,
            value: Value::Int(7),
            coercion: CoercionSpec::new(CoercionId::Strict),
        }),
        Predicate::Compare(ComparePredicate {
            field: "score".to_string(),
            op: CompareOp::In,
            value: Value::List(vec![Value::Int(1), Value::Int(7)]),
            coercion: CoercionSpec::new(CoercionId::Strict),
        }),
        Predicate::Compare(ComparePredicate {
            field: "name".to_string(),
            op: CompareOp::StartsWith,
            value: Value::Text("Al".to_string()),
            coercion: CoercionSpec::new(CoercionId::Strict),
        }),
        Predicate::Compare(ComparePredicate {
            field: "name".to_string(),
            op: CompareOp::EndsWith,
            value: Value::Text("HA".to_string()),
            coercion: CoercionSpec::new(CoercionId::TextCasefold),
        }),
        Predicate::IsNull {
            field: "score".to_string(),
        },
        Predicate::IsNotNull {
            field: "score".to_string(),
        },
        Predicate::IsMissing {
            field: "score".to_string(),
        },
        Predicate::IsMissing {
            field: "missing".to_string(),
        },
        Predicate::IsEmpty {
            field: "name".to_string(),
        },
        Predicate::IsNotEmpty {
            field: "name".to_string(),
        },
        Predicate::TextContains {
            field: "name".to_string(),
            value: Value::Text("lp".to_string()),
        },
        Predicate::TextContainsCi {
            field: "name".to_string(),
            value: Value::Text("LP".to_string()),
        },
        Predicate::And(vec![
            Predicate::IsNotNull {
                field: "score".to_string(),
            },
            Predicate::TextContainsCi {
                field: "name".to_string(),
                value: Value::Text("LP".to_string()),
            },
        ]),
        Predicate::Or(vec![
            Predicate::Compare(ComparePredicate {
                field: "score".to_string(),
                op: CompareOp::Eq,
                value: Value::Int(1),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            Predicate::Compare(ComparePredicate {
                field: "score".to_string(),
                op: CompareOp::Eq,
                value: Value::Int(2),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
        ]),
        Predicate::Not(Box::new(Predicate::IsEmpty {
            field: "name".to_string(),
        })),
    ];

    for predicate in scalar_predicates {
        let program = PredicateProgram::compile(&PREDICATE_MODEL, &predicate);
        assert!(
            program.uses_scalar_program(),
            "expected scalar program for predicate: {predicate:?}"
        );
    }
}

#[test]
fn scalar_predicate_program_audit_preserves_expected_generic_shapes() {
    let generic_predicates = [
        Predicate::Compare(ComparePredicate {
            field: "score".to_string(),
            op: CompareOp::Eq,
            value: Value::Float32(Float32::try_new(7.0).expect("finite float should build")),
            coercion: CoercionSpec::new(CoercionId::NumericWiden),
        }),
        Predicate::Compare(ComparePredicate {
            field: "tags".to_string(),
            op: CompareOp::Contains,
            value: Value::Text("x".to_string()),
            coercion: CoercionSpec::new(CoercionId::CollectionElement),
        }),
        Predicate::IsEmpty {
            field: "tags".to_string(),
        },
        Predicate::TextContains {
            field: "name".to_string(),
            value: Value::Int(1),
        },
        Predicate::And(vec![
            Predicate::Compare(ComparePredicate {
                field: "score".to_string(),
                op: CompareOp::Eq,
                value: Value::Int(7),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            Predicate::Compare(ComparePredicate {
                field: "score".to_string(),
                op: CompareOp::Eq,
                value: Value::Float32(Float32::try_new(7.0).expect("finite float should build")),
                coercion: CoercionSpec::new(CoercionId::NumericWiden),
            }),
        ]),
    ];

    for predicate in generic_predicates {
        let program = PredicateProgram::compile(&PREDICATE_MODEL, &predicate);
        assert!(
            !program.uses_scalar_program(),
            "expected generic program for predicate: {predicate:?}"
        );
    }
}
