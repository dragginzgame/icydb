//! Owned CASE expansion and pre-allocation accounting contracts.

use super::{
    budget::admit_expansion, guarded_bool_case_branch, normalize_bool_case_expr,
    searched_case_match_guard,
};
use crate::{
    db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            plan::expr::{
                BinaryOp, CaseWhenArm, Expr, FieldPath, Function, UnaryOp,
                eval_builder_expr_for_value_preview,
            },
            preparation::{PreparationWork, with_preparation_work},
        },
    },
    retained::RetainedBytes,
    value::{Value, ValueEnum},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
};

fn root(limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, limit),
    )
}

fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}

fn case(arms: Vec<CaseWhenArm>, otherwise: Expr) -> Expr {
    Expr::Case {
        when_then_arms: arms,
        else_expr: Box::new(otherwise),
    }
}

fn field_pointers(expr: &Expr, name: &str, out: &mut Vec<*const u8>) {
    match expr {
        Expr::Field(field) if field.as_str() == name => out.push(field.as_str().as_ptr()),
        Expr::Binary { left, right, .. } => {
            field_pointers(left, name, out);
            field_pointers(right, name, out);
        }
        Expr::Unary { expr, .. } => field_pointers(expr, name, out),
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                field_pointers(arg, name, out);
            }
        }
        _ => {}
    }
}

#[test]
fn declined_case_keeps_arms_and_else_backing_with_zero_construction_budget() {
    for arms in [
        vec![],
        (0..9)
            .map(|_| CaseWhenArm::new(field("condition"), field("yes")))
            .collect(),
        vec![CaseWhenArm::new(
            field(&"x".repeat(16 * 1024)),
            field("yes"),
        )],
    ] {
        let input = case(arms, field("no"));
        let backing = |expr: &Expr| {
            let Expr::Case {
                when_then_arms,
                else_expr,
            } = expr
            else {
                panic!("CASE");
            };
            (
                when_then_arms.as_ptr(),
                std::ptr::from_ref(else_expr.as_ref()),
            )
        };
        let before = backing(&input);
        let root = root(0);
        let output =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                normalize_bool_case_expr(input, false, work)
            })
            .unwrap();
        assert_eq!(before, backing(&output));
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }
}

#[test]
fn expanded_case_moves_results_else_and_one_condition_occurrence() {
    let condition = field("condition");
    let yes = field("yes");
    let no = field("no");
    let pointer = |expr: &Expr| {
        let Expr::Field(field) = expr else {
            panic!("field");
        };
        field.as_str().as_ptr()
    };
    let before = [pointer(&condition), pointer(&yes), pointer(&no)];
    let output = with_preparation_work(|work| {
        normalize_bool_case_expr(
            case(vec![CaseWhenArm::new(condition, yes)], no),
            false,
            work,
        )
    })
    .unwrap();
    assert!(!matches!(output, Expr::Case { .. }));
    for (index, name) in ["condition", "yes", "no"].into_iter().enumerate() {
        let mut pointers = Vec::new();
        field_pointers(&output, name, &mut pointers);
        assert_eq!(pointers.len(), if index == 0 { 2 } else { 1 });
        assert_eq!(pointers.iter().filter(|&&p| p == before[index]).count(), 1);
    }
}

#[test]
fn condition_copy_allowance_covers_owned_shapes_and_fails_before_clone() {
    let conditions = vec![
        field(&"x".repeat(129)),
        Expr::FieldPath(FieldPath::new(
            "profile",
            vec!["nested".into(), "rank".into()],
        )),
        Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(field("condition")),
        },
        Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(field("x")),
            right: Box::new(Expr::Literal(Value::Text("a".repeat(129)))),
        },
        Expr::FunctionCall {
            function: Function::Coalesce,
            args: vec![field("a"), field("b")],
        },
        case(vec![CaseWhenArm::new(field("a"), field("b"))], field("c")),
        Expr::Aggregate(
            crate::db::query::builder::sum("amount").with_filter_expr(field("condition")),
        ),
        Expr::Alias {
            expr: Box::new(field("aliased")),
            name: "label".into(),
        },
        Expr::Literal(Value::List(vec![
            Value::Map(vec![(Value::Text("key".into()), Value::Blob(vec![1; 257]))]),
            Value::Enum(ValueEnum::test_payload(
                1,
                1,
                Value::Text("enum payload".into()),
            )),
        ])),
    ];
    for condition in conditions {
        let otherwise = field("no");
        let arms = [CaseWhenArm::new(condition, field("yes"))];
        let admitted = with_preparation_work(|work| admit_expansion(&arms, &otherwise, work))
            .unwrap()
            .expect("small bounded operand");
        let copy = admitted.condition_copies[0];
        let generous = root(128 * 1024 * 1024);
        PreparationWork::run(
            &generous.scope(),
            DiagnosticExecutionLane::PublicRead,
            |work| copy.charge(work),
        )
        .unwrap();
        let allowance = generous.observed(Resource::TemporaryBytes);
        let cloned = arms[0].condition().clone();
        let retained = RetainedBytes::measure(&cloned, usize::MAX).unwrap();
        assert!(allowance >= retained as u64, "{allowance} >= {retained}");
        for limit in [allowance - 1, allowance] {
            let root = root(limit);
            let result =
                PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                    copy.charge(work)
                });
            if limit == allowance {
                result.unwrap();
            } else {
                let error = result.expect_err("copy precharge fails before any clone");
                assert!(error.diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw()
                )));
            }
            assert_eq!(root.observed(Resource::TemporaryBytes), allowance);
        }
    }
}

#[test]
fn big_integer_copy_allowance_covers_initialized_limbs_not_retained_capacity() {
    for value in [
        Value::IntBig("9".repeat(90).parse().unwrap()),
        Value::NatBig("9".repeat(90).parse().unwrap()),
    ] {
        let bits = match &value {
            Value::IntBig(value) => value.magnitude_bits(),
            Value::NatBig(value) => value.magnitude_bits(),
            _ => unreachable!(),
        };
        let arms = [CaseWhenArm::new(Expr::Literal(value), field("yes"))];
        let admitted = with_preparation_work(|work| admit_expansion(&arms, &field("no"), work))
            .unwrap()
            .unwrap();
        let root = root(128 * 1024 * 1024);
        PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
            admitted.condition_copies[0].charge(work)
        })
        .unwrap();
        assert!(
            root.observed(Resource::TemporaryBytes)
                >= size_of::<Expr>() as u64 + bits.div_ceil(64) * 8
        );
        assert_eq!(root.observed(Resource::NestedValueSteps), 1);
    }
}

#[test]
fn copy_exhaustion_discards_the_intermediate_and_retries_cannot_reset_charges() {
    let root = root(0);
    let mut observed = 0;
    for _ in 0..2 {
        let input = case(
            vec![CaseWhenArm::new(field("condition"), field("yes"))],
            field("no"),
        );
        let error =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                normalize_bool_case_expr(input, false, work)
            })
            .expect_err("no partially built expansion or compact success");
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw()
        )));
        assert!(root.observed(Resource::TemporaryBytes) > observed);
        observed = root.observed(Resource::TemporaryBytes);
    }
}

#[test]
fn negative_truth_wrapper_precharges_its_new_box() {
    for limit in [0, size_of::<Expr>() as u64] {
        let root = root(limit);
        let input = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(field("flag")),
            right: Box::new(Expr::Literal(Value::Bool(false))),
        };
        let result =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                super::maybe_collapse_truth_wrapper_in_bool_context(
                    input,
                    Some(super::TruthWrapperScope::ScalarWhere),
                    work,
                )
            });
        assert_eq!(result.is_ok(), limit != 0);
        assert_eq!(
            root.observed(Resource::TemporaryBytes),
            size_of::<Expr>() as u64
        );
    }
}

#[test]
fn wrappers_charge_only_new_backing_and_moves_need_none() {
    let bytes = (2 * size_of::<Expr>()) as u64;
    for limit in [bytes - 1, bytes] {
        for coalesce in [false, true] {
            let root = root(limit);
            let result =
                PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                    if coalesce {
                        searched_case_match_guard(field("condition"), work)
                    } else {
                        guarded_bool_case_branch(field("guard"), field("result"), work)
                    }
                });
            assert_eq!(result.is_ok(), limit == bytes);
            assert_eq!(root.observed(Resource::TemporaryBytes), bytes);
        }
    }
    for result in [true, false] {
        let root = root(0);
        PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
            guarded_bool_case_branch(field("guard"), Expr::Literal(Value::Bool(result)), work)
        })
        .unwrap();
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }
}

#[test]
fn expanded_case_preserves_first_match_and_null_results() {
    with_preparation_work(|work| {
        let truth = [Value::Bool(true), Value::Bool(false), Value::Null];
        for first in &truth {
            for second in &truth {
                for yes in &truth {
                    for later in &truth {
                        for otherwise in &truth {
                            let expected = if first == &Value::Bool(true) {
                                yes
                            } else if second == &Value::Bool(true) {
                                later
                            } else {
                                otherwise
                            };
                            let output = normalize_bool_case_expr(
                                case(
                                    vec![
                                        CaseWhenArm::new(
                                            Expr::Literal(first.clone()),
                                            Expr::Literal(yes.clone()),
                                        ),
                                        CaseWhenArm::new(
                                            Expr::Literal(second.clone()),
                                            Expr::Literal(later.clone()),
                                        ),
                                    ],
                                    Expr::Literal(otherwise.clone()),
                                ),
                                false,
                                work,
                            )
                            .unwrap();
                            assert!(!matches!(output, Expr::Case { .. }));
                            assert_eq!(
                                eval_builder_expr_for_value_preview(
                                    &output,
                                    "unused",
                                    &Value::Null
                                )
                                .unwrap(),
                                *expected
                            );
                        }
                    }
                }
            }
        }
    });
}
