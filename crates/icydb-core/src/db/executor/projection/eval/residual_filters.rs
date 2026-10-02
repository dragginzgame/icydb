//! Combined residuals preserve TRUE-only admission and both required slot sets.

use crate::{
    db::{
        executor::projection::eval::eval_effective_runtime_filter_program_with_value_cow_reader,
        predicate::{Predicate, PredicateProgram},
        query::plan::{
            EffectiveRuntimeFilterProgram, exact_metadata_schema,
            expr::{BinaryOp, CompiledExpr},
        },
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

#[test]
fn complete_residual_runtime_cow_conjunction_keeps_truth_slots_and_errors() {
    let schema = exact_metadata_schema(&[], &["age"]);
    let predicate = PredicateProgram::compile_with_schema_info(
        &schema,
        &Predicate::eq("id".into(), Value::Int64(1)),
    );
    let program = EffectiveRuntimeFilterProgram::expression(
        CompiledExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(CompiledExpr::Slot { slot: 1 }),
            right: Box::new(CompiledExpr::Literal(Value::Int64(5))),
        },
        Some(predicate),
    );
    let mut slots = [false; 4];
    program.mark_referenced_slots(&mut slots);
    assert_eq!(slots, [true, true, false, false]);
    for (id, age, expected) in [
        (1, Value::Int64(7), true),
        (1, Value::Int64(2), false),
        (1, Value::Null, false),
        (2, Value::Int64(7), false),
        (2, Value::Null, false),
    ] {
        let values = [Value::Int64(id), age];
        assert_eq!(
            eval_effective_runtime_filter_program_with_value_cow_reader(&program, &mut |slot| {
                values.get(slot).map(Cow::Borrowed)
            },)
            .unwrap(),
            expected,
        );
    }
    // A rejecting conjunct does not read the expression's unavailable slot.
    let nonmatching = Value::Int64(2);
    assert!(
        !eval_effective_runtime_filter_program_with_value_cow_reader(&program, &mut |slot| {
            assert_eq!(slot, 0);
            Some(Cow::Borrowed(&nonmatching))
        },)
        .unwrap()
    );
    // An admitted predicate cannot hide a required expression-reader failure.
    let matching = Value::Int64(1);
    let error =
        eval_effective_runtime_filter_program_with_value_cow_reader(&program, &mut |slot| {
            (slot == 0).then_some(Cow::Borrowed(&matching))
        })
        .unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::query_invalid_logical_plan().diagnostic_code(),
    );
}
