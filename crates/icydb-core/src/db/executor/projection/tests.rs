//! Module: db::executor::projection::tests
//! Responsibility: module-local ownership and contracts for db::executor::projection::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(feature = "sql")]
use crate::db::response::ProjectedRow;
use crate::{
    db::query::{
        builder::aggregate::{count, sum},
        plan::{
            FieldSlot, GroupedAggregateExecutionSpec, GroupedAggregateProjectionSpec,
            expr::{Alias, BinaryOp, Expr, FieldId, ProjectionField, ProjectionSpec},
        },
    },
    db::{
        codec::serialize_row_payload,
        data::{
            CanonicalSlotReader, RawRow, SlotReader, StructuralSlotReader,
            encode_persisted_scalar_slot_payload,
        },
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::{field::FieldKind, index::IndexModel},
    serialize::serialize,
    traits::{EntitySchema, EntityValue, FieldProjection as _},
    types::Ulid,
    value::Value,
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[cfg(feature = "sql")]
use super::project_rows_from_projection;
use super::{
    GroupedRowView, compile_scalar_projection_expr, eval_canonical_scalar_projection_expr,
    eval_expr_grouped, eval_expr_with_required_value_reader, eval_expr_with_slot_reader,
    eval_scalar_projection_expr, evaluate_grouped_projection_values,
};

const EMPTY_INDEX_FIELDS: [&str; 0] = [];
const EMPTY_INDEX: IndexModel = IndexModel::new(
    "query::executor::projection::idx_empty",
    "query::executor::projection::Store",
    &EMPTY_INDEX_FIELDS,
    false,
);

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct ProjectionEvalEntity {
    id: Ulid,
    rank: i64,
    flag: bool,
    label: String,
}

crate::test_canister! {
    ident = ProjectionEvalCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = ProjectionEvalStore,
    canister = ProjectionEvalCanister,
}

crate::test_entity_schema! {
    ident = ProjectionEvalEntity,
    id = Ulid,
    id_field = id,
    entity_name = "ProjectionEvalEntity",
    entity_tag = crate::testing::PROJECTION_EVAL_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("rank", FieldKind::Int),
        ("flag", FieldKind::Bool),
        ("label", FieldKind::Text),
    ],
    indexes = [&EMPTY_INDEX],
    store = ProjectionEvalStore,
    canister = ProjectionEvalCanister,
}

fn row(
    id: u128,
    rank: i64,
    flag: bool,
) -> (crate::types::Id<ProjectionEvalEntity>, ProjectionEvalEntity) {
    let entity = ProjectionEvalEntity {
        id: Ulid::from_u128(id),
        rank,
        flag,
        label: format!("label-{id}"),
    };

    (entity.id(), entity)
}

fn eval_expr_for_row(
    expr: &Expr,
    row: &ProjectionEvalEntity,
) -> Result<Value, crate::db::executor::projection::ProjectionEvalError> {
    eval_expr_with_slot_reader(expr, ProjectionEvalEntity::MODEL, &mut |slot| {
        row.get_value_by_index(slot)
    })
}

fn eval_scalar_expr_for_row(
    expr: &Expr,
    row: &ProjectionEvalEntity,
) -> Result<Value, crate::db::executor::projection::ScalarProjectionEvalError> {
    let compiled = compile_scalar_projection_expr(ProjectionEvalEntity::MODEL, expr)
        .expect("expression should compile onto scalar projection seam");
    let raw_row = RawRow::from_entity(row).expect("persisted row should encode");
    let row_fields = StructuralSlotReader::from_raw_row(&raw_row, ProjectionEvalEntity::MODEL)
        .expect("persisted row should decode structurally");

    eval_scalar_projection_expr(&compiled, &row_fields)
}

fn grouped_execution_specs<const N: usize>(
    aggregate_exprs: [crate::db::query::builder::aggregate::AggregateExpr; N],
) -> [GroupedAggregateExecutionSpec; N] {
    aggregate_exprs.map(|aggregate_expr| {
        GroupedAggregateExecutionSpec::from_projection_spec_with_model(
            ProjectionEvalEntity::MODEL,
            &GroupedAggregateProjectionSpec::from_aggregate_expr(&aggregate_expr),
        )
        .expect("grouped execution spec should lower from aggregate expression")
    })
}

///
/// ProjectionMissingDeclaredSlotReader
///
/// ProjectionMissingDeclaredSlotReader
///
/// ProjectionMissingDeclaredSlotReader simulates one canonical structural row
/// whose declared slots are absent so projection evaluators can prove they
/// preserve corruption diagnostics instead of flattening them into
/// invalid-logical-plan failures.
///

struct ProjectionMissingDeclaredSlotReader;

impl SlotReader for ProjectionMissingDeclaredSlotReader {
    fn model(&self) -> &'static crate::model::entity::EntityModel {
        ProjectionEvalEntity::MODEL
    }

    fn has(&self, _slot: usize) -> bool {
        false
    }

    fn get_bytes(&self, _slot: usize) -> Option<&[u8]> {
        None
    }

    fn get_scalar(
        &self,
        _slot: usize,
    ) -> Result<Option<crate::db::data::ScalarSlotValueRef<'_>>, InternalError> {
        Ok(None)
    }

    fn get_value(&mut self, _slot: usize) -> Result<Option<Value>, InternalError> {
        panic!("projection missing-slot test reader should not route through get_value")
    }
}

impl CanonicalSlotReader for ProjectionMissingDeclaredSlotReader {}

#[test]
fn eval_expr_supports_arithmetic_projection() {
    let (_, entity) = row(1, 7, true);
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Int(1))),
    };

    let value =
        eval_expr_for_row(&expr, &entity).expect("numeric projection expression should evaluate");

    assert_eq!(
        value.cmp_numeric(&Value::Int(8)),
        Some(Ordering::Equal),
        "arithmetic projection must preserve numeric semantics",
    );
}

#[test]
fn scalar_projection_expr_matches_generic_eval_for_arithmetic_projection() {
    let (_, entity) = row(7, 41, true);
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Int(1))),
    };

    let generic_value =
        eval_expr_for_row(&expr, &entity).expect("generic arithmetic projection should evaluate");
    let scalar_value = eval_scalar_expr_for_row(&expr, &entity)
        .expect("scalar arithmetic projection should evaluate");

    assert_eq!(
        generic_value.cmp_numeric(&scalar_value),
        Some(Ordering::Equal),
        "compiled scalar projection should preserve arithmetic projection semantics",
    );
}

#[test]
fn required_projection_eval_preserves_internal_slot_errors() {
    let expr = Expr::Field(FieldId::new("rank"));
    let err = eval_expr_with_required_value_reader(&expr, ProjectionEvalEntity::MODEL, &mut |_| {
        Err(InternalError::persisted_row_declared_field_missing("rank"))
    })
    .expect_err("required projection evaluation should preserve structural slot errors");

    assert_eq!(err.class(), ErrorClass::Corruption);
    assert_eq!(err.origin(), ErrorOrigin::Serialize);
}

#[test]
fn canonical_scalar_projection_preserves_missing_declared_slot_corruption() {
    let expr = Expr::Field(FieldId::new("rank"));
    let compiled = compile_scalar_projection_expr(ProjectionEvalEntity::MODEL, &expr)
        .expect("rank field should compile onto scalar seam");
    let err =
        eval_canonical_scalar_projection_expr(&compiled, &ProjectionMissingDeclaredSlotReader)
            .expect_err("canonical scalar projection should fail closed on missing declared slot");

    assert_eq!(err.class(), ErrorClass::Corruption);
    assert_eq!(err.origin(), ErrorOrigin::Serialize);
}

#[test]
fn structural_row_boundary_rejects_malformed_unprojected_scalar_slot_before_projection() {
    let (_, entity) = row(77, 9, true);
    let _compiled = compile_scalar_projection_expr(
        ProjectionEvalEntity::MODEL,
        &Expr::Field(FieldId::new("rank")),
    )
    .expect("rank field should compile onto scalar seam");
    let id_bytes =
        encode_persisted_scalar_slot_payload(&entity.id, "id").expect("id payload should encode");
    let rank_bytes = encode_persisted_scalar_slot_payload(&entity.rank, "rank")
        .expect("rank payload should encode");
    let flag_bytes = encode_persisted_scalar_slot_payload(&entity.flag, "flag")
        .expect("flag payload should encode");
    let raw_label = serialize(&entity.label).expect("raw scalar label should encode");
    let slot_payloads = [
        id_bytes.as_slice(),
        rank_bytes.as_slice(),
        flag_bytes.as_slice(),
        raw_label.as_slice(),
    ];
    let mut payload = Vec::new();
    let mut offset = 0_u32;

    // Build one row whose projected `rank` slot remains canonical while an
    // unrelated scalar slot intentionally bypasses the `0xFF` envelope.
    payload.extend_from_slice(&4_u16.to_be_bytes());
    for bytes in slot_payloads {
        let len = u32::try_from(bytes.len()).expect("slot length should fit u32");
        payload.extend_from_slice(&offset.to_be_bytes());
        payload.extend_from_slice(&len.to_be_bytes());
        offset = offset.saturating_add(len);
    }
    for bytes in slot_payloads {
        payload.extend_from_slice(bytes);
    }
    let raw_row = RawRow::try_new(serialize_row_payload(payload).expect("serialize row payload"))
        .expect("build raw row");

    let Err(err) = StructuralSlotReader::from_raw_row(&raw_row, ProjectionEvalEntity::MODEL) else {
        panic!("structural read boundary must reject malformed unprojected scalar slots");
    };

    assert_eq!(err.class(), ErrorClass::Corruption);
    assert_eq!(err.origin(), ErrorOrigin::Serialize);
    assert!(
        err.message.contains("field 'label'"),
        "unexpected error: {err:?}"
    );
    assert!(
        err.message
            .contains("expected slot envelope prefix byte 0xFF"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn eval_expr_supports_boolean_projection() {
    let (_, entity) = row(2, 3, true);
    let expr = Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(Expr::Field(FieldId::new("flag"))),
        right: Box::new(Expr::Literal(Value::Bool(true))),
    };

    let value =
        eval_expr_for_row(&expr, &entity).expect("boolean projection expression should evaluate");

    assert_eq!(value, Value::Bool(true));
}

#[test]
fn eval_expr_supports_numeric_equality_widening() {
    let (_, entity) = row(12, 7, true);
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Uint(7))),
    };

    let value = eval_expr_for_row(&expr, &entity).expect("numeric equality should widen");

    assert_eq!(value, Value::Bool(true));
}

#[test]
fn eval_expr_rejects_numeric_and_non_numeric_equality_mix() {
    let (_, entity) = row(13, 7, true);
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Field(FieldId::new("label"))),
    };

    let err = eval_expr_for_row(&expr, &entity)
        .expect_err("mixed numeric/non-numeric equality should fail invariant checks");
    assert!(matches!(
        err,
        crate::db::executor::projection::ProjectionEvalError::InvalidBinaryOperands { op, .. }
            if op == "eq"
    ));
}

#[test]
fn eval_expr_propagates_null_values() {
    let (_, entity) = row(3, 5, false);
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Null)),
    };

    let value =
        eval_expr_for_row(&expr, &entity).expect("null propagation should remain deterministic");

    assert_eq!(value, Value::Null);
}

#[test]
fn eval_expr_alias_wrapper_is_semantic_no_op() {
    let (_, entity) = row(4, 11, true);
    let plain = Expr::Field(FieldId::new("rank"));
    let aliased = Expr::Alias {
        expr: Box::new(Expr::Field(FieldId::new("rank"))),
        name: Alias::new("rank_alias"),
    };

    let plain_value =
        eval_expr_for_row(&plain, &entity).expect("plain field expression should evaluate");
    let alias_value = eval_expr_for_row(&aliased, &entity)
        .expect("aliased expression should evaluate identically");

    assert_eq!(plain_value, alias_value);
}

#[cfg(feature = "sql")]
#[test]
fn projection_hash_alias_identity_matches_evaluated_projection_output() {
    let row = row(5, 42, true);
    let base_projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Field(FieldId::new("rank")),
        alias: None,
    }]);
    let aliased_projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Alias {
            expr: Box::new(Expr::Field(FieldId::new("rank"))),
            name: Alias::new("rank_expr"),
        },
        alias: Some(Alias::new("rank_out")),
    }]);

    let base_rows: Vec<ProjectedRow<ProjectionEvalEntity>> =
        project_rows_from_projection(&base_projection, std::slice::from_ref(&row))
            .expect("base projection should evaluate");
    let aliased_rows: Vec<ProjectedRow<ProjectionEvalEntity>> =
        project_rows_from_projection(&aliased_projection, std::slice::from_ref(&row))
            .expect("aliased projection should evaluate");

    assert_eq!(
        base_projection.structural_hash_for_test(),
        aliased_projection.structural_hash_for_test(),
        "alias-insensitive projection hash must align with evaluator output identity",
    );
    assert_eq!(
        base_rows[0].values(),
        aliased_rows[0].values(),
        "alias wrappers must not affect evaluated projection values",
    );
    assert_eq!(
        base_rows[0].id(),
        aliased_rows[0].id(),
        "projection identity checks must preserve source row identity",
    );
}

#[cfg(feature = "sql")]
#[test]
fn projection_field_order_preserved_for_multi_field_selection() {
    let rows = [row(51, 7, true), row(52, 9, false)];
    let projection = ProjectionSpec::from_fields_for_test(vec![
        ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("label")),
            alias: None,
        },
        ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("rank")),
            alias: None,
        },
        ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("flag")),
            alias: None,
        },
    ]);

    let projected = project_rows_from_projection(&projection, rows.as_slice())
        .expect("multi-field projection should evaluate");

    assert_eq!(
        projected[0].values(),
        &[
            Value::Text("label-51".to_string()),
            Value::Int(7),
            Value::Bool(true),
        ],
        "projection values must preserve declaration order for the first row",
    );
    assert_eq!(
        projected[1].values(),
        &[
            Value::Text("label-52".to_string()),
            Value::Int(9),
            Value::Bool(false),
        ],
        "projection values must preserve declaration order for the second row",
    );
}

#[cfg(feature = "sql")]
#[test]
fn scalar_arithmetic_projection_returns_computed_values() {
    let rows = [row(7, 41, true)];
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: None,
    }]);

    let projected = project_rows_from_projection(&projection, rows.as_slice())
        .expect("arithmetic scalar projection should evaluate");
    let only_value = projected[0]
        .values()
        .first()
        .expect("projection should emit one value");
    assert_eq!(
        only_value.cmp_numeric(&Value::Int(42)),
        Some(Ordering::Equal),
        "arithmetic scalar projection should emit computed expression result",
    );
}

#[cfg(feature = "sql")]
#[test]
fn ordering_is_preserved_when_projecting_computed_fields() {
    // Input rows are already in execution order; projection must preserve that
    // row ordering while evaluating computed scalar expressions.
    let rows = [row(8, 1, true), row(9, 2, true), row(10, 3, true)];
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(100))),
        },
        alias: None,
    }]);

    let projected = project_rows_from_projection(&projection, rows.as_slice())
        .expect("computed projection should evaluate deterministically");

    let projected_ids: Vec<_> = projected.iter().map(ProjectedRow::id).collect();
    let expected_ids: Vec<_> = rows.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        projected_ids, expected_ids,
        "projection phase must preserve established row ordering",
    );
    let expected_values = [Value::Int(101), Value::Int(102), Value::Int(103)];
    for (actual, expected) in projected
        .iter()
        .map(|row| row.values()[0].clone())
        .zip(expected_values)
    {
        assert_eq!(
            actual.cmp_numeric(&expected),
            Some(Ordering::Equal),
            "computed projection values must align with preserved row order",
        );
    }
}

#[test]
fn grouped_projection_arithmetic_over_group_field_evaluates() {
    let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
    let aggregate_execution_specs: [GroupedAggregateExecutionSpec; 0] = [];
    let grouped_row = GroupedRowView::new(
        &[Value::Int(7)],
        &[],
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    );
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Int(2))),
    };

    let value = eval_expr_grouped(&expr, &grouped_row).expect("grouped arithmetic should evaluate");
    assert_eq!(
        value.cmp_numeric(&Value::Int(9)),
        Some(Ordering::Equal),
        "grouped arithmetic projection should evaluate over grouped keys",
    );
}

#[test]
fn grouped_projection_supports_numeric_equality_widening() {
    let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
    let aggregate_execution_specs: [GroupedAggregateExecutionSpec; 0] = [];
    let grouped_row = GroupedRowView::new(
        &[Value::Int(7)],
        &[],
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    );
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Uint(7))),
    };

    let value = eval_expr_grouped(&expr, &grouped_row)
        .expect("grouped numeric equality should widen deterministically");
    assert_eq!(value, Value::Bool(true));
}

#[test]
fn grouped_projection_rejects_numeric_and_non_numeric_equality_mix() {
    let group_fields = [
        FieldSlot::from_parts_for_test(1, "rank"),
        FieldSlot::from_parts_for_test(2, "label"),
    ];
    let aggregate_execution_specs: [GroupedAggregateExecutionSpec; 0] = [];
    let key_values = [Value::Int(7), Value::Text("label-7".to_string())];
    let grouped_row = GroupedRowView::new(
        key_values.as_slice(),
        &[],
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    );
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Field(FieldId::new("label"))),
    };

    let err = eval_expr_grouped(&expr, &grouped_row)
        .expect_err("grouped mixed numeric/non-numeric equality should fail");
    assert!(matches!(
        err,
        crate::db::executor::projection::ProjectionEvalError::InvalidBinaryOperands { op, .. }
            if op == "eq"
    ));
}

#[test]
fn grouped_projection_mixing_aggregate_and_arithmetic_evaluates() {
    let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
    let aggregate_execution_specs = grouped_execution_specs([sum("rank")]);
    let grouped_row = GroupedRowView::new(
        &[Value::Int(7)],
        &[Value::Int(40)],
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    );
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Aggregate(sum("rank"))),
        right: Box::new(Expr::Literal(Value::Int(2))),
    };

    let value = eval_expr_grouped(&expr, &grouped_row)
        .expect("grouped aggregate arithmetic projection should evaluate");
    assert_eq!(
        value.cmp_numeric(&Value::Int(42)),
        Some(Ordering::Equal),
        "grouped projections must evaluate aggregate+scalar arithmetic deterministically",
    );
}

#[test]
fn grouped_projection_alias_wrapping_is_semantic_no_op() {
    let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
    let aggregate_execution_specs = grouped_execution_specs([sum("rank")]);
    let grouped_row = GroupedRowView::new(
        &[Value::Int(7)],
        &[Value::Int(40)],
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    );
    let plain = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Aggregate(sum("rank"))),
        right: Box::new(Expr::Literal(Value::Int(2))),
    };
    let aliased = Expr::Alias {
        expr: Box::new(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Aggregate(sum("rank"))),
            right: Box::new(Expr::Literal(Value::Int(2))),
        }),
        name: Alias::new("sum_plus_two"),
    };

    let plain_value =
        eval_expr_grouped(&plain, &grouped_row).expect("plain grouped expression should work");
    let alias_value =
        eval_expr_grouped(&aliased, &grouped_row).expect("aliased grouped expression should work");
    assert_eq!(
        plain_value, alias_value,
        "grouped alias wrapping must not change expression values",
    );
}

#[test]
fn grouped_projection_column_order_is_stable() {
    let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
    let aggregate_execution_specs = grouped_execution_specs([count(), sum("rank")]);
    let grouped_row = GroupedRowView::new(
        &[Value::Int(7)],
        &[Value::Uint(3), Value::Int(40)],
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    );
    let projection = ProjectionSpec::from_fields_for_test(vec![
        ProjectionField::Scalar {
            expr: Expr::Aggregate(sum("rank")),
            alias: Some(Alias::new("sum_rank")),
        },
        ProjectionField::Scalar {
            expr: Expr::Aggregate(count()),
            alias: Some(Alias::new("count_all")),
        },
        ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Aggregate(count())),
                right: Box::new(Expr::Literal(Value::Int(1))),
            },
            alias: Some(Alias::new("count_plus_one")),
        },
    ]);

    let values = evaluate_grouped_projection_values(&projection, &grouped_row)
        .expect("grouped projection vector should evaluate");

    assert_eq!(
        values.len(),
        3,
        "grouped projection must preserve declared field count",
    );
    assert_eq!(
        values[0].cmp_numeric(&Value::Int(40)),
        Some(Ordering::Equal),
        "first grouped projection output must follow projection declaration order",
    );
    assert_eq!(
        values[1].cmp_numeric(&Value::Uint(3)),
        Some(Ordering::Equal),
        "second grouped projection output must follow projection declaration order",
    );
    assert_eq!(
        values[2].cmp_numeric(&Value::Int(4)),
        Some(Ordering::Equal),
        "third grouped projection output must evaluate computed aggregate expression in order",
    );
}

#[cfg(feature = "sql")]
#[test]
fn expression_projection_column_identity_is_deterministic() {
    let rows = [row(53, 7, true)];
    let base_projection = ProjectionSpec::from_fields_for_test(vec![
        ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(1))),
                }),
                name: Alias::new("rank_plus_one_internal_a"),
            },
            alias: Some(Alias::new("rank_plus_one_a")),
        },
        ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Binary {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(2))),
                }),
                name: Alias::new("rank_times_two_internal_a"),
            },
            alias: Some(Alias::new("rank_times_two_a")),
        },
    ]);
    let alias_variant_projection = ProjectionSpec::from_fields_for_test(vec![
        ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(1))),
                }),
                name: Alias::new("rank_plus_one_internal_b"),
            },
            alias: Some(Alias::new("rank_plus_one_b")),
        },
        ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Binary {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(2))),
                }),
                name: Alias::new("rank_times_two_internal_b"),
            },
            alias: Some(Alias::new("rank_times_two_b")),
        },
    ]);

    let base_rows: Vec<ProjectedRow<ProjectionEvalEntity>> =
        project_rows_from_projection(&base_projection, rows.as_slice())
            .expect("base expression projection should evaluate");
    let alias_rows: Vec<ProjectedRow<ProjectionEvalEntity>> =
        project_rows_from_projection(&alias_variant_projection, rows.as_slice())
            .expect("alias-variant expression projection should evaluate");

    assert_eq!(
        base_projection.structural_hash_for_test(),
        alias_variant_projection.structural_hash_for_test(),
        "expression projection identity must remain deterministic across alias-only renames",
    );
    assert_eq!(
        base_rows[0].values(),
        alias_rows[0].values(),
        "expression projection output values must remain deterministic across alias-only renames",
    );
    assert_eq!(base_rows[0].values().len(), 2);
    assert_eq!(
        base_rows[0].values()[0].cmp_numeric(&Value::Int(8)),
        Some(Ordering::Equal),
        "first expression projection output should preserve deterministic declared order",
    );
    assert_eq!(
        base_rows[0].values()[1].cmp_numeric(&Value::Int(14)),
        Some(Ordering::Equal),
        "second expression projection output should preserve deterministic declared order",
    );
}

#[test]
fn grouped_projection_ordering_preserves_input_group_order() {
    let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
    let aggregate_execution_specs = grouped_execution_specs([sum("rank")]);
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Aggregate(sum("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: Some(Alias::new("sum_plus_one")),
    }]);
    let grouped_inputs = vec![
        (vec![Value::Int(1)], vec![Value::Int(10)]),
        (vec![Value::Int(2)], vec![Value::Int(20)]),
        (vec![Value::Int(3)], vec![Value::Int(30)]),
    ];
    let mut observed = Vec::new();
    for (key_values, aggregate_values) in grouped_inputs {
        let row_view = GroupedRowView::new(
            key_values.as_slice(),
            aggregate_values.as_slice(),
            group_fields.as_slice(),
            aggregate_execution_specs.as_slice(),
        );
        let evaluated = evaluate_grouped_projection_values(&projection, &row_view)
            .expect("grouped projection should evaluate per-row");
        observed.push(evaluated[0].clone());
    }

    let expected = [Value::Int(11), Value::Int(21), Value::Int(31)];
    for (actual, expected_value) in observed.into_iter().zip(expected) {
        assert_eq!(
            actual.cmp_numeric(&expected_value),
            Some(Ordering::Equal),
            "grouped projection evaluation order must preserve grouped row order",
        );
    }
}

#[cfg(feature = "sql")]
#[test]
fn projection_materialization_exposes_projected_rows_payload() {
    let row = row(6, 19, true);
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Field(FieldId::new("rank")),
        alias: None,
    }]);
    let projected_rows = project_rows_from_projection::<ProjectionEvalEntity>(
        &projection,
        std::slice::from_ref(&row),
    )
    .expect("projection materialization should succeed for one row");

    assert_eq!(
        projected_rows.len(),
        1,
        "projection payload should preserve row cardinality"
    );
    assert_eq!(
        projected_rows[0].id(),
        row.0,
        "projection payload should preserve row identity"
    );
    assert_eq!(
        projected_rows[0].values(),
        &[Value::Int(19)],
        "projection payload should preserve projection value ordering",
    );
}
