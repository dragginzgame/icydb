use super::{
    SqlProjectionMaterializationMetrics, projection_materialization_metrics_recorder,
    with_sql_projection_materialization_metrics,
};

use crate::{
    db::{
        executor::{
            PreparedProjectionPlan, StructuralCursorPage, project,
            projection::PreparedProjectionShape, projection_eval_data_row_for_materialize_tests,
            projection_eval_row_layout_for_materialize_tests, terminal::RetainedSlotRow,
        },
        query::plan::expr::{Expr, FieldId, ProjectionField, ProjectionSpec},
    },
    value::Value,
};

fn direct_rank_projection_shape() -> PreparedProjectionShape {
    PreparedProjectionShape::from_test_parts(
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("rank")),
            alias: None,
        }]),
        PreparedProjectionPlan::Scalar(Vec::new()),
        false,
        Some(vec![("rank".to_string(), 1)]),
        Some(vec![("rank".to_string(), 1)]),
        vec![false, true, false, false],
    )
}

fn repeated_direct_rank_projection_shape() -> PreparedProjectionShape {
    PreparedProjectionShape::from_test_parts(
        ProjectionSpec::from_fields_for_test(vec![
            ProjectionField::Scalar {
                expr: Expr::Field(FieldId::new("rank")),
                alias: None,
            },
            ProjectionField::Scalar {
                expr: Expr::Field(FieldId::new("rank")),
                alias: None,
            },
        ]),
        PreparedProjectionPlan::Scalar(Vec::new()),
        false,
        None,
        Some(vec![("rank".to_string(), 1), ("rank".to_string(), 1)]),
        vec![false, true, false, false],
    )
}

fn expect_projection_metrics<T>(f: impl FnOnce() -> T) -> (T, SqlProjectionMaterializationMetrics) {
    with_sql_projection_materialization_metrics(f)
}

#[test]
fn sql_projection_materialization_prefers_retained_slot_rows() {
    let row_layout = projection_eval_row_layout_for_materialize_tests();
    let page = StructuralCursorPage::new_with_slot_rows(
        vec![RetainedSlotRow::new(4, vec![(1, Value::Int(19))])],
        None,
    );
    let prepared_projection = direct_rank_projection_shape();

    let (payload, metrics) = expect_projection_metrics(|| {
        project(
            row_layout,
            &prepared_projection,
            page,
            projection_materialization_metrics_recorder(),
        )
    });
    let payload = payload
        .expect("slot-row SQL projection materialization should succeed")
        .into_value_rows();

    assert_eq!(payload, vec![vec![Value::Int(19)]]);

    assert_eq!(
        metrics.slot_rows_path_hits, 1,
        "slot-row projection should stay on the retained-slot path",
    );
    assert_eq!(
        metrics.data_rows_path_hits, 0,
        "slot-row projection should not reopen raw data rows",
    );
    assert_eq!(
        metrics.data_rows_scalar_fallback_hits, 0,
        "slot-row projection should avoid the scalar data-row fallback",
    );
    assert_eq!(
        metrics.full_row_decode_materializations, 0,
        "slot-row projection should not trigger eager full-row decode",
    );
}

#[test]
fn sql_projection_materialization_prefers_direct_data_row_field_copies() {
    let row_layout = projection_eval_row_layout_for_materialize_tests();
    let page = StructuralCursorPage::new(
        vec![projection_eval_data_row_for_materialize_tests(41, 19, true)],
        None,
    );
    let prepared_projection = direct_rank_projection_shape();

    let (payload, metrics) = expect_projection_metrics(|| {
        project(
            row_layout,
            &prepared_projection,
            page,
            projection_materialization_metrics_recorder(),
        )
    });
    let payload = payload
        .expect("data-row SQL projection materialization should succeed")
        .into_value_rows();

    assert_eq!(payload, vec![vec![Value::Int(19)]]);

    assert_eq!(
        metrics.data_rows_path_hits, 1,
        "data-row projection should stay on the raw-row path",
    );
    assert_eq!(
        metrics.data_rows_scalar_fallback_hits, 0,
        "direct data-row field copies should avoid the scalar fallback path",
    );
    assert_eq!(
        metrics.data_rows_projected_slot_accesses, 1,
        "direct data-row field copies should decode only the declared projected slot",
    );
    assert_eq!(
        metrics.data_rows_non_projected_slot_accesses, 0,
        "direct data-row field copies should avoid unrelated slot reads",
    );
}

#[test]
fn sql_projection_materialization_prefers_direct_data_row_field_copies_for_repeated_fields() {
    let row_layout = projection_eval_row_layout_for_materialize_tests();
    let page = StructuralCursorPage::new(
        vec![projection_eval_data_row_for_materialize_tests(41, 19, true)],
        None,
    );
    let prepared_projection = repeated_direct_rank_projection_shape();

    let (payload, metrics) = expect_projection_metrics(|| {
        project(
            row_layout,
            &prepared_projection,
            page,
            projection_materialization_metrics_recorder(),
        )
    });
    let payload = payload
        .expect("repeated data-row SQL projection materialization should succeed")
        .into_value_rows();

    assert_eq!(payload, vec![vec![Value::Int(19), Value::Int(19)]]);

    assert_eq!(
        metrics.data_rows_path_hits, 1,
        "repeated data-row projection should stay on the raw-row path",
    );
    assert_eq!(
        metrics.data_rows_scalar_fallback_hits, 0,
        "repeated direct data-row fields should avoid the scalar fallback path",
    );
    assert_eq!(
        metrics.data_rows_projected_slot_accesses, 2,
        "repeated direct data-row fields should read only the repeated projected slot",
    );
    assert_eq!(
        metrics.data_rows_non_projected_slot_accesses, 0,
        "repeated direct data-row fields should avoid unrelated slot reads",
    );
}
