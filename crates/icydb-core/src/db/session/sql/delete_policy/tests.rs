use super::*;
use crate::db::session::sql::write_policy::{
    SqlWriteReturningBounds, SqlWriteReturningShape, SqlWriteWhereProof,
};

const PRIMARY_KEY: &[&str] = &["id"];

fn context() -> SqlDeletePolicyContext<'static> {
    SqlDeletePolicyContext::new(PRIMARY_KEY)
}

fn classify(sql: &str, policy: SqlDeleteExposurePolicy) -> SqlDeletePolicyReport {
    classify_sql_delete_policy(sql, policy, context()).expect("SQL should parse")
}

fn expect_plan(report: &SqlDeletePolicyReport) -> &SqlValidatedDeletePlan {
    report
        .plan
        .as_ref()
        .expect("admitted policy should produce a validated plan")
}

fn assert_no_plan(report: &SqlDeletePolicyReport) {
    assert!(
        report.plan.is_none(),
        "rejected policy should not expose a partially usable plan",
    );
}

#[test]
fn delete_policy_rejects_non_delete_statement() {
    let report = classify(
        "SELECT id FROM Character",
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
    );

    assert_eq!(report.classification, None);
    assert_eq!(report.rejection, Some(SqlDeletePolicyRejection::NotDelete),);
    assert_no_plan(&report);
}

#[test]
fn delete_policy_public_primary_key_only_accepts_primary_key_equality() {
    let report = classify(
        "DELETE FROM Character WHERE id = 1",
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
    );

    assert!(report.is_admitted());
    assert_eq!(
        report
            .classification
            .as_ref()
            .expect("classification should be present")
            .write_shape
            .where_proof,
        SqlWriteWhereProof::PrimaryKeyEquality,
    );
    assert!(matches!(
        expect_plan(&report),
        SqlValidatedDeletePlan::PublicPrimaryKeyOnly(_),
    ));
}

#[test]
fn delete_policy_public_primary_key_only_accepts_alias_qualified_primary_key_equality() {
    let report = classify(
        "DELETE FROM Character c WHERE c.id = 1",
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
    );

    assert!(report.is_admitted());
    assert_eq!(
        report
            .classification
            .as_ref()
            .expect("classification should be present")
            .write_shape
            .where_proof,
        SqlWriteWhereProof::PrimaryKeyEquality,
    );
}

#[test]
fn delete_policy_public_primary_key_only_rejects_missing_where() {
    let report = classify(
        "DELETE FROM Character",
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
    );

    assert_eq!(
        report.rejection,
        Some(SqlDeletePolicyRejection::MissingWhere),
    );
    assert_no_plan(&report);
}

#[test]
fn delete_policy_public_primary_key_only_rejects_non_primary_key_where() {
    let report = classify(
        "DELETE FROM Character WHERE age = 21",
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
    );

    assert_eq!(
        report.rejection,
        Some(SqlDeletePolicyRejection::PrimaryKeyProofFailed),
    );
    assert_no_plan(&report);
}

#[test]
fn delete_policy_public_primary_key_only_rejects_extra_where_guard() {
    let report = classify(
        "DELETE FROM Character WHERE id = 1 AND active = true",
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
    );

    assert_eq!(
        report.rejection,
        Some(SqlDeletePolicyRejection::PrimaryKeyProofFailed),
    );
    assert_no_plan(&report);
}

#[test]
fn delete_policy_public_primary_key_only_accepts_complete_composite_primary_key() {
    let context = SqlDeletePolicyContext::new(&["tenant_id", "id"]);
    let report = classify_sql_delete_policy(
        "DELETE FROM Character WHERE tenant_id = 7 AND id = 1",
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        context,
    )
    .expect("SQL should parse");

    assert!(report.is_admitted());
    assert!(matches!(
        expect_plan(&report),
        SqlValidatedDeletePlan::PublicPrimaryKeyOnly(_),
    ));
}

#[test]
fn delete_policy_public_bounded_accepts_explicit_primary_key_order_and_limit() {
    let report = classify(
        "DELETE FROM Character WHERE age = 21 ORDER BY id LIMIT 10",
        SqlDeleteExposurePolicy::PublicBoundedDeterministic,
    );

    assert!(report.is_admitted());
    let classification = report
        .classification
        .as_ref()
        .expect("admitted DELETE should include classification");
    assert!(classification.write_shape.is_bounded());
    assert!(
        classification
            .write_shape
            .has_explicit_canonical_primary_key_order()
    );
    assert!(matches!(
        expect_plan(&report),
        SqlValidatedDeletePlan::PublicBoundedDeterministic(_),
    ));
}

#[test]
fn delete_policy_public_bounded_rejects_missing_where() {
    let report = classify(
        "DELETE FROM Character ORDER BY id LIMIT 10",
        SqlDeleteExposurePolicy::PublicBoundedDeterministic,
    );

    assert_eq!(
        report.rejection,
        Some(SqlDeletePolicyRejection::MissingWhere),
    );
    assert_no_plan(&report);
}

#[test]
fn delete_policy_public_bounded_rejects_implicit_primary_key_fallback() {
    let report = classify(
        "DELETE FROM Character WHERE age = 21 LIMIT 10",
        SqlDeleteExposurePolicy::PublicBoundedDeterministic,
    );

    assert_eq!(
        report.rejection,
        Some(SqlDeletePolicyRejection::MissingCanonicalPrimaryKeyOrder),
    );
    assert_no_plan(&report);
}

#[test]
fn delete_policy_public_bounded_rejects_non_primary_key_ordering() {
    let report = classify(
        "DELETE FROM Character WHERE age = 21 ORDER BY age LIMIT 10",
        SqlDeleteExposurePolicy::PublicBoundedDeterministic,
    );

    assert_eq!(
        report.rejection,
        Some(SqlDeletePolicyRejection::MissingCanonicalPrimaryKeyOrder),
    );
    assert_no_plan(&report);
}

#[test]
fn delete_policy_public_bounded_rejects_descending_order() {
    let report = classify(
        "DELETE FROM Character WHERE age = 21 ORDER BY id DESC LIMIT 10",
        SqlDeleteExposurePolicy::PublicBoundedDeterministic,
    );

    assert_eq!(
        report.rejection,
        Some(SqlDeletePolicyRejection::DescendingOrder),
    );
    assert_no_plan(&report);
}

#[test]
fn delete_policy_public_bounded_rejects_excessive_limit() {
    let excessive_limit = DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT
        .checked_add(1)
        .expect("test default public bounded delete limit should fit u32");
    let report = classify(
        format!("DELETE FROM Character WHERE age = 21 ORDER BY id LIMIT {excessive_limit}")
            .as_str(),
        SqlDeleteExposurePolicy::PublicBoundedDeterministic,
    );

    assert_eq!(
        report.rejection,
        Some(SqlDeletePolicyRejection::LimitTooHigh),
    );
    assert_no_plan(&report);
}

#[test]
fn delete_policy_public_bounded_rejects_offset() {
    let report = classify(
        "DELETE FROM Character WHERE age = 21 ORDER BY id LIMIT 10 OFFSET 1",
        SqlDeleteExposurePolicy::PublicBoundedDeterministic,
    );

    assert_eq!(
        report.rejection,
        Some(SqlDeletePolicyRejection::OffsetUnsupported),
    );
    assert_no_plan(&report);
}

#[test]
fn delete_policy_classifies_narrow_returning_shapes() {
    let returning_all = classify(
        "DELETE FROM Character WHERE id = 1 RETURNING *",
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
    );
    let returning_fields = classify(
        "DELETE FROM Character WHERE id = 1 RETURNING id, age",
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
    );

    assert!(returning_all.is_admitted());
    assert_eq!(
        returning_all
            .classification
            .as_ref()
            .expect("classification should be present")
            .write_shape
            .returning_shape,
        SqlWriteReturningShape::NarrowAll,
    );
    assert!(returning_fields.is_admitted());
    assert_eq!(
        returning_fields
            .classification
            .as_ref()
            .expect("classification should be present")
            .write_shape
            .returning_shape,
        SqlWriteReturningShape::NarrowFields,
    );
}

#[test]
fn delete_policy_validated_plans_carry_execution_and_returning_bounds() {
    let context = SqlDeletePolicyContext {
        primary_key_fields: PRIMARY_KEY,
        max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT,
        max_returning_rows: None,
        max_returning_response_bytes: Some(4096),
    };
    let primary_key = classify_sql_delete_policy(
        "DELETE FROM Character WHERE id = 1 RETURNING id",
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        context,
    )
    .expect("SQL should parse");
    let bounded = classify_sql_delete_policy(
        "DELETE FROM Character WHERE age = 21 ORDER BY id LIMIT 10 RETURNING id",
        SqlDeleteExposurePolicy::PublicBoundedDeterministic,
        context,
    )
    .expect("SQL should parse");

    assert_eq!(
        expect_plan(&primary_key).returning_bounds(),
        SqlWriteReturningBounds {
            max_rows: Some(1),
            max_response_bytes: Some(4096),
        },
    );
    assert_eq!(
        expect_plan(&bounded).returning_bounds(),
        SqlWriteReturningBounds {
            max_rows: Some(10),
            max_response_bytes: Some(4096),
        },
    );
    assert_eq!(
        expect_plan(&primary_key).execution_bounds().max_staged_rows,
        Some(1),
    );
    assert_eq!(
        expect_plan(&bounded).execution_bounds().max_staged_rows,
        Some(10),
    );
}

#[test]
fn delete_policy_public_generated_context_carries_default_returning_byte_bound() {
    let context = SqlDeletePolicyContext::public_generated(PRIMARY_KEY);
    let report = classify_sql_delete_policy(
        "DELETE FROM Character WHERE id = 1 RETURNING id",
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        context,
    )
    .expect("SQL should parse");

    assert_eq!(
        expect_plan(&report).returning_bounds(),
        SqlWriteReturningBounds {
            max_rows: Some(1),
            max_response_bytes: Some(DEFAULT_PUBLIC_DELETE_RETURNING_RESPONSE_BYTES),
        },
    );
}

#[test]
fn delete_policy_validated_plans_lower_configured_returning_row_bound() {
    let context = SqlDeletePolicyContext {
        primary_key_fields: PRIMARY_KEY,
        max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT,
        max_returning_rows: Some(2),
        max_returning_response_bytes: None,
    };
    let primary_key = classify_sql_delete_policy(
        "DELETE FROM Character WHERE id = 1 RETURNING id",
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        context,
    )
    .expect("SQL should parse");
    let bounded = classify_sql_delete_policy(
        "DELETE FROM Character WHERE age = 21 ORDER BY id LIMIT 10 RETURNING id",
        SqlDeleteExposurePolicy::PublicBoundedDeterministic,
        context,
    )
    .expect("SQL should parse");

    assert_eq!(
        expect_plan(&primary_key).returning_bounds(),
        SqlWriteReturningBounds {
            max_rows: Some(1),
            max_response_bytes: None,
        },
    );
    assert_eq!(
        expect_plan(&bounded).returning_bounds(),
        SqlWriteReturningBounds {
            max_rows: Some(2),
            max_response_bytes: None,
        },
    );
}
