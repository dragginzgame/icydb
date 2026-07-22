use super::model::MAX_TRUSTED_EXACT_UPDATE_ROWS;
use super::*;
use crate::db::session::sql::write_policy::{
    SqlWriteReturningBounds, SqlWriteReturningShape, SqlWriteWhereProof,
};

const PRIMARY_KEY: &[&str] = &["id"];

fn context() -> SqlUpdatePolicyContext<'static> {
    SqlUpdatePolicyContext::new(PRIMARY_KEY)
}

fn classify(sql: &str, policy: SqlUpdateExposurePolicy) -> SqlUpdatePolicyReport {
    classify_sql_update_policy(sql, policy, context()).expect("SQL should parse")
}

fn expect_plan(report: &SqlUpdatePolicyReport) -> &SqlValidatedUpdatePlan {
    assert!(
        report.rejection.is_none(),
        "admitted policy must not also carry a rejection",
    );
    report
        .plan
        .as_ref()
        .expect("admitted policy should produce a validated plan")
}

fn assert_no_plan(report: &SqlUpdatePolicyReport) {
    assert!(
        report.rejection.is_some(),
        "policy without a plan must carry a typed rejection",
    );
    assert!(
        report.plan.is_none(),
        "rejected policy should not expose a partially usable plan",
    );
}

#[test]
fn update_policy_rejects_non_update_statement() {
    let report = classify(
        "SELECT id FROM Character",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
    );

    assert_eq!(report.classification, None);
    assert_eq!(report.rejection, Some(SqlUpdatePolicyRejection::NotUpdate),);
    assert_no_plan(&report);
}

#[test]
fn update_policy_public_primary_key_rejects_missing_where() {
    let report = classify(
        "UPDATE Character SET active = false",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
    );

    assert_eq!(
        report
            .classification
            .as_ref()
            .expect("UPDATE should still classify")
            .write_shape
            .where_proof,
        SqlWriteWhereProof::Missing,
    );
    assert_eq!(
        report.rejection,
        Some(SqlUpdatePolicyRejection::MissingWhere),
    );
    assert_no_plan(&report);
}

#[test]
fn update_policy_public_primary_key_only_accepts_primary_key_equality() {
    let report = classify(
        "UPDATE Character SET age = 22 WHERE id = 1",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
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
        SqlValidatedUpdatePlan::PublicPrimaryKeyOnly(_),
    ));
}

#[test]
fn update_policy_public_primary_key_only_accepts_alias_qualified_primary_key_equality() {
    let report = classify(
        "UPDATE Character c SET age = 22 WHERE c.id = 1",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
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
fn update_policy_public_primary_key_only_rejects_primary_key_assignment() {
    let report = classify(
        "UPDATE Character SET id = 2 WHERE id = 1",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
    );

    assert_eq!(
        report
            .classification
            .as_ref()
            .expect("classification should be present")
            .assignment_policy,
        SqlUpdateAssignmentPolicy {
            mutates_primary_key: true,
            mutates_generated: false,
            mutates_managed: false,
        },
    );
    assert_eq!(
        report.rejection,
        Some(SqlUpdatePolicyRejection::PrimaryKeyMutation),
    );
    assert_no_plan(&report);
}

#[test]
fn update_policy_public_primary_key_only_rejects_non_primary_key_where() {
    let report = classify(
        "UPDATE Character SET active = false WHERE age = 21",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
    );

    assert_eq!(
        report.rejection,
        Some(SqlUpdatePolicyRejection::PrimaryKeyProofFailed),
    );
    assert_no_plan(&report);
}

#[test]
fn update_policy_public_primary_key_only_rejects_extra_where_guard() {
    let report = classify(
        "UPDATE Character SET age = 22 WHERE id = 1 AND active = true",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
    );

    assert_eq!(
        report.rejection,
        Some(SqlUpdatePolicyRejection::PrimaryKeyProofFailed),
    );
    assert_no_plan(&report);
}

#[test]
fn update_policy_public_primary_key_only_accepts_complete_composite_primary_key() {
    let context = SqlUpdatePolicyContext::new(&["tenant_id", "id"]);
    let report = classify_sql_update_policy(
        "UPDATE Character SET age = 22 WHERE tenant_id = 7 AND id = 1",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        context,
    )
    .expect("SQL should parse");

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
        SqlValidatedUpdatePlan::PublicPrimaryKeyOnly(_),
    ));
}

#[test]
fn update_policy_public_primary_key_only_rejects_partial_composite_primary_key() {
    let context = SqlUpdatePolicyContext::new(&["tenant_id", "id"]);
    let report = classify_sql_update_policy(
        "UPDATE Character SET age = 22 WHERE id = 1",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        context,
    )
    .expect("SQL should parse");

    assert_eq!(
        report.rejection,
        Some(SqlUpdatePolicyRejection::PrimaryKeyProofFailed),
    );
    assert_no_plan(&report);
}

#[test]
fn update_policy_classifies_narrow_returning_shapes() {
    let returning_all = classify(
        "UPDATE Character SET age = 22 WHERE id = 1 RETURNING *",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
    );
    let returning_fields = classify(
        "UPDATE Character SET age = 22 WHERE id = 1 RETURNING id, age",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
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
fn update_policy_validated_plans_carry_execution_and_returning_bounds() {
    let context = SqlUpdatePolicyContext {
        primary_key_fields: PRIMARY_KEY,
        generated_fields: &[],
        managed_fields: &[],
        max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
        max_returning_rows: None,
        max_returning_response_bytes: Some(4096),
    };
    let primary_key = classify_sql_update_policy(
        "UPDATE Character SET age = 22 WHERE id = 1 RETURNING id",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        context,
    )
    .expect("SQL should parse");
    let bounded = classify_sql_update_policy(
        "UPDATE Character SET active = false WHERE age = 21 ORDER BY id LIMIT 10 RETURNING id",
        SqlUpdateExposurePolicy::PublicBoundedDeterministic,
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
fn update_policy_validated_plans_lower_configured_returning_row_bound() {
    let context = SqlUpdatePolicyContext {
        primary_key_fields: PRIMARY_KEY,
        generated_fields: &[],
        managed_fields: &[],
        max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
        max_returning_rows: Some(2),
        max_returning_response_bytes: None,
    };
    let primary_key = classify_sql_update_policy(
        "UPDATE Character SET age = 22 WHERE id = 1 RETURNING id",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        context,
    )
    .expect("SQL should parse");
    let bounded = classify_sql_update_policy(
        "UPDATE Character SET active = false WHERE age = 21 ORDER BY id LIMIT 10 RETURNING id",
        SqlUpdateExposurePolicy::PublicBoundedDeterministic,
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

#[test]
fn update_policy_public_bounded_accepts_explicit_primary_key_order_and_limit() {
    let report = classify(
        "UPDATE Character SET active = false WHERE age = 21 ORDER BY id LIMIT 10",
        SqlUpdateExposurePolicy::PublicBoundedDeterministic,
    );

    assert!(report.is_admitted());
    let classification = report
        .classification
        .as_ref()
        .expect("admitted UPDATE should include classification");
    assert!(classification.write_shape.is_bounded());
    assert!(
        classification
            .write_shape
            .has_explicit_canonical_primary_key_order()
    );
    assert!(matches!(
        expect_plan(&report),
        SqlValidatedUpdatePlan::PublicBoundedDeterministic(_),
    ));
}

#[test]
fn update_policy_public_bounded_rejects_implicit_primary_key_fallback() {
    let report = classify(
        "UPDATE Character SET active = false WHERE age = 21 LIMIT 10",
        SqlUpdateExposurePolicy::PublicBoundedDeterministic,
    );

    assert_eq!(
        report.rejection,
        Some(SqlUpdatePolicyRejection::MissingCanonicalPrimaryKeyOrder),
    );
    assert_no_plan(&report);
}

#[test]
fn update_policy_public_bounded_rejects_missing_limit() {
    let report = classify(
        "UPDATE Character SET active = false WHERE age = 21 ORDER BY id",
        SqlUpdateExposurePolicy::PublicBoundedDeterministic,
    );

    assert_eq!(
        report.rejection,
        Some(SqlUpdatePolicyRejection::MissingLimit),
    );
    assert_no_plan(&report);
}

#[test]
fn update_policy_public_bounded_rejects_non_primary_key_ordering() {
    let report = classify(
        "UPDATE Character SET active = false WHERE age = 21 ORDER BY age LIMIT 10",
        SqlUpdateExposurePolicy::PublicBoundedDeterministic,
    );

    assert_eq!(
        report.rejection,
        Some(SqlUpdatePolicyRejection::MissingCanonicalPrimaryKeyOrder),
    );
    assert_no_plan(&report);
}

#[test]
fn update_policy_public_bounded_rejects_descending_order() {
    let report = classify(
        "UPDATE Character SET active = false WHERE age = 21 ORDER BY id DESC LIMIT 10",
        SqlUpdateExposurePolicy::PublicBoundedDeterministic,
    );

    assert_eq!(
        report.rejection,
        Some(SqlUpdatePolicyRejection::DescendingOrder),
    );
    assert_no_plan(&report);
}

#[test]
fn update_policy_public_bounded_rejects_excessive_limit() {
    let excessive_limit = DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT
        .checked_add(1)
        .expect("test default public bounded update limit should fit u32");
    let report = classify(
        format!(
            "UPDATE Character SET active = false WHERE age = 21 ORDER BY id \
                 LIMIT {excessive_limit}",
        )
        .as_str(),
        SqlUpdateExposurePolicy::PublicBoundedDeterministic,
    );

    assert_eq!(
        report.rejection,
        Some(SqlUpdatePolicyRejection::LimitTooHigh),
    );
    assert_no_plan(&report);
}

#[test]
fn update_policy_public_bounded_rejects_offset() {
    let report = classify(
        "UPDATE Character SET active = false WHERE age = 21 ORDER BY id LIMIT 10 OFFSET 1",
        SqlUpdateExposurePolicy::PublicBoundedDeterministic,
    );

    assert_eq!(
        report.rejection,
        Some(SqlUpdatePolicyRejection::OffsetUnsupported),
    );
    assert_no_plan(&report);
}

#[test]
fn update_policy_rejects_generated_and_managed_assignment() {
    let context = SqlUpdatePolicyContext {
        primary_key_fields: PRIMARY_KEY,
        generated_fields: &["slug"],
        managed_fields: &["updated_at"],
        max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
        max_returning_rows: None,
        max_returning_response_bytes: None,
    };

    let generated = classify_sql_update_policy(
        "UPDATE Character SET slug = 'ada' WHERE id = 1",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        context,
    )
    .expect("SQL should parse");
    let managed = classify_sql_update_policy(
        "UPDATE Character SET updated_at = 1 WHERE id = 1",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        context,
    )
    .expect("SQL should parse");

    assert_eq!(
        generated.rejection,
        Some(SqlUpdatePolicyRejection::GeneratedFieldMutation),
    );
    assert_eq!(
        managed.rejection,
        Some(SqlUpdatePolicyRejection::ManagedFieldMutation),
    );
    assert_no_plan(&generated);
    assert_no_plan(&managed);
}

#[test]
fn update_policy_allows_schema_owned_returning_fields_on_public_surfaces() {
    let context = SqlUpdatePolicyContext {
        primary_key_fields: PRIMARY_KEY,
        generated_fields: &["slug"],
        managed_fields: &["updated_at"],
        max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
        max_returning_rows: None,
        max_returning_response_bytes: None,
    };
    let cases = [
        (
            "UPDATE Character SET age = 22 WHERE id = 1 RETURNING *",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        ),
        (
            "UPDATE Character SET age = 22 WHERE id = 1 RETURNING slug",
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        ),
        (
            "UPDATE Character SET active = false WHERE age = 21 ORDER BY id LIMIT 10 \
                 RETURNING updated_at",
            SqlUpdateExposurePolicy::PublicBoundedDeterministic,
        ),
    ];

    for (sql, policy) in cases {
        let report = classify_sql_update_policy(sql, policy, context)
            .expect("schema-owned RETURNING SQL should parse");

        assert!(
            report.is_admitted(),
            "public returning follows accepted row projection visibility",
        );
        let _ = expect_plan(&report);
    }
}

#[test]
fn update_policy_preserves_shape_rejections_with_schema_owned_returning_fields() {
    let context = SqlUpdatePolicyContext {
        primary_key_fields: PRIMARY_KEY,
        generated_fields: &["slug"],
        managed_fields: &[],
        max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
        max_returning_rows: None,
        max_returning_response_bytes: None,
    };
    let primary_key = classify_sql_update_policy(
        "UPDATE Character SET age = 22 WHERE age = 21 RETURNING *",
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        context,
    )
    .expect("primary-key policy rejection SQL should parse");
    let bounded = classify_sql_update_policy(
        "UPDATE Character SET age = 22 WHERE age = 21 LIMIT 10 RETURNING *",
        SqlUpdateExposurePolicy::PublicBoundedDeterministic,
        context,
    )
    .expect("bounded policy rejection SQL should parse");

    assert_eq!(
        primary_key.rejection,
        Some(SqlUpdatePolicyRejection::PrimaryKeyProofFailed),
    );
    assert_eq!(
        bounded.rejection,
        Some(SqlUpdatePolicyRejection::MissingCanonicalPrimaryKeyOrder),
    );
    assert_no_plan(&primary_key);
    assert_no_plan(&bounded);
}

#[test]
fn exact_update_policy_carries_assertion_and_execution_bounds() {
    let policy = SqlExactUpdatePolicy::try_new(3).expect("positive exact assertion should admit");
    let report = classify(
        "UPDATE Character SET age = 22 WHERE active = true RETURNING id",
        SqlUpdateExposurePolicy::TrustedExact(policy),
    );
    let SqlValidatedUpdatePlan::TrustedExact(plan) = expect_plan(&report) else {
        panic!("exact policy should produce an exact plan");
    };

    assert_eq!(plan.policy().require_affected_at_most(), 3);
    assert_eq!(plan.policy().selection_limit(), 4);
    assert_eq!(SqlExactUpdatePolicy::scan_budget(), 4_096);
    assert_eq!(plan.execution_bounds().max_staged_rows, Some(3));
    assert_eq!(plan.execution_bounds().returning.max_rows, Some(3));
}

#[test]
fn exact_update_policy_rejects_sql_windows_and_noncanonical_order() {
    let policy = SqlExactUpdatePolicy::try_new(3).expect("positive exact assertion should admit");

    for sql in [
        "UPDATE Character SET age = 22 WHERE active = true LIMIT 2",
        "UPDATE Character SET age = 22 WHERE active = true OFFSET 1",
        "UPDATE Character SET age = 22 WHERE active = true ORDER BY id DESC",
        "UPDATE Character SET age = 22 WHERE active = true ORDER BY age ASC",
    ] {
        let report = classify(sql, SqlUpdateExposurePolicy::TrustedExact(policy));

        assert_eq!(
            report.rejection,
            Some(SqlUpdatePolicyRejection::ExactWindowUnsupported),
            "{sql}",
        );
        assert_no_plan(&report);
    }

    let canonical = classify(
        "UPDATE Character SET age = 22 WHERE active = true ORDER BY id ASC",
        SqlUpdateExposurePolicy::TrustedExact(policy),
    );
    assert!(canonical.is_admitted());
}

#[test]
fn exact_update_policy_rejects_zero_and_engine_ceiling_overflow() {
    assert_eq!(
        SqlExactUpdatePolicy::try_new(0),
        Err(SqlExactUpdatePolicyRejection::AssertionRequired),
    );
    assert_eq!(
        SqlExactUpdatePolicy::try_new(
            MAX_TRUSTED_EXACT_UPDATE_ROWS
                .checked_add(1)
                .expect("exact ceiling should leave room for overflow test"),
        ),
        Err(SqlExactUpdatePolicyRejection::AssertionTooHigh),
    );
}
