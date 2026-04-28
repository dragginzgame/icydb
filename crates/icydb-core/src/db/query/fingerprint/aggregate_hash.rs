//! Module: query::fingerprint::aggregate_hash
//! Responsibility: grouped aggregate structural hash encoding.
//! Does not own: explain projection assembly or plan profile ordering.
//! Boundary: semantic grouped aggregate hash bytes independent from explain-only metadata.

use crate::db::query::{
    fingerprint::hash_parts::{write_str, write_tag},
    plan::{AggregateIdentity, AggregateKind},
};
use sha2::Sha256;

const GROUP_AGGREGATE_STRUCTURAL_FINGERPRINT_TAG: u8 = 0x01;

const AGGREGATE_TARGET_ABSENT_TAG: u8 = 0x00;
const AGGREGATE_TARGET_PRESENT_TAG: u8 = 0x01;
const AGGREGATE_DISTINCT_TAG: u8 = 0x02;
const AGGREGATE_NON_DISTINCT_TAG: u8 = 0x03;
const AGGREGATE_INPUT_EXPR_PRESENT_TAG: u8 = 0x04;
const AGGREGATE_FILTER_EXPR_PRESENT_TAG: u8 = 0x05;
const AGGREGATE_FILTER_EXPR_ABSENT_TAG: u8 = 0x06;

///
/// AggregateHashShape
/// Canonical aggregate identity hash shape for grouped aggregate hashing.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AggregateHashShape<'a> {
    kind: AggregateKind,
    target_field: Option<&'a str>,
    input_expr: Option<String>,
    filter_expr: Option<String>,
    distinct: bool,
}

impl<'a> AggregateHashShape<'a> {
    /// Build one semantic grouped aggregate hash shape.
    #[must_use]
    pub(in crate::db) const fn semantic(
        kind: AggregateKind,
        target_field: Option<&'a str>,
        input_expr: Option<String>,
        filter_expr: Option<String>,
        distinct: bool,
    ) -> Self {
        Self {
            kind,
            target_field,
            input_expr,
            filter_expr,
            distinct: AggregateIdentity::normalize_distinct_for_kind(kind, distinct),
        }
    }
}

// Hash one grouped aggregate identity shape using the current structural encoding.
pub(in crate::db) fn hash_group_aggregate_structural_fingerprint(
    hasher: &mut Sha256,
    shape: &AggregateHashShape<'_>,
) {
    // The grouped aggregate fingerprint includes exactly:
    // - aggregate kind discriminant
    // - optional target field
    // - distinct modifier flag
    //
    // Aggregate fingerprint identity must remain purely semantic.
    write_tag(hasher, GROUP_AGGREGATE_STRUCTURAL_FINGERPRINT_TAG);
    write_tag(hasher, shape.kind.fingerprint_tag());
    match shape.target_field {
        Some(field) => {
            write_tag(hasher, AGGREGATE_TARGET_PRESENT_TAG);
            write_str(hasher, field);
        }
        None => write_tag(hasher, AGGREGATE_TARGET_ABSENT_TAG),
    }
    write_tag(
        hasher,
        if shape.distinct {
            AGGREGATE_DISTINCT_TAG
        } else {
            AGGREGATE_NON_DISTINCT_TAG
        },
    );
    if let Some(input_expr) = shape.input_expr.as_deref() {
        write_tag(hasher, AGGREGATE_INPUT_EXPR_PRESENT_TAG);
        write_str(hasher, input_expr);
    }
    if let Some(filter_expr) = shape.filter_expr.as_deref() {
        write_tag(hasher, AGGREGATE_FILTER_EXPR_PRESENT_TAG);
        write_str(hasher, filter_expr);
    } else {
        write_tag(hasher, AGGREGATE_FILTER_EXPR_ABSENT_TAG);
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        codec::new_hash_sha256,
        query::{
            builder::count_by,
            fingerprint::aggregate_hash::{
                AggregateHashShape, hash_group_aggregate_structural_fingerprint,
            },
            plan::{AggregateKind, GroupAggregateSpec},
        },
    };

    ///
    /// AggregateSource
    ///
    /// Test-only source adapter to verify alias/explain metadata is excluded
    /// from aggregate identity hash construction.
    ///

    struct AggregateSource<'a> {
        kind: AggregateKind,
        target_field: Option<&'a str>,
        input_expr: Option<String>,
        filter_expr: Option<String>,
        distinct: bool,
        alias: Option<&'a str>,
        explain_projection_tag: Option<u8>,
    }

    impl<'a> AggregateSource<'a> {
        fn identity_shape(&self) -> AggregateHashShape<'a> {
            let _ = self.alias;
            let _ = self.explain_projection_tag;

            AggregateHashShape::semantic(
                self.kind,
                self.target_field,
                self.input_expr.clone(),
                self.filter_expr.clone(),
                self.distinct,
            )
        }
    }

    fn hash_shapes(shapes: &[AggregateHashShape<'_>]) -> [u8; 32] {
        let mut hasher = new_hash_sha256();
        for shape in shapes {
            hash_group_aggregate_structural_fingerprint(&mut hasher, shape);
        }
        super::super::finalize_sha256_digest(hasher)
    }

    #[test]
    fn equivalent_aggregate_identity_shapes_hash_identically() {
        let left =
            AggregateHashShape::semantic(AggregateKind::Count, Some("rank"), None, None, true);
        let right =
            AggregateHashShape::semantic(AggregateKind::Count, Some("rank"), None, None, true);

        assert_eq!(hash_shapes(&[left]), hash_shapes(&[right]));
    }

    #[test]
    fn alias_and_explain_metadata_are_excluded_from_aggregate_hash_identity() {
        let semantic = AggregateSource {
            kind: AggregateKind::Sum,
            target_field: Some("rank"),
            input_expr: None,
            filter_expr: None,
            distinct: true,
            alias: None,
            explain_projection_tag: None,
        }
        .identity_shape();
        let with_alias_and_tag = AggregateSource {
            kind: AggregateKind::Sum,
            target_field: Some("rank"),
            input_expr: None,
            filter_expr: None,
            distinct: true,
            alias: Some("sum_rank"),
            explain_projection_tag: Some(0xAA),
        }
        .identity_shape();

        assert_eq!(hash_shapes(&[semantic]), hash_shapes(&[with_alias_and_tag]),);
    }

    #[test]
    fn aggregate_projection_order_remains_hash_significant() {
        let count = AggregateHashShape::semantic(AggregateKind::Count, None, None, None, false);
        let sum = AggregateHashShape::semantic(AggregateKind::Sum, Some("rank"), None, None, false);

        assert_ne!(
            hash_shapes(&[count.clone(), sum.clone()]),
            hash_shapes(&[sum, count]),
        );
    }

    #[test]
    fn aggregate_expr_and_helper_shapes_hash_identically() {
        let aggregate_expr = count_by("rank").distinct();
        let helper_shape = GroupAggregateSpec::from_aggregate_expr(&aggregate_expr);
        let from_helper = AggregateHashShape::semantic(
            helper_shape.kind(),
            helper_shape.target_field(),
            helper_shape
                .input_expr()
                .map(crate::db::query::builder::scalar_projection::render_scalar_projection_expr_plan_label),
            helper_shape
                .filter_expr()
                .map(crate::db::query::builder::scalar_projection::render_scalar_projection_expr_plan_label),
            helper_shape.distinct(),
        );
        let manual = AggregateHashShape::semantic(
            AggregateKind::Count,
            Some("rank"),
            Some("rank".to_string()),
            None,
            true,
        );

        assert_eq!(hash_shapes(&[from_helper]), hash_shapes(&[manual]));
    }

    #[test]
    fn aggregate_input_expression_shape_remains_hash_significant() {
        let direct = AggregateHashShape::semantic(
            AggregateKind::Avg,
            Some("rank"),
            Some("rank".to_string()),
            None,
            false,
        );
        let widened = AggregateHashShape::semantic(
            AggregateKind::Avg,
            None,
            Some("rank + 1".to_string()),
            None,
            false,
        );

        assert_ne!(
            hash_shapes(&[direct]),
            hash_shapes(&[widened]),
            "aggregate fingerprint identity must distinguish widened aggregate input expressions",
        );
    }

    #[test]
    fn extrema_distinct_modifier_is_not_group_aggregate_hash_significant() {
        let min_rank =
            AggregateHashShape::semantic(AggregateKind::Min, Some("rank"), None, None, false);
        let min_distinct_rank =
            AggregateHashShape::semantic(AggregateKind::Min, Some("rank"), None, None, true);

        assert_eq!(hash_shapes(&[min_rank]), hash_shapes(&[min_distinct_rank]));
    }

    #[test]
    fn count_distinct_modifier_remains_group_aggregate_hash_significant() {
        let count_rank =
            AggregateHashShape::semantic(AggregateKind::Count, Some("rank"), None, None, false);
        let count_distinct_rank =
            AggregateHashShape::semantic(AggregateKind::Count, Some("rank"), None, None, true);

        assert_ne!(
            hash_shapes(&[count_rank]),
            hash_shapes(&[count_distinct_rank])
        );
    }

    #[test]
    fn aggregate_filter_expression_shape_remains_hash_significant() {
        let filtered = AggregateHashShape::semantic(
            AggregateKind::Count,
            None,
            None,
            Some("rank >= 10".to_string()),
            false,
        );
        let threshold_varied = AggregateHashShape::semantic(
            AggregateKind::Count,
            None,
            None,
            Some("rank >= 20".to_string()),
            false,
        );
        let unfiltered =
            AggregateHashShape::semantic(AggregateKind::Count, None, None, None, false);

        assert_ne!(
            hash_shapes(std::slice::from_ref(&filtered)),
            hash_shapes(&[threshold_varied]),
            "aggregate fingerprint identity must distinguish filtered aggregate threshold changes",
        );
        assert_ne!(
            hash_shapes(&[filtered]),
            hash_shapes(&[unfiltered]),
            "aggregate fingerprint identity must distinguish filtered and unfiltered aggregate shapes",
        );
    }
}
