//! Module: query::fingerprint::aggregate_hash
//! Responsibility: grouped aggregate structural hash encoding.
//! Does not own: explain projection assembly or plan profile ordering.
//! Boundary: semantic grouped aggregate hash bytes independent from explain-only metadata.

use crate::db::query::{
    fingerprint::hash_parts::{write_str, write_tag},
    plan::AggregateKind,
};
use sha2::Sha256;

const GROUP_AGGREGATE_STRUCTURAL_FINGERPRINT_V1: u8 = 0x01;

const AGGREGATE_TARGET_ABSENT_TAG: u8 = 0x00;
const AGGREGATE_TARGET_PRESENT_TAG: u8 = 0x01;
const AGGREGATE_DISTINCT_TAG: u8 = 0x02;
const AGGREGATE_NON_DISTINCT_TAG: u8 = 0x03;

const AGGREGATE_KIND_COUNT_TAG: u8 = 0x01;
const AGGREGATE_KIND_SUM_TAG: u8 = 0x02;
const AGGREGATE_KIND_EXISTS_TAG: u8 = 0x03;
const AGGREGATE_KIND_MIN_TAG: u8 = 0x04;
const AGGREGATE_KIND_MAX_TAG: u8 = 0x05;
const AGGREGATE_KIND_FIRST_TAG: u8 = 0x06;
const AGGREGATE_KIND_LAST_TAG: u8 = 0x07;
const AGGREGATE_KIND_AVG_TAG: u8 = 0x08;

///
/// AggregateHashShape
/// Canonical semantic aggregate hash shape for grouped aggregate hashing
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AggregateHashShape<'a> {
    kind: AggregateKind,
    target_field: Option<&'a str>,
    distinct: bool,
}

impl<'a> AggregateHashShape<'a> {
    /// Build one semantic grouped aggregate hash shape.
    #[must_use]
    pub(super) const fn semantic(
        kind: AggregateKind,
        target_field: Option<&'a str>,
        distinct: bool,
    ) -> Self {
        Self {
            kind,
            target_field,
            distinct,
        }
    }
}

// Hash one grouped aggregate semantic shape using the v1 structural encoding.
pub(super) fn hash_group_aggregate_structural_fingerprint_v1(
    hasher: &mut Sha256,
    shape: &AggregateHashShape<'_>,
) {
    // v1 grouped aggregate fingerprint includes exactly:
    // - aggregate kind discriminant
    // - optional target field
    // - distinct modifier flag
    //
    // Aggregate fingerprint identity must remain purely semantic.
    write_tag(hasher, GROUP_AGGREGATE_STRUCTURAL_FINGERPRINT_V1);
    write_tag(hasher, aggregate_kind_tag_v1(shape.kind));
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
}

const fn aggregate_kind_tag_v1(kind: AggregateKind) -> u8 {
    match kind {
        AggregateKind::Count => AGGREGATE_KIND_COUNT_TAG,
        AggregateKind::Sum => AGGREGATE_KIND_SUM_TAG,
        AggregateKind::Exists => AGGREGATE_KIND_EXISTS_TAG,
        AggregateKind::Min => AGGREGATE_KIND_MIN_TAG,
        AggregateKind::Max => AGGREGATE_KIND_MAX_TAG,
        AggregateKind::First => AGGREGATE_KIND_FIRST_TAG,
        AggregateKind::Last => AGGREGATE_KIND_LAST_TAG,
        AggregateKind::Avg => AGGREGATE_KIND_AVG_TAG,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::query::{
        builder::count_by,
        fingerprint::aggregate_hash::{
            AggregateHashShape, hash_group_aggregate_structural_fingerprint_v1,
        },
        plan::{AggregateKind, GroupAggregateSpec},
    };
    use sha2::Sha256;

    ///
    /// AggregateSource
    ///
    /// Test-only source adapter to verify alias/explain metadata is excluded
    /// from semantic aggregate hash construction.
    ///

    struct AggregateSource<'a> {
        kind: AggregateKind,
        target_field: Option<&'a str>,
        distinct: bool,
        alias: Option<&'a str>,
        explain_projection_tag: Option<u8>,
    }

    impl<'a> AggregateSource<'a> {
        fn semantic_shape(&self) -> AggregateHashShape<'a> {
            let _ = self.alias;
            let _ = self.explain_projection_tag;

            AggregateHashShape::semantic(self.kind, self.target_field, self.distinct)
        }
    }

    fn hash_shapes(shapes: &[AggregateHashShape<'_>]) -> [u8; 32] {
        let mut hasher = crate::db::codec::new_hash_sha256();
        for shape in shapes {
            hash_group_aggregate_structural_fingerprint_v1(&mut hasher, shape);
        }
        super::super::finalize_sha256_digest(hasher)
    }

    #[test]
    fn equivalent_semantic_aggregate_shapes_hash_identically() {
        let left = AggregateHashShape::semantic(AggregateKind::Count, Some("rank"), true);
        let right = AggregateHashShape::semantic(AggregateKind::Count, Some("rank"), true);

        assert_eq!(hash_shapes(&[left]), hash_shapes(&[right]));
    }

    #[test]
    fn alias_and_explain_metadata_are_excluded_from_aggregate_hash_identity() {
        let semantic = AggregateSource {
            kind: AggregateKind::Sum,
            target_field: Some("rank"),
            distinct: true,
            alias: None,
            explain_projection_tag: None,
        }
        .semantic_shape();
        let with_alias_and_tag = AggregateSource {
            kind: AggregateKind::Sum,
            target_field: Some("rank"),
            distinct: true,
            alias: Some("sum_rank"),
            explain_projection_tag: Some(0xAA),
        }
        .semantic_shape();

        assert_eq!(hash_shapes(&[semantic]), hash_shapes(&[with_alias_and_tag]),);
    }

    #[test]
    fn aggregate_projection_order_remains_hash_significant() {
        let count = AggregateHashShape::semantic(AggregateKind::Count, None, false);
        let sum = AggregateHashShape::semantic(AggregateKind::Sum, Some("rank"), false);

        assert_ne!(hash_shapes(&[count, sum]), hash_shapes(&[sum, count]));
    }

    #[test]
    fn aggregate_expr_and_helper_shapes_hash_identically() {
        let aggregate_expr = count_by("rank").distinct();
        let helper_shape = GroupAggregateSpec::from_aggregate_expr(&aggregate_expr);
        let from_helper = AggregateHashShape::semantic(
            helper_shape.kind(),
            helper_shape.target_field(),
            helper_shape.distinct(),
        );
        let manual = AggregateHashShape::semantic(AggregateKind::Count, Some("rank"), true);

        assert_eq!(hash_shapes(&[from_helper]), hash_shapes(&[manual]));
    }

    #[test]
    fn aggregate_hash_shape_constructor_signature_accepts_only_semantic_fields() {
        let constructor: fn(
            AggregateKind,
            Option<&'static str>,
            bool,
        ) -> AggregateHashShape<'static> = AggregateHashShape::semantic;

        let _ = constructor;
    }

    #[test]
    fn aggregate_hash_encoder_signature_accepts_semantic_shape_only() {
        let hash: fn(&mut Sha256, &AggregateHashShape<'static>) =
            hash_group_aggregate_structural_fingerprint_v1;

        let _ = hash;
    }
}
