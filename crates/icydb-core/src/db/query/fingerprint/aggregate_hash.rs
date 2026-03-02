//! Module: query::fingerprint::aggregate_hash
//! Responsibility: grouped aggregate structural hash encoding.
//! Does not own: explain projection assembly or plan profile ordering.
//! Boundary: semantic grouped aggregate hash bytes independent from explain-only metadata.

use crate::db::query::plan::AggregateKind;
use sha2::{Digest, Sha256};

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
    const GROUP_AGGREGATE_STRUCTURAL_FINGERPRINT_V1: u8 = 0x01;

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
            write_tag(hasher, 0x01);
            write_str(hasher, field);
        }
        None => write_tag(hasher, 0x00),
    }
    write_tag(hasher, if shape.distinct { 0x02 } else { 0x03 });
}

const fn aggregate_kind_tag_v1(kind: AggregateKind) -> u8 {
    match kind {
        AggregateKind::Count => 0x01,
        AggregateKind::Sum => 0x02,
        AggregateKind::Exists => 0x03,
        AggregateKind::Min => 0x04,
        AggregateKind::Max => 0x05,
        AggregateKind::First => 0x06,
        AggregateKind::Last => 0x07,
    }
}

fn write_tag(hasher: &mut Sha256, tag: u8) {
    hasher.update([tag]);
}

#[expect(clippy::cast_possible_truncation)]
fn write_str(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u32).to_be_bytes());
    hasher.update(value.as_bytes());
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
    use sha2::{Digest, Sha256};

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
        let mut hasher = Sha256::new();
        for shape in shapes {
            hash_group_aggregate_structural_fingerprint_v1(&mut hasher, shape);
        }
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        out
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
