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
    alias: Option<&'a str>,
    explain_projection_tag: Option<u8>,
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
            alias: None,
            explain_projection_tag: None,
        }
    }

    /// Attach optional alias metadata ignored by aggregate structural hashing.
    #[must_use]
    #[cfg(test)]
    pub(super) const fn with_alias(mut self, alias: Option<&'a str>) -> Self {
        self.alias = alias;
        self
    }

    /// Attach explain-only metadata ignored by aggregate structural hashing.
    #[must_use]
    #[cfg(test)]
    pub(super) const fn with_explain_projection_tag(mut self, tag: Option<u8>) -> Self {
        self.explain_projection_tag = tag;
        self
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
    // Alias and explain projection tags are intentionally excluded so aggregate
    // fingerprint identity remains purely semantic.
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

    let _ = shape.alias;
    let _ = shape.explain_projection_tag;
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
        fingerprint::aggregate_hash::{
            AggregateHashShape, hash_group_aggregate_structural_fingerprint_v1,
        },
        plan::AggregateKind,
    };
    use sha2::{Digest, Sha256};

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
    fn alias_metadata_is_excluded_from_aggregate_hash_identity() {
        let without_alias = AggregateHashShape::semantic(AggregateKind::Sum, Some("rank"), true);
        let with_alias = AggregateHashShape::semantic(AggregateKind::Sum, Some("rank"), true)
            .with_alias(Some("sum_rank"));

        assert_eq!(hash_shapes(&[without_alias]), hash_shapes(&[with_alias]));
    }

    #[test]
    fn explain_projection_metadata_is_excluded_from_aggregate_hash_identity() {
        let without_tag = AggregateHashShape::semantic(AggregateKind::Min, Some("rank"), false);
        let with_tag = AggregateHashShape::semantic(AggregateKind::Min, Some("rank"), false)
            .with_explain_projection_tag(Some(0xAA));

        assert_eq!(hash_shapes(&[without_tag]), hash_shapes(&[with_tag]));
    }

    #[test]
    fn aggregate_projection_order_remains_hash_significant() {
        let count = AggregateHashShape::semantic(AggregateKind::Count, None, false);
        let sum = AggregateHashShape::semantic(AggregateKind::Sum, Some("rank"), false);

        assert_ne!(hash_shapes(&[count, sum]), hash_shapes(&[sum, count]));
    }
}
