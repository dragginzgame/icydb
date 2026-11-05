use super::{
    EntityTag, IndexId, IndexKeyKind, LoweredIndexPrefixRawBounds, LoweredIndexPrefixSpec,
    LoweredIndexScanContract,
};
use crate::{MAX_INDEX_FIELDS, db::index::IndexKey, retained::RetainedBytes};
use std::sync::Arc;

#[test]
fn retained_deferred_bounds_reserve_encoder_capacity_before_materialization() {
    for arity in 1..=MAX_INDEX_FIELDS {
        for prefix_len in 0..=arity {
            let prefix_components = vec![vec![1_u8; 8]; prefix_len];
            let expected = IndexKey::raw_prefix_bounds_retained_capacity(arity, &prefix_components);
            let spec = LoweredIndexPrefixSpec {
                scan_contract: LoweredIndexScanContract {
                    name: Arc::from("index"),
                    store_path: Arc::from("store"),
                },
                raw_bounds: LoweredIndexPrefixRawBounds::deferred_component_prefix(
                    IndexId::new(EntityTag::new(254), 1),
                    IndexKeyKind::User,
                    arity,
                ),
                prefix_components,
            };
            let before =
                RetainedBytes::measure(&spec, usize::MAX).expect("reserved bound capacity");
            let (lower, upper) =
                crate::db::query::preparation::with_preparation_work(|work| spec.raw_bounds(work))
                    .expect("valid prefix");
            let mut materialized = RetainedBytes::new(usize::MAX);
            materialized.visit(lower).expect("known lower allocation");
            materialized.visit(upper).expect("known upper allocation");
            assert_eq!(materialized.total(), expected);
            assert_eq!(RetainedBytes::measure(&spec, usize::MAX), Some(before));
        }
    }
}
