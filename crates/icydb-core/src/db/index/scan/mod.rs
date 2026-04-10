//! Module: index::scan
//! Responsibility: raw-range index scan resolution under cursor-owned continuation contracts.
//! Does not own: index persistence layout or predicate compilation.
//! Boundary: executor/query range reads go through this layer above `index::store`.

mod covering;
mod decode;
mod raw;

use crate::{
    db::{data::DataKey, index::IndexEntryExistenceWitness},
    value::StorageKey,
};

type IndexComponentValues = Vec<Vec<u8>>;
type DataKeyWitnessRows = Vec<(DataKey, IndexEntryExistenceWitness)>;
type DataKeyComponentRows = Vec<(DataKey, IndexEntryExistenceWitness, IndexComponentValues)>;

///
/// SingleComponentCoveringCollector
///
/// Narrow collector contract for the single-component covering fast path.
/// The index layer streams decoded membership entries through this boundary
/// without owning projection semantics beyond "emit storage key + component".
///
pub(in crate::db) trait SingleComponentCoveringCollector<T> {
    fn push(
        &mut self,
        storage_key: StorageKey,
        existence_witness: IndexEntryExistenceWitness,
        component: &[u8],
        out: &mut Vec<T>,
    ) -> Result<(), crate::error::InternalError>;
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::index::store::IndexStore;
    use crate::{
        db::{
            data::{DataKey, StorageKey},
            index::{RawIndexEntry, RawIndexKey},
            with_row_check_metrics,
        },
        error::ErrorClass,
        model::index::IndexModel,
        traits::Storable,
        types::EntityTag,
    };
    use std::borrow::Cow;

    const TEST_SCAN_INDEX_FIELDS: &[&str] = &["name"];
    const TEST_SCAN_INDEX: IndexModel = IndexModel::generated(
        "scan::idx_name",
        "scan::IndexStore",
        TEST_SCAN_INDEX_FIELDS,
        false,
    );

    #[test]
    fn decode_index_entry_and_push_without_index_predicate_skips_raw_key_decode() {
        let entity = EntityTag::new(7);
        let raw_key = RawIndexKey::from_bytes(Cow::Owned(vec![0xFF]));
        let raw_entry =
            RawIndexEntry::try_from_keys([StorageKey::Uint(11)]).expect("encode index entry");
        let mut out = Vec::new();

        let halted = IndexStore::decode_index_entry_and_push(
            entity,
            &TEST_SCAN_INDEX,
            &raw_key,
            &raw_entry,
            &mut out,
            Some(1),
            "test scan",
            None,
        )
        .expect("plain membership scan should not require raw key decode");

        assert!(halted, "bounded single-row scan should stop at the limit");
        assert_eq!(out, vec![DataKey::new(entity, StorageKey::Uint(11))]);
    }

    #[test]
    fn decode_index_entry_and_push_records_single_key_row_check_metrics() {
        let entity = EntityTag::new(7);
        let raw_key = RawIndexKey::from_bytes(Cow::Owned(vec![0xFF]));
        let raw_entry =
            RawIndexEntry::try_from_keys([StorageKey::Uint(11)]).expect("encode index entry");

        let ((halted, out), metrics) = with_row_check_metrics(|| {
            let mut out = Vec::new();
            let halted = IndexStore::decode_index_entry_and_push(
                entity,
                &TEST_SCAN_INDEX,
                &raw_key,
                &raw_entry,
                &mut out,
                Some(1),
                "test scan",
                None,
            )
            .expect("single-key scan should succeed");

            (halted, out)
        });

        assert!(halted, "bounded single-row scan should stop at the limit");
        assert_eq!(out, vec![DataKey::new(entity, StorageKey::Uint(11))]);
        assert_eq!(metrics.index_entries_scanned, 1);
        assert_eq!(metrics.index_membership_single_key_entries, 1);
        assert_eq!(metrics.index_membership_multi_key_entries, 0);
        assert_eq!(metrics.index_membership_keys_decoded, 1);
    }

    #[test]
    fn decode_index_entry_and_push_limit_still_validates_full_multi_key_entry() {
        let entity = EntityTag::new(7);
        let raw_key = RawIndexKey::from_bytes(Cow::Owned(vec![0xFF]));
        let duplicate = StorageKey::Uint(11);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(&duplicate.to_bytes().expect("encode first key"));
        bytes.extend_from_slice(&duplicate.to_bytes().expect("encode second key"));
        let raw_entry = RawIndexEntry::from_bytes(Cow::Owned(bytes));
        let mut out = Vec::new();

        let err = IndexStore::decode_index_entry_and_push(
            entity,
            &TEST_SCAN_INDEX,
            &raw_key,
            &raw_entry,
            &mut out,
            Some(1),
            "test scan",
            None,
        )
        .expect_err("bounded multi-key scan must still reject duplicate membership corruption");

        assert_eq!(err.class(), ErrorClass::Corruption);
    }

    #[test]
    fn decode_index_entry_and_push_records_multi_key_row_check_metrics() {
        let entity = EntityTag::new(7);
        let raw_key = RawIndexKey::from_bytes(Cow::Owned(vec![0xFF]));
        let raw_entry = RawIndexEntry::try_from_keys([StorageKey::Uint(11), StorageKey::Uint(12)])
            .expect("encode multi-key entry");

        let ((halted, out), metrics) = with_row_check_metrics(|| {
            let mut out = Vec::new();
            let halted = IndexStore::decode_index_entry_and_push(
                entity,
                &TEST_SCAN_INDEX,
                &raw_key,
                &raw_entry,
                &mut out,
                Some(2),
                "test scan",
                None,
            )
            .expect("multi-key scan should succeed");

            (halted, out)
        });

        assert!(halted, "bounded multi-key scan should stop at the limit");
        assert_eq!(
            out,
            vec![
                DataKey::new(entity, StorageKey::Uint(11)),
                DataKey::new(entity, StorageKey::Uint(12)),
            ],
        );
        assert_eq!(metrics.index_entries_scanned, 1);
        assert_eq!(metrics.index_membership_single_key_entries, 0);
        assert_eq!(metrics.index_membership_multi_key_entries, 1);
        assert_eq!(metrics.index_membership_keys_decoded, 2);
    }
}
