use super::*;
use crate::error::{ErrorClass, ErrorOrigin};

#[test]
fn raw_row_rejects_oversized_payload() {
    let bytes = vec![0u8; MAX_ROW_BYTES as usize + 1];
    let err = RawRow::try_new(bytes).unwrap_err();
    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn raw_row_storable_bound_does_not_amplify_stable_btree_nodes() {
    assert_eq!(RawRow::BOUND, Bound::Unbounded);
}

#[test]
fn raw_row_accepts_protocol_limit() {
    let row = RawRow::try_new(vec![0; MAX_ROW_BYTES as usize])
        .expect("row at the protocol limit should admit");
    assert_eq!(row.len(), MAX_ROW_BYTES as usize);
}
