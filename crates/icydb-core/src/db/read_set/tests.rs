use super::*;

fn store(byte: u8, data_revision: u64) -> ReadSetStoreRevision {
    ReadSetStoreRevision::new(
        ReadSetStoreIdentity::from_bytes([byte; 32]),
        data_revision,
        1,
    )
}

fn proof(stores: Vec<ReadSetStoreRevision>) -> Result<ReadSetRevisionProof, ReadSetRevisionError> {
    ReadSetRevisionProof::from_parts([1; 16], 1, 1, [2; 32], stores)
}

#[test]
fn read_set_proof_requires_bounded_canonical_distinct_store_order() {
    assert_eq!(proof(Vec::new()), Err(ReadSetRevisionError::Empty));
    assert_eq!(
        proof(vec![store(2, 1), store(1, 1)]),
        Err(ReadSetRevisionError::NonCanonical),
    );
    assert_eq!(
        proof(vec![store(1, 1), store(1, 2)]),
        Err(ReadSetRevisionError::NonCanonical),
    );

    let too_many = (1..=MAX_READ_SET_PROOF_STORES + 1)
        .map(|index| {
            let mut identity = [0; 32];
            identity[24..].copy_from_slice(
                &u64::try_from(index)
                    .expect("bounded test store count should fit u64")
                    .to_be_bytes(),
            );
            ReadSetStoreRevision::new(ReadSetStoreIdentity::from_bytes(identity), 1, 1)
        })
        .collect();
    assert_eq!(
        proof(too_many),
        Err(ReadSetRevisionError::TooManyStores {
            limit: MAX_READ_SET_PROOF_STORES_U32,
            actual: u32::try_from(MAX_READ_SET_PROOF_STORES + 1)
                .expect("bounded test store count should fit u32"),
        }),
    );
}

#[test]
fn read_set_proof_rejects_zero_authority_or_revision_components() {
    let valid = proof(vec![store(1, 1)]).expect("nonzero canonical proof should admit");
    assert_eq!(
        valid.encoded_len(),
        READ_SET_PROOF_FIXED_BYTES + READ_SET_STORE_ENTRY_BYTES
    );

    assert_eq!(
        ReadSetRevisionProof::from_parts([1; 16], 0, 1, [2; 32], vec![store(1, 1)]),
        Err(ReadSetRevisionError::NonCanonical),
    );
    assert_eq!(
        proof(vec![store(1, 0)]),
        Err(ReadSetRevisionError::NonCanonical),
    );
}
