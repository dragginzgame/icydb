use super::Blob;
use crate::{MAX_PROPOSAL_LITERAL_BYTES, ScalarLiteral, SchemaContractError};

#[test]
fn blob_candid_round_trip_accepts_runtime_values_above_the_proposal_literal_bound() {
    let blob = Blob::from(vec![0xAB; MAX_PROPOSAL_LITERAL_BYTES + 1]);
    let encoded = candid::encode_one(&blob).expect("runtime blob should encode");
    let encoded_bytes =
        candid::encode_one(blob.as_bytes()).expect("canonical byte vector should encode");
    let decoded = candid::decode_one::<Blob>(&encoded).expect("runtime blob should decode");

    assert_eq!(encoded, encoded_bytes);
    assert_eq!(decoded, blob);
}

#[test]
fn blob_proposal_literals_retain_their_dedicated_bound() {
    let maximum = ScalarLiteral::Blob(Blob::from(vec![0; MAX_PROPOSAL_LITERAL_BYTES]));
    let oversized = ScalarLiteral::Blob(Blob::from(vec![0; MAX_PROPOSAL_LITERAL_BYTES + 1]));

    assert_eq!(maximum.validate(), Ok(()));
    assert_eq!(
        oversized.validate(),
        Err(SchemaContractError::InvalidLiteral),
    );
}
