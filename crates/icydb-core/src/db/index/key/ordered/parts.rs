use crate::{
    db::index::key::ordered::{OrderedValueEncodeError, encode_canonical_index_component},
    types::{Account, Principal},
    value::ValueEnum,
};

const LENGTH_BYTES: usize = 2;
const MAX_SEGMENT_LEN: usize = u16::MAX as usize;
const ACCOUNT_OWNER_MAX_LEN: usize = Principal::MAX_LENGTH_IN_BYTES as usize;
const ACCOUNT_SUBACCOUNT_LEN: usize = 32;
const ACCOUNT_SUBACCOUNT_TAG: u8 = 0x80;

// Account ordering uses the same tuple contract as `Account::cmp`.
pub(super) fn push_account_payload(
    out: &mut Vec<u8>,
    account: &Account,
) -> Result<(), OrderedValueEncodeError> {
    let owner = account.owner.as_slice();
    let owner_len = owner.len();
    if owner_len > ACCOUNT_OWNER_MAX_LEN {
        return Err(OrderedValueEncodeError::AccountOwnerTooLarge {
            len: owner_len,
            max: ACCOUNT_OWNER_MAX_LEN,
        });
    }

    let mut ordering_tag =
        u8::try_from(owner_len).expect("account owner length should fit in one byte");
    if account.subaccount.is_some() {
        ordering_tag |= ACCOUNT_SUBACCOUNT_TAG;
    }

    out.push(ordering_tag);

    let mut owner_padded = [0u8; ACCOUNT_OWNER_MAX_LEN];
    owner_padded[..owner_len].copy_from_slice(&owner[..owner_len]);
    out.extend_from_slice(&owner_padded);

    let subaccount = account.subaccount.unwrap_or_default().to_array();
    let _ = ACCOUNT_SUBACCOUNT_LEN;
    out.extend_from_slice(&subaccount);

    Ok(())
}

// Enum ordering is variant -> path option -> payload option, recursively.
pub(super) fn push_enum_payload(
    out: &mut Vec<u8>,
    value: &ValueEnum,
) -> Result<(), OrderedValueEncodeError> {
    push_terminated_bytes(out, value.variant.as_bytes());

    match &value.path {
        Some(path) => {
            out.push(1);
            push_terminated_bytes(out, path.as_bytes());
        }
        None => out.push(0),
    }

    match &value.payload {
        Some(payload) => {
            out.push(1);

            let payload_bytes = encode_canonical_index_component(payload)?;
            push_len_prefixed_bytes(out, &payload_bytes)?;
        }
        None => out.push(0),
    }

    Ok(())
}

// Byte strings are escaped so tuple boundaries remain unambiguous.
// Segment size bounds for these terminated payloads are enforced by the outer
// index-key component caps in `IndexKey`, not at this primitive encoder layer.
pub(super) fn push_terminated_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    for &byte in bytes {
        if byte == 0 {
            out.extend_from_slice(&[0, 0xFF]);
        } else {
            out.push(byte);
        }
    }

    out.extend_from_slice(&[0, 0]);
}

fn push_len_prefixed_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), OrderedValueEncodeError> {
    let len = encode_segment_len(bytes.len())?;
    out.extend_from_slice(&len);
    out.extend_from_slice(bytes);
    Ok(())
}

pub(super) fn push_inverted(out: &mut Vec<u8>, bytes: &[u8]) {
    for &byte in bytes {
        out.push(!byte);
    }
}

pub(super) fn encode_segment_len(
    len: usize,
) -> Result<[u8; LENGTH_BYTES], OrderedValueEncodeError> {
    let len_u16 = u16::try_from(len).map_err(|_| OrderedValueEncodeError::SegmentTooLarge {
        len,
        max: MAX_SEGMENT_LEN,
    })?;

    Ok(len_u16.to_be_bytes())
}
