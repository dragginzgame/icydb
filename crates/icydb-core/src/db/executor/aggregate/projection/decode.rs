use crate::{
    error::InternalError,
    types::Ulid,
    value::{Value, ValueTag},
};

// Decode one canonical encoded index component payload into a runtime `Value`.
// Returns `Ok(None)` when this component kind is not supported by the current
// covering fast-path decoder.
pub(super) fn decode_covering_projection_component(
    component: &[u8],
) -> Result<Option<Value>, InternalError> {
    let Some((&tag, payload)) = component.split_first() else {
        return Err(InternalError::index_corruption(
            "index component payload is empty during covering projection decode",
        ));
    };

    if tag == ValueTag::Bool.to_u8() {
        return decode_covering_bool(payload);
    }
    if tag == ValueTag::Int.to_u8() {
        return decode_covering_i64(payload);
    }
    if tag == ValueTag::Uint.to_u8() {
        return decode_covering_u64(payload);
    }
    if tag == ValueTag::Text.to_u8() {
        return decode_covering_text(payload);
    }
    if tag == ValueTag::Ulid.to_u8() {
        return decode_covering_ulid(payload);
    }
    if tag == ValueTag::Unit.to_u8() {
        return Ok(Some(Value::Unit));
    }

    Ok(None)
}

fn decode_covering_bool(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    let Some(value) = payload.first() else {
        return Err(InternalError::index_corruption(
            "bool covering component payload is truncated",
        ));
    };
    if payload.len() != 1 {
        return Err(InternalError::index_corruption(
            "bool covering component payload has invalid length",
        ));
    }

    match *value {
        0 => Ok(Some(Value::Bool(false))),
        1 => Ok(Some(Value::Bool(true))),
        _ => Err(InternalError::index_corruption(
            "bool covering component payload has invalid value",
        )),
    }
}

fn decode_covering_i64(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != 8 {
        return Err(InternalError::index_corruption(
            "int covering component payload has invalid length",
        ));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(payload);
    let biased = u64::from_be_bytes(bytes);
    let unsigned = biased ^ (1u64 << 63);
    let value = i64::from_be_bytes(unsigned.to_be_bytes());

    Ok(Some(Value::Int(value)))
}

fn decode_covering_u64(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != 8 {
        return Err(InternalError::index_corruption(
            "uint covering component payload has invalid length",
        ));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(payload);

    Ok(Some(Value::Uint(u64::from_be_bytes(bytes))))
}

fn decode_covering_text(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    let mut bytes = Vec::new();
    let mut i = 0usize;
    while i < payload.len() {
        let byte = payload[i];
        if byte != 0 {
            bytes.push(byte);
            i = i.saturating_add(1);
            continue;
        }

        let Some(next) = payload.get(i.saturating_add(1)).copied() else {
            return Err(InternalError::index_corruption(
                "text covering component payload has invalid terminator",
            ));
        };
        match next {
            0 => {
                i = i.saturating_add(2);
                if i != payload.len() {
                    return Err(InternalError::index_corruption(
                        "text covering component payload contains trailing bytes",
                    ));
                }

                let text = String::from_utf8(bytes).map_err(|_| {
                    InternalError::index_corruption(
                        "text covering component payload is not valid utf-8",
                    )
                })?;
                return Ok(Some(Value::Text(text)));
            }
            0xFF => {
                bytes.push(0);
                i = i.saturating_add(2);
            }
            _ => {
                return Err(InternalError::index_corruption(
                    "text covering component payload has invalid escape sequence",
                ));
            }
        }
    }

    Err(InternalError::index_corruption(
        "text covering component payload is missing terminator",
    ))
}

fn decode_covering_ulid(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != 16 {
        return Err(InternalError::index_corruption(
            "ulid covering component payload has invalid length",
        ));
    }

    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(payload);

    Ok(Some(Value::Ulid(Ulid::from_bytes(bytes))))
}
