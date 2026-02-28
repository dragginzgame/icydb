use crate::{
    db::predicate::{CoercionId, Predicate},
    value::{Value, hash_value},
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Hash predicate structure into the plan hash stream.
pub(in crate::db) fn hash_predicate(hasher: &mut Sha256, predicate: &Predicate) {
    match predicate {
        Predicate::True => write_tag(hasher, 0x21),
        Predicate::False => write_tag(hasher, 0x22),
        Predicate::And(children) => {
            write_tag(hasher, 0x23);
            write_len_u32(hasher, children.len());
            for child in children {
                hash_predicate(hasher, child);
            }
        }
        Predicate::Or(children) => {
            write_tag(hasher, 0x24);
            write_len_u32(hasher, children.len());
            for child in children {
                hash_predicate(hasher, child);
            }
        }
        Predicate::Not(inner) => {
            write_tag(hasher, 0x25);
            hash_predicate(hasher, inner);
        }
        Predicate::Compare(compare) => {
            write_tag(hasher, 0x26);
            write_str(hasher, &compare.field);
            write_tag(hasher, compare.op.tag());
            write_value(hasher, &compare.value);
            hash_coercion(hasher, compare.coercion.id, &compare.coercion.params);
        }
        Predicate::IsNull { field } => {
            write_tag(hasher, 0x27);
            write_str(hasher, field);
        }
        Predicate::IsMissing { field } => {
            write_tag(hasher, 0x28);
            write_str(hasher, field);
        }
        Predicate::IsEmpty { field } => {
            write_tag(hasher, 0x29);
            write_str(hasher, field);
        }
        Predicate::IsNotEmpty { field } => {
            write_tag(hasher, 0x2a);
            write_str(hasher, field);
        }
        Predicate::TextContains { field, value } => {
            write_tag(hasher, 0x2e);
            write_str(hasher, field);
            write_value(hasher, value);
        }
        Predicate::TextContainsCi { field, value } => {
            write_tag(hasher, 0x2f);
            write_str(hasher, field);
            write_value(hasher, value);
        }
    }
}

/// Hash coercion information into the plan hash stream.
pub(in crate::db) fn hash_coercion(
    hasher: &mut Sha256,
    id: CoercionId,
    params: &BTreeMap<String, String>,
) {
    write_tag(hasher, id.plan_hash_tag());
    write_len_u32(hasher, params.len());
    for (key, value) in params {
        write_str(hasher, key);
        write_str(hasher, value);
    }
}

///
/// Encode one value digest into the plan hash stream.
///

fn write_value(hasher: &mut Sha256, value: &Value) {
    match hash_value(value) {
        Ok(digest) => hasher.update(digest),
        Err(err) => {
            write_tag(hasher, 0xEE);
            write_str(hasher, &err.display_with_class());
        }
    }
}

///
/// Encode one string with length prefix into the plan hash stream.
///

fn write_str(hasher: &mut Sha256, value: &str) {
    write_len_u32(hasher, value.len());
    hasher.update(value.as_bytes());
}

/// Encode a platform-sized length as u32 with deterministic saturation.
fn write_len_u32(hasher: &mut Sha256, len: usize) {
    let len = u32::try_from(len).unwrap_or(u32::MAX);
    write_u32(hasher, len);
}

///
/// Encode one u32 in network byte order into the plan hash stream.
///

fn write_u32(hasher: &mut Sha256, value: u32) {
    hasher.update(value.to_be_bytes());
}

///
/// Encode one tag byte into the plan hash stream.
///

fn write_tag(hasher: &mut Sha256, tag: u8) {
    hasher.update([tag]);
}
