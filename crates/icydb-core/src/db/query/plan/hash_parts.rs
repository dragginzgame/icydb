//! Shared deterministic hash encoding for plan fingerprinting and continuation signatures.
#![allow(clippy::cast_possible_truncation)]

use crate::{
    db::{
        index::fingerprint::hash_value,
        query::{
            QueryMode,
            plan::{ExplainAccessPath, ExplainOrderBy, ExplainPredicate, OrderDirection},
            predicate::coercion::CoercionId,
        },
    },
    value::Value,
};
use sha2::{Digest, Sha256};

///
/// Hash explain access paths into the plan hash stream.
///

pub fn hash_access(hasher: &mut Sha256, access: &ExplainAccessPath) {
    match access {
        ExplainAccessPath::ByKey { key } => {
            write_tag(hasher, 0x10);
            write_value(hasher, key);
        }
        ExplainAccessPath::ByKeys { keys } => {
            write_tag(hasher, 0x11);
            write_u32(hasher, keys.len() as u32);
            for key in keys {
                write_value(hasher, key);
            }
        }
        ExplainAccessPath::KeyRange { start, end } => {
            write_tag(hasher, 0x12);
            write_value(hasher, start);
            write_value(hasher, end);
        }
        ExplainAccessPath::IndexPrefix {
            name,
            fields,
            prefix_len,
            values,
        } => {
            write_tag(hasher, 0x13);
            write_str(hasher, name);
            write_u32(hasher, fields.len() as u32);
            for field in fields {
                write_str(hasher, field);
            }
            write_u32(hasher, *prefix_len as u32);
            write_u32(hasher, values.len() as u32);
            for value in values {
                write_value(hasher, value);
            }
        }
        ExplainAccessPath::FullScan => {
            write_tag(hasher, 0x14);
        }
        ExplainAccessPath::Union(children) => {
            write_tag(hasher, 0x15);
            write_u32(hasher, children.len() as u32);
            for child in children {
                hash_access(hasher, child);
            }
        }
        ExplainAccessPath::Intersection(children) => {
            write_tag(hasher, 0x16);
            write_u32(hasher, children.len() as u32);
            for child in children {
                hash_access(hasher, child);
            }
        }
    }
}

///
/// Hash explain predicates into the plan hash stream.
///

pub fn hash_predicate(hasher: &mut Sha256, predicate: &ExplainPredicate) {
    match predicate {
        ExplainPredicate::None => write_tag(hasher, 0x20),
        ExplainPredicate::True => write_tag(hasher, 0x21),
        ExplainPredicate::False => write_tag(hasher, 0x22),
        ExplainPredicate::And(children) => {
            write_tag(hasher, 0x23);
            write_u32(hasher, children.len() as u32);
            for child in children {
                hash_predicate(hasher, child);
            }
        }
        ExplainPredicate::Or(children) => {
            write_tag(hasher, 0x24);
            write_u32(hasher, children.len() as u32);
            for child in children {
                hash_predicate(hasher, child);
            }
        }
        ExplainPredicate::Not(inner) => {
            write_tag(hasher, 0x25);
            hash_predicate(hasher, inner);
        }
        ExplainPredicate::Compare {
            field,
            op,
            value,
            coercion,
        } => {
            write_tag(hasher, 0x26);
            write_str(hasher, field);
            write_tag(hasher, op.tag());
            write_value(hasher, value);
            hash_coercion(hasher, coercion.id, &coercion.params);
        }
        ExplainPredicate::IsNull { field } => {
            write_tag(hasher, 0x27);
            write_str(hasher, field);
        }
        ExplainPredicate::IsMissing { field } => {
            write_tag(hasher, 0x28);
            write_str(hasher, field);
        }
        ExplainPredicate::IsEmpty { field } => {
            write_tag(hasher, 0x29);
            write_str(hasher, field);
        }
        ExplainPredicate::IsNotEmpty { field } => {
            write_tag(hasher, 0x2a);
            write_str(hasher, field);
        }
        ExplainPredicate::TextContains { field, value } => {
            write_tag(hasher, 0x2e);
            write_str(hasher, field);
            write_value(hasher, value);
        }
        ExplainPredicate::TextContainsCi { field, value } => {
            write_tag(hasher, 0x2f);
            write_str(hasher, field);
            write_value(hasher, value);
        }
    }
}

///
/// Hash explain order specs into the plan hash stream.
///

pub fn hash_order(hasher: &mut Sha256, order: &ExplainOrderBy) {
    match order {
        ExplainOrderBy::None => write_tag(hasher, 0x30),
        ExplainOrderBy::Fields(fields) => {
            write_tag(hasher, 0x31);
            write_u32(hasher, fields.len() as u32);
            for field in fields {
                write_str(hasher, &field.field);
                write_tag(hasher, order_direction_tag(field.direction));
            }
        }
    }
}

///
/// Hash query mode into the plan hash stream.
///

pub fn hash_mode(hasher: &mut Sha256, mode: QueryMode) {
    match mode {
        QueryMode::Load(_) => write_tag(hasher, 0x60),
        QueryMode::Delete(_) => write_tag(hasher, 0x61),
    }
}

///
/// Hash coercion information into the plan hash stream.
///

pub fn hash_coercion(
    hasher: &mut Sha256,
    id: CoercionId,
    params: &std::collections::BTreeMap<String, String>,
) {
    write_tag(hasher, id.plan_hash_tag());
    write_u32(hasher, params.len() as u32);
    for (key, value) in params {
        write_str(hasher, key);
        write_str(hasher, value);
    }
}

///
/// Encode one value digest into the plan hash stream.
///

pub fn write_value(hasher: &mut Sha256, value: &Value) {
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

pub fn write_str(hasher: &mut Sha256, value: &str) {
    write_u32(hasher, value.len() as u32);
    hasher.update(value.as_bytes());
}

///
/// Encode one u32 in network byte order into the plan hash stream.
///

pub fn write_u32(hasher: &mut Sha256, value: u32) {
    hasher.update(value.to_be_bytes());
}

///
/// Encode one tag byte into the plan hash stream.
///

pub fn write_tag(hasher: &mut Sha256, tag: u8) {
    hasher.update([tag]);
}

const fn order_direction_tag(direction: OrderDirection) -> u8 {
    match direction {
        OrderDirection::Asc => 0x01,
        OrderDirection::Desc => 0x02,
    }
}
