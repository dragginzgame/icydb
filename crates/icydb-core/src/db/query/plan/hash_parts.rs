//! Shared deterministic hash encoding for plan fingerprinting and continuation signatures.
#![expect(clippy::cast_possible_truncation)]

use crate::{
    db::{
        index::fingerprint::hash_value,
        query::{
            QueryMode, ReadConsistency,
            plan::{
                ExplainAccessPath, ExplainDeleteLimit, ExplainOrderBy, ExplainPagination,
                ExplainPlan, ExplainPredicate, OrderDirection,
            },
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

///
/// ExplainHashProfile
///
/// Hashing profiles that select canonical explain-surface fields.
///

pub enum ExplainHashProfile<'a> {
    FingerprintV2,
    ContinuationV1 { entity_path: &'a str },
}

#[derive(Clone, Copy)]
enum ExplainHashField {
    EntityPath,
    Mode,
    Access,
    Predicate,
    Order,
    Page,
    DeleteLimit,
    Consistency,
    ProjectionDefault,
}

#[derive(Clone, Copy)]
struct ExplainHashStep {
    section_tag: u8,
    field: ExplainHashField,
}

struct ExplainHashProfileSpec<'a> {
    entity_path: Option<&'a str>,
    steps: &'static [ExplainHashStep],
}

const FINGERPRINT_V2_STEPS: [ExplainHashStep; 7] = [
    ExplainHashStep {
        section_tag: 0x01,
        field: ExplainHashField::Access,
    },
    ExplainHashStep {
        section_tag: 0x02,
        field: ExplainHashField::Predicate,
    },
    ExplainHashStep {
        section_tag: 0x03,
        field: ExplainHashField::Order,
    },
    ExplainHashStep {
        section_tag: 0x04,
        field: ExplainHashField::Page,
    },
    ExplainHashStep {
        section_tag: 0x05,
        field: ExplainHashField::DeleteLimit,
    },
    ExplainHashStep {
        section_tag: 0x06,
        field: ExplainHashField::Consistency,
    },
    ExplainHashStep {
        section_tag: 0x07,
        field: ExplainHashField::Mode,
    },
];

const CONTINUATION_V1_STEPS: [ExplainHashStep; 6] = [
    ExplainHashStep {
        section_tag: 0x01,
        field: ExplainHashField::EntityPath,
    },
    ExplainHashStep {
        section_tag: 0x02,
        field: ExplainHashField::Mode,
    },
    ExplainHashStep {
        section_tag: 0x03,
        field: ExplainHashField::Access,
    },
    ExplainHashStep {
        section_tag: 0x04,
        field: ExplainHashField::Predicate,
    },
    ExplainHashStep {
        section_tag: 0x05,
        field: ExplainHashField::Order,
    },
    ExplainHashStep {
        section_tag: 0x06,
        field: ExplainHashField::ProjectionDefault,
    },
];

impl<'a> ExplainHashProfile<'a> {
    const fn spec(self) -> ExplainHashProfileSpec<'a> {
        match self {
            Self::FingerprintV2 => ExplainHashProfileSpec {
                entity_path: None,
                steps: &FINGERPRINT_V2_STEPS,
            },
            Self::ContinuationV1 { entity_path } => ExplainHashProfileSpec {
                entity_path: Some(entity_path),
                steps: &CONTINUATION_V1_STEPS,
            },
        }
    }
}

fn hash_explain_field(
    hasher: &mut Sha256,
    plan: &ExplainPlan,
    field: ExplainHashField,
    entity_path: Option<&str>,
) {
    match field {
        ExplainHashField::EntityPath => {
            let entity_path = entity_path.expect("entity path required by hash profile");
            write_str(hasher, entity_path);
        }
        ExplainHashField::Mode => hash_mode(hasher, plan.mode),
        ExplainHashField::Access => hash_access(hasher, &plan.access),
        ExplainHashField::Predicate => hash_predicate(hasher, &plan.predicate),
        ExplainHashField::Order => hash_order(hasher, &plan.order_by),
        ExplainHashField::Page => hash_page(hasher, &plan.page),
        ExplainHashField::DeleteLimit => hash_delete_limit(hasher, &plan.delete_limit),
        ExplainHashField::Consistency => hash_consistency(hasher, plan.consistency),
        ExplainHashField::ProjectionDefault => hash_projection_default(hasher),
    }
}

/// Hash an `ExplainPlan` using a profile-specific canonical field set.
pub fn hash_explain_plan_profile(
    hasher: &mut Sha256,
    plan: &ExplainPlan,
    profile: ExplainHashProfile<'_>,
) {
    let spec = profile.spec();
    for step in spec.steps {
        write_tag(hasher, step.section_tag);
        hash_explain_field(hasher, plan, step.field, spec.entity_path);
    }
}

fn hash_page(hasher: &mut Sha256, page: &ExplainPagination) {
    match page {
        ExplainPagination::None => write_tag(hasher, 0x40),
        ExplainPagination::Page { limit, offset } => {
            write_tag(hasher, 0x41);
            match limit {
                Some(limit) => {
                    write_tag(hasher, 0x01);
                    write_u32(hasher, *limit);
                }
                None => write_tag(hasher, 0x00),
            }
            write_u32(hasher, *offset);
        }
    }
}

fn hash_delete_limit(hasher: &mut Sha256, limit: &ExplainDeleteLimit) {
    match limit {
        ExplainDeleteLimit::None => write_tag(hasher, 0x42),
        ExplainDeleteLimit::Limit { max_rows } => {
            write_tag(hasher, 0x43);
            write_u32(hasher, *max_rows);
        }
    }
}

fn hash_consistency(hasher: &mut Sha256, consistency: ReadConsistency) {
    match consistency {
        ReadConsistency::MissingOk => write_tag(hasher, 0x50),
        ReadConsistency::Strict => write_tag(hasher, 0x51),
    }
}

// Phase 1 projection surface is always full row `(Id<E>, E)`.
fn hash_projection_default(hasher: &mut Sha256) {
    write_tag(hasher, 0x70);
}
