//! Schema node graph for validated canister/entity/type definitions.
//!
//! This module owns the typed node descriptors used by schema validation,
//! derive code generation, and visitor traversal.

mod arg;
mod canister;
mod def;
mod entity;
mod r#enum;
mod field;
mod index;
mod item;
mod list;
mod map;
mod newtype;
mod primary_key;
mod record;
mod sanitizer;
mod schema;
mod set;
mod store;
mod tuple;
mod r#type;
mod validator;
mod value;

use crate::{
    prelude::*,
    visit::{Event, Visitor},
};
use std::any::Any;
use thiserror::Error as ThisError;

pub use arg::*;
pub use canister::*;
pub use def::*;
pub use entity::*;
pub use r#enum::*;
pub use field::*;
pub use index::*;
pub use item::*;
pub use list::*;
pub use map::*;
pub use newtype::*;
pub use primary_key::*;
pub use record::*;
pub use sanitizer::*;
pub use schema::*;
pub use set::*;
pub use store::*;
pub use tuple::*;
pub use r#type::*;
pub use validator::*;
pub use value::*;

pub const APP_MEMORY_ID_MIN: u8 = 100;
pub const APP_MEMORY_ID_MAX: u8 = 254;
const RESERVED_INTERNAL_MEMORY_ID: u8 = u8::MAX;

///
/// NodeError
///
/// Error raised when schema-node lookup or downcasting crosses an invalid
/// boundary.
///

#[derive(Debug, ThisError)]
pub enum NodeError {
    #[error("{0} is an incorrect node type")]
    IncorrectNodeType(String),

    #[error("path not found: {0}")]
    PathNotFound(String),
}

///
/// NODE TRAITS
///

///
/// MacroNode
///
/// Shared trait implemented by every concrete schema node descriptor.
/// `as_any` keeps type erasure and downcasting local to the schema-node
/// boundary instead of leaking it into callers.
///

pub(crate) trait MacroNode: Any {
    fn as_any(&self) -> &dyn Any;
}

///
/// ValidateNode
///
/// Trait implemented by schema nodes that validate local invariants against
/// the surrounding schema graph.
///

pub(crate) trait ValidateNode {
    fn validate(&self) -> Result<(), ErrorTree> {
        Ok(())
    }
}

///
/// VisitableNode
///
/// Trait implemented by schema nodes that participate in recursive visitor
/// traversal with canonical route-key ordering.
///

pub(crate) trait VisitableNode: ValidateNode {
    // Route key contributes one node-local path segment to the visitor path.
    fn route_key(&self) -> String {
        String::new()
    }

    // Drive the enter/children/exit visitor sequence for this node.
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.push(&self.route_key());
        visitor.visit(self, Event::Enter);
        self.drive(visitor);
        visitor.visit(self, Event::Exit);
        visitor.pop();
    }

    // Visit child nodes in canonical order.
    fn drive<V: Visitor>(&self, _: &mut V) {}
}

// Validate one memory id against the declared canister range.
pub(crate) fn validate_memory_id_in_range(
    errs: &mut ErrorTree,
    label: &str,
    memory_id: u8,
    min: u8,
    max: u8,
) {
    if !memory_id_is_in_range(memory_id, min, max) {
        err!(errs, "{label} {memory_id} outside of range {min}-{max}");
    }
}

// Reject memory id values reserved by stable-structures internals.
pub(crate) fn validate_memory_id_not_reserved(errs: &mut ErrorTree, label: &str, memory_id: u8) {
    if memory_id_is_reserved(memory_id) {
        err!(
            errs,
            "{label} {memory_id} is reserved for stable-structures internals",
        );
    }
}

// Validate one application-owned memory id against IcyDB's generated-store range.
pub(crate) fn validate_app_memory_id(errs: &mut ErrorTree, label: &str, memory_id: u8) {
    if !app_memory_id_is_valid(memory_id) {
        err!(
            errs,
            "{label} {memory_id} outside of application memory range {APP_MEMORY_ID_MIN}-{APP_MEMORY_ID_MAX}",
        );
    }
}

#[must_use]
pub const fn memory_id_is_in_range(memory_id: u8, min: u8, max: u8) -> bool {
    memory_id >= min && memory_id <= max
}

#[must_use]
pub const fn memory_id_is_reserved(memory_id: u8) -> bool {
    memory_id == RESERVED_INTERNAL_MEMORY_ID
}

#[must_use]
pub const fn app_memory_id_is_valid(memory_id: u8) -> bool {
    memory_id >= APP_MEMORY_ID_MIN && memory_id <= APP_MEMORY_ID_MAX
}

pub(crate) fn validate_stable_key_segment(errs: &mut ErrorTree, label: &str, value: &str) {
    if !stable_key_segment_is_canonical(value) {
        err!(
            errs,
            "{label} `{value}` must use lowercase ASCII letters, digits, and underscores",
        );
    }
}

pub(crate) fn validate_stable_key(errs: &mut ErrorTree, label: &str, value: &str) {
    if !stable_key_is_canonical(value) {
        err!(
            errs,
            "{label} `{value}` must be canonical lowercase ASCII, must use dots as separators, must use underscores instead of hyphens, must end in .v1, and must not start with canic.",
        );
    }
}

#[must_use]
pub fn stable_key_segment_is_canonical(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[must_use]
pub(crate) fn stable_key_is_canonical(value: &str) -> bool {
    if value.starts_with("canic.") {
        return false;
    }

    let mut saw_segment = false;
    let mut last_segment = "";
    for segment in value.split('.') {
        if !stable_key_segment_is_canonical(segment) {
            return false;
        }
        saw_segment = true;
        last_segment = segment;
    }

    saw_segment && last_segment == "v1"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_memory_id_policy_accepts_only_application_range() {
        for memory_id in APP_MEMORY_ID_MIN..=APP_MEMORY_ID_MAX {
            let mut errors = ErrorTree::new();
            validate_app_memory_id(&mut errors, "memory_id", memory_id);
            validate_memory_id_not_reserved(&mut errors, "memory_id", memory_id);
            assert!(
                errors.is_empty(),
                "schema should accept app memory id {memory_id}: {errors}",
            );
        }

        for memory_id in [0, APP_MEMORY_ID_MIN - 1] {
            let mut errors = ErrorTree::new();
            validate_app_memory_id(&mut errors, "memory_id", memory_id);
            assert!(
                !errors.is_empty(),
                "schema should reject below-range app memory id {memory_id}",
            );
        }

        let mut errors = ErrorTree::new();
        validate_app_memory_id(&mut errors, "memory_id", u8::MAX);
        validate_memory_id_not_reserved(&mut errors, "memory_id", u8::MAX);
        let rendered = errors.to_string();
        assert!(
            rendered.contains("outside of application memory range 100-254"),
            "reserved id should also fail the app range check: {rendered}",
        );
        assert!(
            rendered.contains("reserved for stable-structures internals"),
            "reserved id should fail closed explicitly: {rendered}",
        );
    }

    #[test]
    fn stable_key_segment_policy_is_canonical_ascii_only() {
        for segment in ["db", "demo_rpg", "store_1", "v1"] {
            assert!(stable_key_segment_is_canonical(segment));
        }

        for segment in ["", "Demo", "demo-rpg", "demo.rpg", "canic.owned"] {
            assert!(!stable_key_segment_is_canonical(segment));
        }
    }

    #[test]
    fn full_stable_key_policy_rejects_reserved_and_malformed_keys() {
        assert!(stable_key_is_canonical("icydb.demo_rpg.characters.data.v1"));

        for key in [
            "canic.demo_rpg.characters.data.v1",
            "icydb.demo_rpg.characters.data",
            "icydb.demo-rpg.characters.data.v1",
            "icydb.demo_rpg..data.v1",
            "icydb.Demo.characters.data.v1",
            "icydb.demo_rpg.characters.data.v2",
        ] {
            assert!(!stable_key_is_canonical(key), "key should fail: {key}");
        }
    }
}
