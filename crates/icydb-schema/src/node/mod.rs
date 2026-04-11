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
/// Shared trait implemented by every schema node emitted by macro/codegen
/// surfaces.
/// `as_any` keeps downcasting local to the schema-node boundary.
///

pub trait MacroNode: Any {
    fn as_any(&self) -> &dyn Any;
}

///
/// TypeNode
///
/// Shared trait for schema nodes that expose one canonical runtime `Type`.
///

pub trait TypeNode: MacroNode {
    fn ty(&self) -> &Type;
}

///
/// ValidateNode
///
/// Trait implemented by schema nodes that can validate their own local
/// invariants against the process-global schema graph.
///

pub trait ValidateNode {
    fn validate(&self) -> Result<(), ErrorTree> {
        Ok(())
    }
}

///
/// VisitableNode
///
/// Trait implemented by schema nodes that participate in recursive visitor
/// traversal.
///

pub trait VisitableNode: ValidateNode {
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
    if memory_id < min || memory_id > max {
        err!(errs, "{label} {memory_id} outside of range {min}-{max}");
    }
}

// Reject memory id values reserved by stable-structures internals.
pub(crate) fn validate_memory_id_not_reserved(errs: &mut ErrorTree, label: &str, memory_id: u8) {
    if memory_id == RESERVED_INTERNAL_MEMORY_ID {
        err!(
            errs,
            "{label} {memory_id} is reserved for stable-structures internals",
        );
    }
}
