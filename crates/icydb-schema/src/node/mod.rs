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
mod memory_id;
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
pub(crate) use memory_id::{validate_memory_id_in_range, validate_memory_id_not_reserved};
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

///
/// NodeError
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
/// shared traits for every node that is created via a macro
/// as_any has to be implemented on each type manually
///

pub trait MacroNode: Any {
    fn as_any(&self) -> &dyn Any;
}

///
/// TypeNode
/// shared traits for every type node
///

pub trait TypeNode: MacroNode {
    fn ty(&self) -> &Type;
}

///
/// ValidateNode
///

pub trait ValidateNode {
    fn validate(&self) -> Result<(), ErrorTree> {
        Ok(())
    }
}

///
/// VisitableNode
///

pub trait VisitableNode: ValidateNode {
    // route_key
    fn route_key(&self) -> String {
        String::new()
    }

    // accept
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.push(&self.route_key());
        visitor.visit(self, Event::Enter);
        self.drive(visitor);
        visitor.visit(self, Event::Exit);
        visitor.pop();
    }

    // drive
    fn drive<V: Visitor>(&self, _: &mut V) {}
}
