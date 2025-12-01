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
mod record;
mod sanitizer;
mod schema;
mod set;
mod store;
mod tuple;
mod r#type;
mod validator;
mod value;

pub use self::arg::*;
pub use self::canister::*;
pub use self::def::*;
pub use self::entity::*;
pub use self::r#enum::*;
pub use self::field::*;
pub use self::index::*;
pub use self::item::*;
pub use self::list::*;
pub use self::map::*;
pub use self::newtype::*;
pub use self::record::*;
pub use self::sanitizer::*;
pub use self::schema::*;
pub use self::set::*;
pub use self::store::*;
pub use self::tuple::*;
pub use self::r#type::*;
pub use self::validator::*;
pub use self::value::*;

use crate::{
    prelude::*,
    visit::{Event, Visitor},
};
use std::any::Any;
use thiserror::Error as ThisError;

///
/// NodeError
///

#[derive(Debug, ThisError)]
pub enum NodeError {
    #[error("error downcasting schema node: {0}")]
    DowncastFail(String),

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
