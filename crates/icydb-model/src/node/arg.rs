use crate::{
    node::{ValidateNode, VisitableNode},
    visit::Visitor,
};
use serde::Serialize;

///
/// Arg
///

#[derive(Clone, Debug, Serialize)]
pub enum Arg {
    /// A Boolean constructor argument.
    Bool(bool),
    /// A Unicode scalar constructor argument.
    Char(char),
    /// A numeric argument retaining its authored width and signedness.
    Number(ArgNumber),
    /// A Rust constant path used as a constructor argument.
    ConstPath(&'static str),
    /// A Rust function path invoked to supply a constructor argument.
    FuncPath(&'static str),
    /// A literal string constructor argument.
    String(&'static str),
}

impl ValidateNode for Arg {}

impl VisitableNode for Arg {
    fn route_key(&self) -> String {
        format!("arg ({self:?})")
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        if let Self::Number(node) = self {
            node.accept(v);
        }
    }
}

///
/// Args
///

#[derive(Clone, Debug, Serialize)]
pub struct Args(pub &'static [Arg]);

impl ValidateNode for Args {}

///
/// ArgNumber
///

#[derive(Clone, Debug, Serialize)]
pub enum ArgNumber {
    /// A 32-bit floating-point constructor argument.
    Float32(f32),
    /// A 64-bit floating-point constructor argument.
    Float64(f64),
    /// A 8-bit signed integer constructor argument.
    Int8(i8),
    /// A 16-bit signed integer constructor argument.
    Int16(i16),
    /// A 32-bit signed integer constructor argument.
    Int32(i32),
    /// A 64-bit signed integer constructor argument.
    Int64(i64),
    /// A 128-bit signed integer constructor argument.
    Int128(i128),
    /// A 8-bit unsigned integer constructor argument.
    Nat8(u8),
    /// A 16-bit unsigned integer constructor argument.
    Nat16(u16),
    /// A 32-bit unsigned integer constructor argument.
    Nat32(u32),
    /// A 64-bit unsigned integer constructor argument.
    Nat64(u64),
    /// A 128-bit unsigned integer constructor argument.
    Nat128(u128),
}

impl ValidateNode for ArgNumber {}

impl VisitableNode for ArgNumber {}
