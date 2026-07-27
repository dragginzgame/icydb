//! Module: case
//! Responsibility: shared text case conversion vocabulary and helpers.
//! Does not own: identifier authority, schema naming policy, or external crate behavior.
//! Boundary: wraps local and external case conversion behind one workspace API.

mod constant;
mod snake;

use std::fmt::{self, Display};

use convert_case as cc;

///
/// Case
///
/// Supported case conversion targets shared across schema, derive, and runtime
/// surfaces.
///

#[derive(Clone, Copy, Debug)]
pub enum Case {
    Constant,
    Snake,
    UpperCamel,
    UpperSnake,
}

impl Display for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Constant => "Constant",
            Self::Snake => "Snake",
            Self::UpperCamel => "UpperCamel",
            Self::UpperSnake => "UpperSnake",
        };

        f.write_str(label)
    }
}

///
/// Casing
///
/// Shared string case conversion surface retained locally so workspace crates
/// do not depend on `canic-utils` for text casing.
///

pub trait Casing<T: std::fmt::Display> {
    /// Convert the receiver into the requested case form.
    fn to_case(&self, case: Case) -> String;

    /// Return whether the receiver is already in the requested case form.
    fn is_case(&self, case: Case) -> bool;
}

impl<T: std::fmt::Display> Casing<T> for T
where
    String: PartialEq<T>,
{
    fn to_case(&self, case: Case) -> String {
        let s = &self.to_string();

        match case {
            Case::Snake => snake::to_snake_case(s),
            Case::UpperSnake => snake::to_snake_case(s).to_uppercase(),
            Case::Constant => constant::to_constant_case(s),
            Case::UpperCamel => cc::Casing::to_case(s, cc::Case::UpperCamel),
        }
    }

    fn is_case(&self, case: Case) -> bool {
        &self.to_case(case) == self
    }
}
