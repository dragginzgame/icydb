mod constant;
mod snake;
mod title;

use convert_case as cc;
use std::fmt::{self, Display};

pub use snake::to_snake_case;

///
/// Case
///
/// Supported case conversion targets shared across schema, derive, and runtime
/// surfaces.
///

#[derive(Clone, Copy, Debug)]
pub enum Case {
    Camel,
    Constant,
    Kebab,
    Lower,
    Sentence,
    Snake,
    Title,
    Upper,
    UpperCamel,
    UpperSnake,
    UpperKebab,
}

impl Display for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Camel => "Camel",
            Self::Constant => "Constant",
            Self::Kebab => "Kebab",
            Self::Lower => "Lower",
            Self::Sentence => "Sentence",
            Self::Snake => "Snake",
            Self::Title => "Title",
            Self::Upper => "Upper",
            Self::UpperCamel => "UpperCamel",
            Self::UpperSnake => "UpperSnake",
            Self::UpperKebab => "UpperKebab",
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
            Case::Lower => s.to_lowercase(),
            Case::Upper => s.to_uppercase(),
            Case::Title => title::to_title_case(s),
            Case::Snake => snake::to_snake_case(s),
            Case::UpperSnake => snake::to_snake_case(s).to_uppercase(),
            Case::Constant => constant::to_constant_case(s).to_uppercase(),
            Case::Camel => cc::Casing::to_case(s, cc::Case::Camel),
            Case::Kebab => cc::Casing::to_case(s, cc::Case::Kebab),
            Case::Sentence => cc::Casing::to_case(s, cc::Case::Sentence),
            Case::UpperCamel => cc::Casing::to_case(s, cc::Case::UpperCamel),
            Case::UpperKebab => cc::Casing::to_case(s, cc::Case::Kebab).to_uppercase(),
        }
    }

    fn is_case(&self, case: Case) -> bool {
        &self.to_case(case) == self
    }
}
