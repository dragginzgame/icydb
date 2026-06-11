//! Module: visitor::context
//! Responsibility: visitor issue-reporting context and path scoping helpers.
//! Does not own: concrete sanitize/validate traversal behavior.
//! Boundary: shared diagnostics context passed through visitor entrypoints.

use crate::{sanitize::SanitizeWriteContext, types::Decimal};
use serde::Deserialize;

///
/// VisitorContext
///
/// Narrow interface exposed to visitors for reporting non-fatal issues.
/// Implemented by adapters via a short-lived context object.
///

pub trait VisitorContext {
    fn add_issue(&mut self, issue: Issue);
    fn add_issue_at(&mut self, seg: PathSegment, issue: Issue);

    fn sanitize_write_context(&self) -> Option<SanitizeWriteContext> {
        None
    }
}

impl dyn VisitorContext + '_ {
    pub fn issue(&mut self, issue: Issue) {
        self.add_issue(issue);
    }

    pub fn issue_at(&mut self, seg: PathSegment, issue: Issue) {
        self.add_issue_at(seg, issue);
    }
}

/// VisitorContext that pins all issues to a single path segment.
pub struct ScopedContext<'a> {
    ctx: &'a mut dyn VisitorContext,
    seg: PathSegment,
}

impl<'a> ScopedContext<'a> {
    #[must_use]
    pub fn new(ctx: &'a mut dyn VisitorContext, seg: PathSegment) -> Self {
        Self { ctx, seg }
    }
}

impl VisitorContext for ScopedContext<'_> {
    fn add_issue(&mut self, issue: Issue) {
        self.ctx.add_issue_at(self.seg.clone(), issue);
    }

    fn add_issue_at(&mut self, _seg: PathSegment, issue: Issue) {
        self.ctx.add_issue_at(self.seg.clone(), issue);
    }

    fn sanitize_write_context(&self) -> Option<SanitizeWriteContext> {
        self.ctx.sanitize_write_context()
    }
}

///
/// Issue
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum Issue {
    UlidNil,
    Float32NonFinite,
    Float64NonFinite,
    Utf8Invalid,
    LengthEqual {
        actual: usize,
        expected: usize,
    },
    LengthMin {
        actual: usize,
        min: usize,
    },
    LengthMax {
        actual: usize,
        max: usize,
    },
    LengthRange {
        actual: usize,
        min: usize,
        max: usize,
    },
    NumericNotRepresentableAsDecimal,
    NumericComparison {
        actual: Decimal,
        op: IssueComparisonOp,
        expected: Decimal,
    },
    NumericRange {
        actual: Decimal,
        min: Decimal,
        max: Decimal,
    },
    NumericMultipleOfZero,
    NumericMultipleOf {
        actual: Decimal,
        target: Decimal,
    },
    DecimalScaleMax {
        actual: u32,
        max: u32,
    },
    CollectionValueNotAllowed {
        allowed_count: usize,
    },
    Sha256Length {
        actual: usize,
    },
    Sha256NonHex,
    MimeSlashCount,
    MimeInvalidChars,
    UrlScheme,
    TextPattern {
        pattern: IssueTextPattern,
    },
    ColorHex {
        width: u8,
    },
    PhoneMissingPlus,
    PhoneDigitCount {
        digits: usize,
    },
    Iso3166CountryCode,
    Iso639LanguageCode,
    SanitizerRejected,
    UnspecifiedEnumVariant,
}

impl Issue {
    #[must_use]
    pub const fn code(&self) -> IssueCode {
        match self {
            Self::UlidNil => IssueCode::UlidNil,
            Self::Float32NonFinite => IssueCode::Float32NonFinite,
            Self::Float64NonFinite => IssueCode::Float64NonFinite,
            Self::Utf8Invalid => IssueCode::Utf8Invalid,
            Self::LengthEqual { .. } => IssueCode::LengthEqual,
            Self::LengthMin { .. } => IssueCode::LengthMin,
            Self::LengthMax { .. } => IssueCode::LengthMax,
            Self::LengthRange { .. } => IssueCode::LengthRange,
            Self::NumericNotRepresentableAsDecimal => IssueCode::NumericNotRepresentableAsDecimal,
            Self::NumericComparison { .. } => IssueCode::NumericComparison,
            Self::NumericRange { .. } => IssueCode::NumericRange,
            Self::NumericMultipleOfZero => IssueCode::NumericMultipleOfZero,
            Self::NumericMultipleOf { .. } => IssueCode::NumericMultipleOf,
            Self::DecimalScaleMax { .. } => IssueCode::DecimalScaleMax,
            Self::CollectionValueNotAllowed { .. } => IssueCode::CollectionValueNotAllowed,
            Self::Sha256Length { .. } => IssueCode::Sha256Length,
            Self::Sha256NonHex => IssueCode::Sha256NonHex,
            Self::MimeSlashCount => IssueCode::MimeSlashCount,
            Self::MimeInvalidChars => IssueCode::MimeInvalidChars,
            Self::UrlScheme => IssueCode::UrlScheme,
            Self::TextPattern { .. } => IssueCode::TextPattern,
            Self::ColorHex { .. } => IssueCode::ColorHex,
            Self::PhoneMissingPlus => IssueCode::PhoneMissingPlus,
            Self::PhoneDigitCount { .. } => IssueCode::PhoneDigitCount,
            Self::Iso3166CountryCode => IssueCode::Iso3166CountryCode,
            Self::Iso639LanguageCode => IssueCode::Iso639LanguageCode,
            Self::SanitizerRejected => IssueCode::SanitizerRejected,
            Self::UnspecifiedEnumVariant => IssueCode::UnspecifiedEnumVariant,
        }
    }
}

impl std::fmt::Display for Issue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "visitor issue {}", self.code().raw())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[repr(u16)]
pub enum IssueCode {
    UlidNil = 1,
    Float32NonFinite = 2,
    Float64NonFinite = 3,
    Utf8Invalid = 4,
    LengthEqual = 5,
    LengthMin = 6,
    LengthMax = 7,
    LengthRange = 8,
    NumericNotRepresentableAsDecimal = 9,
    NumericComparison = 10,
    NumericRange = 11,
    NumericMultipleOfZero = 12,
    NumericMultipleOf = 13,
    DecimalScaleMax = 14,
    CollectionValueNotAllowed = 15,
    Sha256Length = 16,
    Sha256NonHex = 17,
    MimeSlashCount = 18,
    MimeInvalidChars = 19,
    UrlScheme = 20,
    TextPattern = 21,
    ColorHex = 22,
    PhoneMissingPlus = 23,
    PhoneDigitCount = 24,
    Iso3166CountryCode = 25,
    Iso639LanguageCode = 26,
    SanitizerRejected = 27,
    UnspecifiedEnumVariant = 28,
}

impl IssueCode {
    #[must_use]
    pub const fn raw(self) -> u16 {
        self as u16
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[repr(u8)]
pub enum IssueComparisonOp {
    Lt,
    Gt,
    Lte,
    Gte,
    Eq,
    Ne,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[repr(u8)]
pub enum IssueTextPattern {
    AlphabeticUnderscore,
    AlphanumericUnderscore,
    Ascii,
    Camel,
    Kebab,
    Lower,
    LowerUnderscore,
    Sentence,
    Snake,
    Title,
    Upper,
    UpperCamel,
    UpperKebab,
    UpperSnake,
}

///
/// PathSegment
///

#[derive(Clone, Debug)]
pub enum PathSegment {
    Empty,
    Field(&'static str),
    Index(usize),
}

impl From<&'static str> for PathSegment {
    fn from(s: &'static str) -> Self {
        Self::Field(s)
    }
}

impl From<usize> for PathSegment {
    fn from(i: usize) -> Self {
        Self::Index(i)
    }
}

impl From<Option<&'static str>> for PathSegment {
    fn from(opt: Option<&'static str>) -> Self {
        match opt {
            Some(s) if !s.is_empty() => Self::Field(s),
            _ => Self::Empty,
        }
    }
}
