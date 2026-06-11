//! Module: visitor::context
//! Responsibility: visitor issue-reporting context and path scoping helpers.
//! Does not own: concrete sanitize/validate traversal behavior.
//! Boundary: shared diagnostics context passed through visitor entrypoints.

use crate::{sanitize::SanitizeWriteContext, types::Decimal};
use serde::Deserialize;
use std::fmt;

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
    pub fn issue(&mut self, issue: impl Into<Issue>) {
        self.add_issue(issue.into());
    }

    pub fn issue_at(&mut self, seg: PathSegment, issue: impl Into<Issue>) {
        self.add_issue_at(seg, issue.into());
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
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
    Custom {
        message: String,
    },
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
            Self::Custom { .. } => IssueCode::Custom,
        }
    }

    #[must_use]
    pub fn custom(message: impl Into<String>) -> Self {
        Self::Custom {
            message: message.into(),
        }
    }
}

impl From<String> for Issue {
    fn from(message: String) -> Self {
        Self::Custom { message }
    }
}

impl From<&str> for Issue {
    fn from(message: &str) -> Self {
        Self::Custom {
            message: message.to_string(),
        }
    }
}

impl fmt::Display for Issue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UlidNil => f.write_str("ulid must not be nil"),
            Self::Float32NonFinite => f.write_str("Float32 must be finite"),
            Self::Float64NonFinite => f.write_str("Float64 must be finite"),
            Self::Utf8Invalid => f.write_str("bytes must be valid UTF-8"),
            Self::LengthEqual { actual, expected } => {
                write!(f, "length {actual} must equal {expected}")
            }
            Self::LengthMin { actual, min } => {
                write!(f, "length {actual} must be at least {min}")
            }
            Self::LengthMax { actual, max } => {
                write!(f, "length {actual} must be at most {max}")
            }
            Self::LengthRange { actual, min, max } => {
                write!(f, "length {actual} must be between {min} and {max}")
            }
            Self::NumericNotRepresentableAsDecimal => {
                f.write_str("numeric value cannot be represented as Decimal")
            }
            Self::NumericComparison {
                actual,
                op,
                expected,
            } => write!(
                f,
                "numeric value {actual} must be {} {expected}",
                op.symbol()
            ),
            Self::NumericRange { actual, min, max } => {
                write!(f, "numeric value {actual} must be between {min} and {max}")
            }
            Self::NumericMultipleOfZero => f.write_str("multipleOf target must be non-zero"),
            Self::NumericMultipleOf { actual, target } => {
                write!(f, "numeric value {actual} must be a multiple of {target}")
            }
            Self::DecimalScaleMax { actual, max } => {
                write!(f, "decimal scale {actual} must be at most {max}")
            }
            Self::CollectionValueNotAllowed { allowed_count } => {
                write!(f, "value must be one of {allowed_count} allowed values")
            }
            Self::Sha256Length { actual } => {
                write!(f, "SHA-256 hex digest length {actual} must be 64")
            }
            Self::Sha256NonHex => {
                f.write_str("SHA-256 digest must contain only hexadecimal characters")
            }
            Self::MimeSlashCount => f.write_str("MIME type must contain exactly one slash"),
            Self::MimeInvalidChars => f.write_str("MIME type contains invalid token characters"),
            Self::UrlScheme => f.write_str("URL must start with http:// or https://"),
            Self::TextPattern { pattern } => write!(f, "text must be {}", pattern.label()),
            Self::ColorHex { width } => {
                write!(f, "hex color must contain {width} hexadecimal digits")
            }
            Self::PhoneMissingPlus => f.write_str("phone number must start with +"),
            Self::PhoneDigitCount { digits } => {
                write!(f, "phone number has {digits} digits; expected 7 to 15")
            }
            Self::Iso3166CountryCode => {
                f.write_str("value must be an ISO 3166-1 alpha-2 country code")
            }
            Self::Iso639LanguageCode => f.write_str("value must be an ISO 639-1 language code"),
            Self::SanitizerRejected => f.write_str("sanitizer rejected value"),
            Self::UnspecifiedEnumVariant => f.write_str("unspecified enum variant is not valid"),
            Self::Custom { message } => f.write_str(message),
        }
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
    Custom = 29,
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

impl IssueComparisonOp {
    #[must_use]
    pub const fn symbol(self) -> &'static str {
        match self {
            Self::Lt => "<",
            Self::Gt => ">",
            Self::Lte => "<=",
            Self::Gte => ">=",
            Self::Eq => "==",
            Self::Ne => "!=",
        }
    }
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

impl IssueTextPattern {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::AlphabeticUnderscore => "alphabetic or underscore",
            Self::AlphanumericUnderscore => "alphanumeric or underscore",
            Self::Ascii => "ASCII",
            Self::Camel => "camel case",
            Self::Kebab => "kebab case",
            Self::Lower => "lower case",
            Self::LowerUnderscore => "lowercase or underscore",
            Self::Sentence => "sentence case",
            Self::Snake => "snake case",
            Self::Title => "title case",
            Self::Upper => "upper case",
            Self::UpperCamel => "upper camel case",
            Self::UpperKebab => "upper kebab case",
            Self::UpperSnake => "upper snake case",
        }
    }
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

#[cfg(test)]
mod tests {
    use super::{Issue, IssueCode, IssueComparisonOp, IssueTextPattern};

    #[test]
    fn custom_issue_preserves_message() {
        let issue = Issue::from("pet name is reserved");

        assert_eq!(issue.code(), IssueCode::Custom);
        assert_eq!(issue.to_string(), "pet name is reserved");
    }

    #[test]
    fn structured_issue_renders_context() {
        let issue = Issue::LengthRange {
            actual: 8,
            min: 10,
            max: 30,
        };

        assert_eq!(issue.to_string(), "length 8 must be between 10 and 30");
    }

    #[test]
    fn numeric_issue_renders_operator_and_values() {
        let issue = Issue::NumericComparison {
            actual: "1".parse().expect("valid decimal"),
            op: IssueComparisonOp::Gte,
            expected: "2".parse().expect("valid decimal"),
        };

        assert_eq!(issue.to_string(), "numeric value 1 must be >= 2");
    }

    #[test]
    fn text_pattern_issue_renders_pattern_name() {
        let issue = Issue::TextPattern {
            pattern: IssueTextPattern::UpperSnake,
        };

        assert_eq!(issue.to_string(), "text must be upper snake case");
    }
}
