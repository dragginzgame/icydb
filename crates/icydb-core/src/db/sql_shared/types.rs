use crate::{types::Decimal, value::Value};
use icydb_diagnostic_code::SqlFeatureCode;
use std::str::FromStr;

#[cfg_attr(
    doc,
    doc = "SqlParseError\n\nReduced SQL parser errors shared by standalone predicate parsing and the statement-level SQL frontend."
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlParseError {
    #[cfg(feature = "sql")]
    EmptyInput,

    UnsupportedFeature {
        feature: SqlFeatureCode,
    },

    InvalidSyntax {
        kind: SqlSyntaxErrorKind,
    },
}

impl SqlParseError {
    pub(in crate::db) const fn unsupported_feature(feature: SqlFeatureCode) -> Self {
        Self::UnsupportedFeature { feature }
    }

    pub(in crate::db) const fn invalid_syntax(kind: SqlSyntaxErrorKind) -> Self {
        Self::InvalidSyntax { kind }
    }

    pub(crate) const fn expected(expected: SqlExpectedToken, found: Option<&TokenKind>) -> Self {
        Self::invalid_syntax(SqlSyntaxErrorKind::ExpectedToken {
            expected,
            found: SqlFoundToken::from_token_kind(found),
        })
    }

    pub(in crate::db) const fn expected_end_of_input(found: Option<&TokenKind>) -> Self {
        Self::expected(SqlExpectedToken::EndOfInput, found)
    }

    pub(in crate::db) const fn invalid_numeric_literal() -> Self {
        Self::invalid_syntax(SqlSyntaxErrorKind::InvalidNumericLiteral)
    }
}

#[cfg_attr(
    doc,
    doc = "SqlSyntaxErrorKind\n\nCompact syntax-error taxonomy for reduced SQL parsing."
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlSyntaxErrorKind {
    ExpectedToken {
        expected: SqlExpectedToken,
        found: SqlFoundToken,
    },
    InvalidNumericLiteral,
    UnexpectedCharacter {
        byte: u8,
    },
    UnexpectedBang,
    ExpectedByte {
        expected: u8,
        found: Option<u8>,
    },
    BlobLiteralNonHexDigit,
    BlobLiteralUnterminated,
    StringLiteralUnterminated,
    BlobLiteralOddHexLength,
    BlobLiteralTooLarge {
        max_decoded_bytes: usize,
    },
    InputTooLong {
        max_bytes: usize,
    },
    TokenLimit {
        max_tokens: usize,
    },
    ExpressionDepthLimit {
        max_depth: usize,
    },
    #[cfg(feature = "sql")]
    IntegerLiteralRequiresNonNegative {
        clause: SqlIntegerLiteralClause,
    },
    #[cfg(feature = "sql")]
    IntegerLiteralU32Overflow {
        clause: SqlIntegerLiteralClause,
    },
    #[cfg(feature = "sql")]
    ClauseOrder {
        rule: SqlClauseOrderRule,
    },
    #[cfg(feature = "sql")]
    InsertValuesTupleLengthMismatch,
    #[cfg(feature = "sql")]
    CoalesceRequiresTwoArguments,
    #[cfg(feature = "sql")]
    RoundScaleRequiresIntegerLiteral,
    #[cfg(feature = "sql")]
    InRequiresLiteral,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlExpectedToken {
    Literal,
    LikeStringPattern,
    IlikeStringPattern,
    StartsWithSecondArgument,
    BooleanOrNullLiteral,
    NumericLiteralAfterMinus,
    Identifier,
    IdentifierAfterDot,
    #[cfg(feature = "sql")]
    IdentifierKeyword {
        keyword: SqlIdentifierKeyword,
    },
    LParen,
    RParen,
    #[cfg(feature = "sql")]
    Eq,
    #[cfg(feature = "sql")]
    UpdateAssignmentEq,
    #[cfg(feature = "sql")]
    UpdateAssignment,
    #[cfg(feature = "sql-explain")]
    SelectOrDelete,
    CompareOperator,
    FieldSpecialOperator,
    NotSpecialOperator,
    #[cfg(feature = "sql")]
    ProjectionItem,
    #[cfg(feature = "sql")]
    Comma,
    PredicateArgumentComma,
    #[cfg(feature = "sql")]
    ScalarFunctionArgumentComma,
    #[cfg(test)]
    Then,
    #[cfg(test)]
    End,
    #[cfg(test)]
    Where,
    EndOfInput,
    #[cfg(feature = "sql")]
    StatementStart,
    #[cfg(feature = "sql")]
    ShowIndexesSource,
    Keyword(Keyword),
    #[cfg(feature = "sql")]
    IntegerLiteral {
        clause: SqlIntegerLiteralClause,
    },
}

impl SqlExpectedToken {
    #[cfg(feature = "sql")]
    pub(crate) fn identifier_keyword(keyword: &str) -> Self {
        Self::IdentifierKeyword {
            keyword: SqlIdentifierKeyword::from_str(keyword),
        }
    }
}

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlIdentifierKeyword {
    Into,
    Values,
    Set,
    MaxBytes,
    Schema,
    Version,
    Other,
}

#[cfg(feature = "sql")]
impl SqlIdentifierKeyword {
    fn from_str(keyword: &str) -> Self {
        match keyword {
            "INTO" => Self::Into,
            "VALUES" => Self::Values,
            "SET" => Self::Set,
            "max_bytes" => Self::MaxBytes,
            "SCHEMA" => Self::Schema,
            "VERSION" => Self::Version,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlFoundToken {
    EndOfInput,
    Identifier,
    Number,
    StringLiteral,
    BlobLiteral,
    Keyword(Keyword),
    Question,
    Comma,
    Dot,
    Plus,
    Minus,
    Slash,
    LParen,
    RParen,
    Semicolon,
    Star,
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl SqlFoundToken {
    const fn from_token_kind(kind: Option<&TokenKind>) -> Self {
        match kind {
            None => Self::EndOfInput,
            Some(TokenKind::Identifier(_)) => Self::Identifier,
            Some(TokenKind::Number(_)) => Self::Number,
            Some(TokenKind::StringLiteral(_)) => Self::StringLiteral,
            Some(TokenKind::BlobLiteral(_)) => Self::BlobLiteral,
            Some(TokenKind::Keyword(keyword)) => Self::Keyword(*keyword),
            Some(TokenKind::Question) => Self::Question,
            Some(TokenKind::Comma) => Self::Comma,
            Some(TokenKind::Dot) => Self::Dot,
            Some(TokenKind::Plus) => Self::Plus,
            Some(TokenKind::Minus) => Self::Minus,
            Some(TokenKind::Slash) => Self::Slash,
            Some(TokenKind::LParen) => Self::LParen,
            Some(TokenKind::RParen) => Self::RParen,
            Some(TokenKind::Semicolon) => Self::Semicolon,
            Some(TokenKind::Star) => Self::Star,
            Some(TokenKind::Eq) => Self::Eq,
            Some(TokenKind::Ne) => Self::Ne,
            Some(TokenKind::Lt) => Self::Lt,
            Some(TokenKind::Lte) => Self::Lte,
            Some(TokenKind::Gt) => Self::Gt,
            Some(TokenKind::Gte) => Self::Gte,
        }
    }
}

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlIntegerLiteralClause {
    Limit,
    Offset,
    MaxBytes,
    ExpectSchemaVersion,
    SetSchemaVersion,
}

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlClauseOrderRule {
    SelectOrderBeforeLimitOffset,
    DeleteOrderBeforeLimit,
    UpdateOrderBeforeLimitOffset,
    UpdateLimitBeforeOffset,
}

#[cfg_attr(
    doc,
    doc = "Keyword\n\nReduced SQL keyword taxonomy shared by predicate parsing and statement parsing."
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Keyword {
    And,
    As,
    Asc,
    Avg,
    Between,
    By,
    Case,
    Columns,
    Count,
    Create,
    Delete,
    Describe,
    Desc,
    Distinct,
    Drop,
    Else,
    End,
    Except,
    Execution,
    Explain,
    Entities,
    False,
    Filter,
    From,
    Group,
    Having,
    In,
    Index,
    Indexes,
    Insert,
    Intersect,
    Is,
    Join,
    Json,
    Limit,
    Memory,
    Max,
    Min,
    Not,
    Null,
    Offset,
    On,
    Or,
    Order,
    Over,
    Returning,
    Select,
    Show,
    Stores,
    Sum,
    Then,
    True,
    Union,
    Unique,
    Update,
    Verbose,
    Where,
    When,
    With,
}

impl Keyword {
    #[cfg(feature = "sql")]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::And => "AND",
            Self::As => "AS",
            Self::Asc => "ASC",
            Self::Avg => "AVG",
            Self::Between => "BETWEEN",
            Self::By => "BY",
            Self::Case => "CASE",
            Self::Columns => "COLUMNS",
            Self::Count => "COUNT",
            Self::Create => "CREATE",
            Self::Delete => "DELETE",
            Self::Describe => "DESCRIBE",
            Self::Desc => "DESC",
            Self::Distinct => "DISTINCT",
            Self::Drop => "DROP",
            Self::Else => "ELSE",
            Self::End => "END",
            Self::Except => "EXCEPT",
            Self::Execution => "EXECUTION",
            Self::Explain => "EXPLAIN",
            Self::Entities => "ENTITIES",
            Self::False => "FALSE",
            Self::Filter => "FILTER",
            Self::From => "FROM",
            Self::Group => "GROUP",
            Self::Having => "HAVING",
            Self::In => "IN",
            Self::Index => "INDEX",
            Self::Indexes => "INDEXES",
            Self::Insert => "INSERT",
            Self::Intersect => "INTERSECT",
            Self::Is => "IS",
            Self::Join => "JOIN",
            Self::Json => "JSON",
            Self::Limit => "LIMIT",
            Self::Memory => "MEMORY",
            Self::Max => "MAX",
            Self::Min => "MIN",
            Self::Not => "NOT",
            Self::Null => "NULL",
            Self::Offset => "OFFSET",
            Self::On => "ON",
            Self::Or => "OR",
            Self::Order => "ORDER",
            Self::Over => "OVER",
            Self::Returning => "RETURNING",
            Self::Select => "SELECT",
            Self::Show => "SHOW",
            Self::Stores => "STORES",
            Self::Sum => "SUM",
            Self::Then => "THEN",
            Self::True => "TRUE",
            Self::Union => "UNION",
            Self::Unique => "UNIQUE",
            Self::Update => "UPDATE",
            Self::Verbose => "VERBOSE",
            Self::Where => "WHERE",
            Self::When => "WHEN",
            Self::With => "WITH",
        }
    }
}

// One reduced SQL token kind shared by predicate and statement parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TokenKind {
    Identifier(String),
    Number(String),
    StringLiteral(String),
    BlobLiteral(Vec<u8>),
    Keyword(Keyword),
    Question,
    Comma,
    Dot,
    Plus,
    Minus,
    Slash,
    LParen,
    RParen,
    Semicolon,
    Star,
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Token {
    pub(crate) kind: TokenKind,
}

#[cfg(feature = "sql")]
pub(crate) fn token_kind_sql_fragment(kind: &TokenKind) -> String {
    match kind {
        TokenKind::Identifier(name) | TokenKind::Number(name) => name.clone(),
        TokenKind::StringLiteral(value) => format!("'{}'", value.replace('\'', "''")),
        TokenKind::BlobLiteral(bytes) => {
            let mut rendered = String::with_capacity(bytes.len().saturating_mul(2) + 3);
            rendered.push_str("X'");
            for byte in bytes {
                rendered.push(hex_digit(byte >> 4));
                rendered.push(hex_digit(byte & 0x0f));
            }
            rendered.push('\'');
            rendered
        }
        TokenKind::Keyword(keyword) => keyword.as_str().to_string(),
        TokenKind::Question => "?".to_string(),
        TokenKind::Comma => ",".to_string(),
        TokenKind::Dot => ".".to_string(),
        TokenKind::Plus => "+".to_string(),
        TokenKind::Minus => "-".to_string(),
        TokenKind::Slash => "/".to_string(),
        TokenKind::LParen => "(".to_string(),
        TokenKind::RParen => ")".to_string(),
        TokenKind::Semicolon => ";".to_string(),
        TokenKind::Star => "*".to_string(),
        TokenKind::Eq => "=".to_string(),
        TokenKind::Ne => "!=".to_string(),
        TokenKind::Lt => "<".to_string(),
        TokenKind::Lte => "<=".to_string(),
        TokenKind::Gt => ">".to_string(),
        TokenKind::Gte => ">=".to_string(),
    }
}

#[cfg(feature = "sql")]
const fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'A' + nibble - 10) as char,
        _ => '?',
    }
}

pub(in crate::db::sql_shared) fn parse_number_literal(raw: &str) -> Result<Value, SqlParseError> {
    if raw.contains('.') {
        let decimal =
            Decimal::from_str(raw).map_err(|_| SqlParseError::invalid_numeric_literal())?;
        return Ok(Value::Decimal(decimal));
    }

    if let Ok(value) = raw.parse::<i64>() {
        return Ok(Value::Int64(value));
    }
    if let Ok(value) = raw.parse::<u64>() {
        return Ok(Value::Nat64(value));
    }
    if let Ok(value) = Decimal::from_str(raw) {
        return Ok(Value::Decimal(value));
    }

    Err(SqlParseError::invalid_numeric_literal())
}
