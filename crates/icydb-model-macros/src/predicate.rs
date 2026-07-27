//! Reduced generated-schema predicate parsing.
//!
//! This is an authoring compiler boundary. It parses only the bounded source
//! expression language representable by `icydb-schema`; accepted binding and
//! runtime evaluation remain IcyDB-owned.

use crate::prelude::*;

const MAX_PREDICATE_BYTES: usize = 4_096;
const MAX_PREDICATE_DEPTH: usize = 32;
const MAX_PREDICATE_TOKENS: usize = 256;
const MAX_MEMBERSHIP_ITEMS: usize = 64;

/// One parsed generated-schema predicate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Predicate {
    Bool(bool),
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare {
        field: String,
        op: CompareOp,
        operand: CompareOperand,
    },
    IsNull {
        field: String,
        negated: bool,
    },
    In {
        field: String,
        values: Vec<Literal>,
        negated: bool,
    },
}

/// One comparison operation in the public source-expression subset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompareOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl CompareOp {
    const fn flipped(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Ne => Self::Ne,
            Self::Lt => Self::Gt,
            Self::Lte => Self::Gte,
            Self::Gt => Self::Lt,
            Self::Gte => Self::Lte,
        }
    }

    pub(crate) const fn is_ordering(self) -> bool {
        matches!(self, Self::Lt | Self::Lte | Self::Gt | Self::Gte)
    }
}

/// Right-hand side of one normalized field-first comparison.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CompareOperand {
    Field(String),
    Literal(Literal),
}

/// One scalar literal retained until field-aware proposal lowering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Literal {
    Bool(bool),
    Number(String),
    Text(String),
}

/// Parse one bounded reduced-SQL predicate.
pub(crate) fn parse(sql: &str) -> Result<Predicate, DarlingError> {
    if sql.is_empty() || sql.len() > MAX_PREDICATE_BYTES {
        return Err(DarlingError::custom(
            "generated schema predicate is empty or exceeds its byte bound",
        ));
    }
    let tokens = tokenize(sql)?;
    if tokens.is_empty() || tokens.len() > MAX_PREDICATE_TOKENS {
        return Err(DarlingError::custom(
            "generated schema predicate is empty or exceeds its token bound",
        ));
    }
    let mut parser = Parser::new(tokens);
    let predicate = parser.parse_or(1)?;
    if !parser.is_eof() {
        return Err(DarlingError::custom(
            "generated schema predicate contains unsupported trailing syntax",
        ));
    }
    Ok(predicate)
}

/// Collect every field referenced by one parsed predicate.
pub(crate) fn referenced_fields(predicate: &Predicate) -> Vec<String> {
    let mut fields = Vec::new();
    collect_fields(predicate, &mut fields);
    fields
}

fn collect_fields(predicate: &Predicate, fields: &mut Vec<String>) {
    match predicate {
        Predicate::Bool(_) => {}
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                collect_fields(child, fields);
            }
        }
        Predicate::Not(inner) => collect_fields(inner, fields),
        Predicate::Compare { field, operand, .. } => {
            push_unique(fields, field);
            if let CompareOperand::Field(other) = operand {
                push_unique(fields, other);
            }
        }
        Predicate::IsNull { field, .. } | Predicate::In { field, .. } => {
            push_unique(fields, field);
        }
    }
}

fn push_unique(fields: &mut Vec<String>, field: &str) {
    if !fields.iter().any(|candidate| candidate == field) {
        fields.push(field.to_string());
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Token {
    Ident(String),
    Literal(Literal),
    And,
    Or,
    Not,
    In,
    Between,
    Is,
    Null,
    LParen,
    RParen,
    Comma,
    Compare(CompareOp),
}

fn tokenize(sql: &str) -> Result<Vec<Token>, DarlingError> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
            continue;
        }
        match bytes[cursor] {
            b'(' => {
                tokens.push(Token::LParen);
                cursor += 1;
            }
            b')' => {
                tokens.push(Token::RParen);
                cursor += 1;
            }
            b',' => {
                tokens.push(Token::Comma);
                cursor += 1;
            }
            b'=' => {
                tokens.push(Token::Compare(CompareOp::Eq));
                cursor += 1;
            }
            b'!' if bytes.get(cursor + 1) == Some(&b'=') => {
                tokens.push(Token::Compare(CompareOp::Ne));
                cursor += 2;
            }
            b'<' => {
                let (op, width) = match bytes.get(cursor + 1) {
                    Some(b'=') => (CompareOp::Lte, 2),
                    Some(b'>') => (CompareOp::Ne, 2),
                    _ => (CompareOp::Lt, 1),
                };
                tokens.push(Token::Compare(op));
                cursor += width;
            }
            b'>' => {
                let (op, width) = if bytes.get(cursor + 1) == Some(&b'=') {
                    (CompareOp::Gte, 2)
                } else {
                    (CompareOp::Gt, 1)
                };
                tokens.push(Token::Compare(op));
                cursor += width;
            }
            b'\'' => {
                let (value, next) = parse_text_literal(sql, cursor)?;
                tokens.push(Token::Literal(Literal::Text(value)));
                cursor = next;
            }
            b'+' | b'-' if bytes.get(cursor + 1).is_some_and(u8::is_ascii_digit) => {
                let (value, next) = parse_number(sql, cursor)?;
                tokens.push(Token::Literal(Literal::Number(value)));
                cursor = next;
            }
            byte if byte.is_ascii_digit() => {
                let (value, next) = parse_number(sql, cursor)?;
                tokens.push(Token::Literal(Literal::Number(value)));
                cursor = next;
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = cursor;
                cursor += 1;
                while bytes
                    .get(cursor)
                    .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                {
                    cursor += 1;
                }
                let word = &sql[start..cursor];
                tokens.push(match word.to_ascii_uppercase().as_str() {
                    "AND" => Token::And,
                    "OR" => Token::Or,
                    "NOT" => Token::Not,
                    "IN" => Token::In,
                    "BETWEEN" => Token::Between,
                    "IS" => Token::Is,
                    "NULL" => Token::Null,
                    "TRUE" => Token::Literal(Literal::Bool(true)),
                    "FALSE" => Token::Literal(Literal::Bool(false)),
                    _ => Token::Ident(word.to_string()),
                });
            }
            _ => {
                return Err(DarlingError::custom(
                    "generated schema predicate contains unsupported syntax",
                ));
            }
        }
        if tokens.len() > MAX_PREDICATE_TOKENS {
            return Err(DarlingError::custom(
                "generated schema predicate exceeds its token bound",
            ));
        }
    }
    Ok(tokens)
}

fn parse_text_literal(sql: &str, start: usize) -> Result<(String, usize), DarlingError> {
    let bytes = sql.as_bytes();
    let mut value = String::new();
    let mut cursor = start + 1;
    while cursor < bytes.len() {
        if bytes[cursor] == b'\'' {
            if bytes.get(cursor + 1) == Some(&b'\'') {
                value.push('\'');
                cursor += 2;
                continue;
            }
            return Ok((value, cursor + 1));
        }
        let tail = &sql[cursor..];
        let character = tail
            .chars()
            .next()
            .ok_or_else(|| DarlingError::custom("unterminated predicate string literal"))?;
        value.push(character);
        cursor += character.len_utf8();
    }
    Err(DarlingError::custom(
        "unterminated predicate string literal",
    ))
}

fn parse_number(sql: &str, start: usize) -> Result<(String, usize), DarlingError> {
    let bytes = sql.as_bytes();
    let mut cursor = start;
    if matches!(bytes[cursor], b'+' | b'-') {
        cursor += 1;
    }
    let integer_start = cursor;
    while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
        cursor += 1;
    }
    if cursor == integer_start {
        return Err(DarlingError::custom("invalid predicate numeric literal"));
    }
    if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        let fraction_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor == fraction_start {
            return Err(DarlingError::custom("invalid predicate decimal literal"));
        }
    }
    Ok((sql[start..cursor].to_string(), cursor))
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
}

impl Parser {
    const fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, cursor: 0 }
    }

    const fn is_eof(&self) -> bool {
        self.cursor == self.tokens.len()
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.cursor)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.cursor).cloned();
        self.cursor = self.cursor.saturating_add(usize::from(token.is_some()));
        token
    }

    fn parse_or(&mut self, depth: usize) -> Result<Predicate, DarlingError> {
        let mut children = vec![self.parse_and(depth)?];
        while matches!(self.peek(), Some(Token::Or)) {
            self.next();
            children.push(self.parse_and(depth)?);
        }
        Ok(if children.len() == 1 {
            children.remove(0)
        } else {
            Predicate::Or(children)
        })
    }

    fn parse_and(&mut self, depth: usize) -> Result<Predicate, DarlingError> {
        let mut children = vec![self.parse_not(depth)?];
        while matches!(self.peek(), Some(Token::And)) {
            self.next();
            children.push(self.parse_not(depth)?);
        }
        Ok(if children.len() == 1 {
            children.remove(0)
        } else {
            Predicate::And(children)
        })
    }

    fn parse_not(&mut self, depth: usize) -> Result<Predicate, DarlingError> {
        if matches!(self.peek(), Some(Token::Not)) {
            self.next();
            return Ok(Predicate::Not(Box::new(self.parse_not(descend(depth)?)?)));
        }
        self.parse_primary(depth)
    }

    fn parse_primary(&mut self, depth: usize) -> Result<Predicate, DarlingError> {
        if matches!(self.peek(), Some(Token::LParen)) {
            self.next();
            let predicate = self.parse_or(descend(depth)?)?;
            self.expect_rparen()?;
            return Ok(predicate);
        }
        match self.next() {
            Some(Token::Ident(field)) => self.parse_field_predicate(field, depth),
            Some(Token::Literal(Literal::Bool(value)))
                if !matches!(self.peek(), Some(Token::Compare(_))) =>
            {
                Ok(Predicate::Bool(value))
            }
            Some(Token::Literal(literal)) => {
                let Some(Token::Compare(op)) = self.next() else {
                    return Err(DarlingError::custom(
                        "literal-leading predicate requires a comparison",
                    ));
                };
                let Some(Token::Ident(field)) = self.next() else {
                    return Err(DarlingError::custom(
                        "literal-leading predicate must compare with a field",
                    ));
                };
                Ok(Predicate::Compare {
                    field,
                    op: op.flipped(),
                    operand: CompareOperand::Literal(literal),
                })
            }
            _ => Err(DarlingError::custom(
                "generated schema predicate expected a field or parenthesized expression",
            )),
        }
    }

    fn parse_field_predicate(
        &mut self,
        field: String,
        depth: usize,
    ) -> Result<Predicate, DarlingError> {
        match self.next() {
            Some(Token::Compare(op)) => Ok(Predicate::Compare {
                field,
                op,
                operand: self.parse_compare_operand()?,
            }),
            Some(Token::Is) => {
                let negated = if matches!(self.peek(), Some(Token::Not)) {
                    self.next();
                    true
                } else {
                    false
                };
                match self.next() {
                    Some(Token::Null) => Ok(Predicate::IsNull { field, negated }),
                    Some(Token::Literal(Literal::Bool(value))) => {
                        let compare = Predicate::Compare {
                            field,
                            op: CompareOp::Eq,
                            operand: CompareOperand::Literal(Literal::Bool(value)),
                        };
                        Ok(if negated {
                            Predicate::Not(Box::new(compare))
                        } else {
                            compare
                        })
                    }
                    _ => Err(DarlingError::custom("IS accepts only NULL, TRUE, or FALSE")),
                }
            }
            Some(Token::In) => self.parse_in(field, false),
            Some(Token::Between) => self.parse_between(field, false, depth),
            Some(Token::Not) => match self.next() {
                Some(Token::In) => self.parse_in(field, true),
                Some(Token::Between) => self.parse_between(field, true, depth),
                _ => Err(DarlingError::custom("field NOT accepts only IN or BETWEEN")),
            },
            _ => Err(DarlingError::custom(
                "generated schema predicate expected a comparison operator",
            )),
        }
    }

    fn parse_compare_operand(&mut self) -> Result<CompareOperand, DarlingError> {
        match self.next() {
            Some(Token::Ident(field)) => Ok(CompareOperand::Field(field)),
            Some(Token::Literal(literal)) => Ok(CompareOperand::Literal(literal)),
            Some(Token::Null) => Err(DarlingError::custom(
                "NULL comparisons must use IS NULL or IS NOT NULL",
            )),
            _ => Err(DarlingError::custom(
                "comparison requires a field or scalar literal",
            )),
        }
    }

    fn parse_in(&mut self, field: String, negated: bool) -> Result<Predicate, DarlingError> {
        self.expect_lparen()?;
        let mut values = Vec::new();
        loop {
            let Some(Token::Literal(literal)) = self.next() else {
                return Err(DarlingError::custom(
                    "IN requires a nonempty scalar literal list",
                ));
            };
            values.push(literal);
            if values.len() > MAX_MEMBERSHIP_ITEMS {
                return Err(DarlingError::custom(
                    "IN literal list exceeds its item bound",
                ));
            }
            match self.next() {
                Some(Token::Comma) => {}
                Some(Token::RParen) => break,
                _ => {
                    return Err(DarlingError::custom("IN literal list expected ',' or ')'"));
                }
            }
        }
        Ok(Predicate::In {
            field,
            values,
            negated,
        })
    }

    fn parse_between(
        &mut self,
        field: String,
        negated: bool,
        depth: usize,
    ) -> Result<Predicate, DarlingError> {
        let lower = self.parse_compare_operand()?;
        if !matches!(self.next(), Some(Token::And)) {
            return Err(DarlingError::custom("BETWEEN requires AND"));
        }
        let upper = self.parse_compare_operand()?;
        let (join, lower_op, upper_op) = if negated {
            (false, CompareOp::Lt, CompareOp::Gt)
        } else {
            (true, CompareOp::Gte, CompareOp::Lte)
        };
        let children = vec![
            Predicate::Compare {
                field: field.clone(),
                op: lower_op,
                operand: lower,
            },
            Predicate::Compare {
                field,
                op: upper_op,
                operand: upper,
            },
        ];
        let _ = descend(depth)?;
        Ok(if join {
            Predicate::And(children)
        } else {
            Predicate::Or(children)
        })
    }

    fn expect_lparen(&mut self) -> Result<(), DarlingError> {
        if matches!(self.next(), Some(Token::LParen)) {
            Ok(())
        } else {
            Err(DarlingError::custom("expected '('"))
        }
    }

    fn expect_rparen(&mut self) -> Result<(), DarlingError> {
        if matches!(self.next(), Some(Token::RParen)) {
            Ok(())
        } else {
            Err(DarlingError::custom("expected ')'"))
        }
    }
}

fn descend(depth: usize) -> Result<usize, DarlingError> {
    if depth >= MAX_PREDICATE_DEPTH {
        return Err(DarlingError::custom(
            "generated schema predicate exceeds its depth bound",
        ));
    }
    Ok(depth + 1)
}

#[cfg(test)]
mod tests {
    use super::{CompareOp, CompareOperand, Literal, Predicate, parse, referenced_fields};

    #[test]
    fn parses_current_generated_predicate_forms() {
        let predicate =
            parse("active = true AND balance BETWEEN 0 AND 10 OR tier IN ('Bronze', 'Gold')")
                .expect("current generated predicate forms should parse");

        assert_eq!(referenced_fields(&predicate), ["active", "balance", "tier"]);
    }

    #[test]
    fn normalizes_literal_leading_order() {
        assert_eq!(
            parse("5 < age").expect("literal-leading comparison should parse"),
            Predicate::Compare {
                field: "age".to_string(),
                op: CompareOp::Gt,
                operand: CompareOperand::Literal(Literal::Number("5".to_string())),
            }
        );
    }

    #[test]
    fn rejects_unbounded_or_unsupported_syntax() {
        assert!(parse("").is_err());
        assert!(parse("LOWER(name) = 'a'").is_err());
        assert!(parse("id IN ()").is_err());
        assert!(parse("value = NULL").is_err());
    }
}
