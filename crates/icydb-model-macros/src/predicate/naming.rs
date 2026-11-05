//! Stable authoring-predicate identity for automatic index names.
//! Parsing owns cosmetic SQL normalization; this encoder preserves its tree,
//! literal types and operand boundaries without using source text or Debug.

use crate::predicate::{CompareOp, CompareOperand, Literal, Predicate};

pub(crate) fn canonical_name_key(predicate: &Predicate) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode(predicate, &mut bytes);
    bytes
}

fn part(value: &str, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

fn literal(value: &Literal, bytes: &mut Vec<u8>) {
    match value {
        Literal::Bool(value) => bytes.extend_from_slice(&[0, u8::from(*value)]),
        Literal::Number(value) => {
            bytes.push(1);
            part(value, bytes);
        }
        Literal::Text(value) => {
            bytes.push(2);
            part(value, bytes);
        }
    }
}

fn encode(predicate: &Predicate, bytes: &mut Vec<u8>) {
    match predicate {
        Predicate::Bool(value) => bytes.extend_from_slice(&[0, u8::from(*value)]),
        Predicate::And(children) | Predicate::Or(children) => {
            bytes.push(if matches!(predicate, Predicate::And(_)) {
                1
            } else {
                2
            });
            bytes.extend_from_slice(&(children.len() as u64).to_be_bytes());
            for child in children {
                encode(child, bytes);
            }
        }
        Predicate::Not(child) => {
            bytes.push(3);
            encode(child, bytes);
        }
        Predicate::Compare { field, op, operand } => {
            bytes.push(4);
            part(field, bytes);
            bytes.push(match op {
                CompareOp::Eq => 0,
                CompareOp::Ne => 1,
                CompareOp::Lt => 2,
                CompareOp::Lte => 3,
                CompareOp::Gt => 4,
                CompareOp::Gte => 5,
            });
            match operand {
                CompareOperand::Field(field) => {
                    bytes.push(0);
                    part(field, bytes);
                }
                CompareOperand::Literal(value) => {
                    bytes.push(1);
                    literal(value, bytes);
                }
            }
        }
        Predicate::IsNull { field, negated } => {
            bytes.extend_from_slice(&[5, u8::from(*negated)]);
            part(field, bytes);
        }
        Predicate::In {
            field,
            values,
            negated,
        } => {
            bytes.extend_from_slice(&[6, u8::from(*negated)]);
            part(field, bytes);
            bytes.extend_from_slice(&(values.len() as u64).to_be_bytes());
            for value in values {
                literal(value, bytes);
            }
        }
    }
}
