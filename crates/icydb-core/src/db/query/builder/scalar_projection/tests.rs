use super::{render_scalar_projection_expr_plan_label, write_scalar_projection_expr_plan_label};
use crate::{
    db::query::{
        builder::aggregate::{count, sum},
        plan::expr::{BinaryOp, CaseWhenArm, Expr, FieldId, FieldPath, Function, UnaryOp},
    },
    value::Value,
};
use std::fmt::{self, Write};

fn field(name: &str) -> Expr {
    Expr::Field(FieldId::new(name))
}

fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn fixtures() -> Vec<(Expr, &'static str)> {
    vec![
        (Expr::Literal(Value::NatBig("0".parse().unwrap())), "0"),
        (
            Expr::Literal(Value::NatBig("1000000001".parse().unwrap())),
            "1_000_000_001",
        ),
        (
            Expr::Literal(Value::IntBig("-1000000000000000001".parse().unwrap())),
            "-1_000_000_000_000_000_001",
        ),
        (Expr::Literal(Value::Text("é'\\\n'".into())), "'é''\\\n'''"),
        (
            Expr::FieldPath(FieldPath::new(
                FieldId::new("account"),
                vec!["owner".into(), "id".into()],
            )),
            "account.owner.id",
        ),
        (
            Expr::FunctionCall {
                function: Function::Coalesce,
                args: vec![Expr::Literal(Value::Null), Expr::Literal(Value::Bool(true))],
            },
            "COALESCE(NULL, TRUE)",
        ),
        (
            binary(
                BinaryOp::Mul,
                binary(BinaryOp::Add, field("a"), field("b")),
                binary(BinaryOp::Sub, field("c"), field("d")),
            ),
            "(a + b) * (c - d)",
        ),
        (
            binary(
                BinaryOp::Sub,
                field("a"),
                binary(BinaryOp::Sub, field("b"), field("c")),
            ),
            "a - (b - c)",
        ),
        (
            Expr::Case {
                when_then_arms: vec![CaseWhenArm::new(
                    field("ready"),
                    Expr::Literal(Value::Int64(1)),
                )],
                else_expr: Box::new(Expr::Literal(Value::Int64(0))),
            },
            "CASE WHEN ready THEN 1 ELSE 0 END",
        ),
        (Expr::Aggregate(count()), "COUNT(*)"),
        (
            Expr::Aggregate(sum("amount").distinct().with_filter_expr(binary(
                BinaryOp::Gt,
                field("amount"),
                Expr::Literal(Value::Int64(0)),
            ))),
            "SUM(DISTINCT amount) FILTER (WHERE amount > 0)",
        ),
        (
            Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(field("ready")),
            },
            "NOT ready",
        ),
    ]
}

// A rejecting sink checks before retaining bytes and records whether rendering
// tries to continue after rejection. It is not a production budget policy.
struct LimitedOutput {
    remaining: usize,
    rendered: String,
    rejected: bool,
}

impl crate::value::decimal::ValueFormatWriter for LimitedOutput {
    fn admit_scratch(&mut self, _bytes: u64, _steps: u64) -> fmt::Result {
        Ok(())
    }
}

impl Write for LimitedOutput {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        assert!(
            !self.rejected,
            "rendering must stop at the first sink error"
        );
        if text.len() > self.remaining {
            self.rejected = true;
            return Err(fmt::Error);
        }
        self.remaining -= text.len();
        self.rendered.push_str(text);
        Ok(())
    }
}

#[test]
fn streaming_labels_preserve_escaping_precedence_paths_and_aggregate_identity() {
    for (expr, expected) in fixtures() {
        assert_eq!(render_scalar_projection_expr_plan_label(&expr), expected);
        let mut output = LimitedOutput {
            remaining: expected.len(),
            rendered: String::new(),
            rejected: false,
        };
        write_scalar_projection_expr_plan_label(&expr, &mut output).expect("exact byte allowance");
        assert_eq!(output.rendered, expected);
        assert_eq!(output.remaining, 0);
    }
}

#[test]
fn streaming_labels_stop_on_every_output_boundary() {
    for (expr, expected) in fixtures() {
        for allowed in 0..expected.len() {
            let mut output = LimitedOutput {
                remaining: allowed,
                rendered: String::new(),
                rejected: false,
            };
            assert!(write_scalar_projection_expr_plan_label(&expr, &mut output).is_err());
            assert!(output.rejected);
            assert!(output.rendered.len() <= allowed);
            assert!(expected.starts_with(&output.rendered));
        }
    }
}
