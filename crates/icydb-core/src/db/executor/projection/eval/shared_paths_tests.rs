//! Compare lazy row-local selection with the maintained uncached path owner.

use crate::{
    db::{
        data::{
            CanonicalSlotReader, ScalarSlotValueRef, SlotReader,
            decode_structural_value_storage_bytes, encode_canonical_value_storage_bytes,
        },
        executor::projection::{
            eval::{
                CanonicalSlotExprReader, ValueRefSlotReader,
                eval_compiled_expr_with_value_cow_reader,
            },
            path::{resolve_slot_field_path, resolve_value_field_path},
        },
        query::plan::expr::{
            BinaryOp, CompiledExpr, CompiledExprCaseArm, CompiledExprValueReader,
            ProjectionEvalError,
        },
        schema::LeafCodec,
    },
    error::InternalError,
    types::Decimal,
    value::Value,
};
use std::{borrow::Cow, cell::Cell};

struct RawRoot<'a> {
    bytes: &'a [u8],
    reads: Cell<usize>,
}

impl SlotReader for RawRoot<'_> {
    fn get_bytes(&self, slot: usize) -> Option<&[u8]> {
        (slot == 0).then_some(self.bytes)
    }
    fn get_scalar(&self, _slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        Ok(None)
    }
    fn get_value(&mut self, slot: usize) -> Result<Option<Value>, InternalError> {
        self.required_value_by_contract(slot).map(Some)
    }
}

impl CanonicalSlotReader for RawRoot<'_> {
    fn field_name(&self, slot: usize) -> Result<&str, InternalError> {
        if slot == 0 {
            Ok("record")
        } else {
            Err(InternalError::persisted_row_decode_corruption())
        }
    }
    fn field_leaf_codec(&self, slot: usize) -> Result<LeafCodec, InternalError> {
        self.field_name(slot).map(|_| LeafCodec::Structural)
    }
    fn required_bytes(&self, slot: usize) -> Result<&[u8], InternalError> {
        self.field_name(slot)?;
        self.reads.set(self.reads.get() + 1);
        Ok(self.bytes)
    }
    fn required_value_by_contract(&self, slot: usize) -> Result<Value, InternalError> {
        decode_structural_value_storage_bytes(self.required_bytes(slot)?)
            .map_err(|_| InternalError::persisted_row_decode_corruption())
    }
}

struct UncachedPaths<'a>(&'a dyn CanonicalSlotReader);

impl CompiledExprValueReader for UncachedPaths<'_> {
    fn read_slot(&self, _slot: usize) -> Option<Cow<'_, Value>> {
        None
    }
    fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
        None
    }
    fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
        None
    }
    fn read_field_path(
        &self,
        root: usize,
        _segments: &[String],
        bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        resolve_slot_field_path(self.0, root, bytes)
            .map(|value| Some(Cow::Owned(value.unwrap_or(Value::Null))))
            .map_err(|error| ProjectionEvalError::FieldPathEvaluationFailed {
                class: error.class(),
                origin: error.origin(),
            })
    }
}

fn path(member: &str) -> CompiledExpr {
    let segments = ["profile", member];
    CompiledExpr::FieldPath {
        root_slot: 0,
        segments: segments.map(str::to_owned).into(),
        segment_bytes: segments
            .map(|part| part.as_bytes().to_vec().into_boxed_slice())
            .into(),
    }
}

fn record() -> Value {
    Value::Map(vec![(
        Value::Text("profile".into()),
        Value::Map(vec![
            (Value::Text("a".into()), Value::Nat64(2)),
            (Value::Text("b".into()), Value::Nat64(3)),
            (Value::Text("a".into()), Value::Nat64(9)),
        ]),
    )])
}

fn expressions() -> Vec<CompiledExpr> {
    vec![
        path("a"),
        CompiledExpr::Literal(Value::Text("kept".into())),
        CompiledExpr::Binary {
            op: BinaryOp::Add,
            left: Box::new(path("b")),
            right: Box::new(CompiledExpr::Literal(Value::Nat64(1))),
        },
        CompiledExpr::Case {
            when_then_arms: vec![CompiledExprCaseArm::new(
                CompiledExpr::Literal(Value::Bool(false)),
                path("dormant"),
            )]
            .into(),
            else_expr: Box::new(CompiledExpr::Literal(Value::Nat64(7))),
        },
        path("a"),
    ]
}

#[test]
fn nonconsecutive_raw_and_retained_paths_share_one_root_and_preserve_output() {
    let source = record();
    let bytes = encode_canonical_value_storage_bytes(&source).unwrap();
    let expressions = expressions();
    let raw = RawRoot {
        bytes: &bytes,
        reads: Cell::new(0),
    };
    let uncached = UncachedPaths(&raw);
    let expected: Vec<_> = expressions
        .iter()
        .map(|expr| expr.evaluate(&uncached).unwrap().into_owned())
        .collect();
    assert!(raw.reads.get() > 1);
    raw.reads.set(0);
    let mut record_slot = |_| {};
    let reader = CanonicalSlotExprReader::new(&expressions, &raw, &mut record_slot, true);
    let actual: Vec<_> = expressions
        .iter()
        .map(|expr| reader.evaluate_projection(expr).unwrap())
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(raw.reads.get(), 1);
    let reads = Cell::new(0);
    let mut load = |slot| {
        assert_eq!(slot, 0);
        reads.set(reads.get() + 1);
        Some(&source)
    };
    let reader = ValueRefSlotReader::new(&expressions, &mut load);
    let actual: Vec<_> = expressions
        .iter()
        .map(|expr| reader.evaluate(expr).unwrap())
        .collect();
    assert_eq!(reads.get(), 1);
    assert_eq!(
        actual,
        vec![
            Value::Nat64(2),
            Value::Text("kept".into()),
            Value::Decimal(Decimal::new(4, 0)),
            Value::Nat64(7),
            Value::Nat64(2)
        ]
    );
}

#[test]
fn paths_inside_expressions_share_owned_and_borrowed_roots_without_eager_branches() {
    let expr = CompiledExpr::Binary {
        op: BinaryOp::Add,
        left: Box::new(path("a")),
        right: Box::new(path("b")),
    };
    let source = record();
    for owned in [false, true] {
        let reads = Cell::new(0);
        let value = eval_compiled_expr_with_value_cow_reader(&expr, &mut |_| {
            reads.set(reads.get() + 1);
            Some(if owned {
                Cow::Owned(source.clone())
            } else {
                Cow::Borrowed(&source)
            })
        })
        .unwrap();
        assert_eq!(value, Value::Decimal(Decimal::new(5, 0)));
        assert_eq!(reads.get(), 1);
    }
    let lazy = CompiledExpr::Case {
        when_then_arms: vec![CompiledExprCaseArm::new(
            CompiledExpr::Literal(Value::Bool(false)),
            expr,
        )]
        .into(),
        else_expr: Box::new(CompiledExpr::Literal(Value::Bool(true))),
    };
    assert_eq!(
        eval_compiled_expr_with_value_cow_reader(&lazy, &mut |_| panic!(
            "unexecuted path must not load a root"
        ))
        .unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn reused_selection_preserves_missing_null_and_complete_root_corruption() {
    let expressions = [path("a"), path("b")];
    for source in [
        Value::Null,
        Value::Map(vec![]),
        Value::Map(vec![(Value::Text("profile".into()), Value::Null)]),
    ] {
        let expected: Vec<_> = expressions
            .iter()
            .map(|expr| {
                let CompiledExpr::FieldPath { segment_bytes, .. } = expr else {
                    unreachable!()
                };
                resolve_value_field_path(&source, segment_bytes)
                    .unwrap()
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .collect();
        let mut load = |_| Some(&source);
        let reader = ValueRefSlotReader::new(&expressions, &mut load);
        assert_eq!(
            expressions
                .iter()
                .map(|expr| reader.evaluate(expr).unwrap())
                .collect::<Vec<_>>(),
            expected
        );
    }
    let mut bytes = encode_canonical_value_storage_bytes(&record()).unwrap();
    bytes.push(255);
    let raw = RawRoot {
        bytes: &bytes,
        reads: Cell::new(0),
    };
    let expected = expressions[0]
        .evaluate(&UncachedPaths(&raw))
        .unwrap_err()
        .into_internal_error();
    let mut record_slot = |_| {};
    let reader = CanonicalSlotExprReader::new(&expressions, &raw, &mut record_slot, true);
    let error = reader.evaluate_projection(&expressions[0]).unwrap_err();
    assert_eq!(error.diagnostic(), expected.diagnostic());
    assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
}
