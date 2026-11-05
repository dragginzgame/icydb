use super::{
    CompiledExpr, CompiledExprValueReader, PathCache, ValueCowSlotReader, ValueRefSlotReader,
};
use crate::{db::query::plan::expr::BinaryOp, value::Value};
use std::{borrow::Cow, cell::RefCell};

struct SliceReader<'row> {
    row: &'row [Value],
}

impl CompiledExprValueReader for SliceReader<'_> {
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.row.get(slot).map(Cow::Borrowed)
    }

    fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
        None
    }
}

#[test]
fn compiled_expression_readers_agree_on_slot_arithmetic() {
    let expr = reader_dispatch_expr();
    for base in 0..4 {
        let row = [
            Value::Nat64(base + 1),
            Value::Nat64(2),
            Value::Nat64(base + 3),
            Value::Nat64(5),
        ];
        let expected = expr
            .evaluate(&SliceReader { row: &row })
            .expect("slice expression")
            .into_owned();
        let mut borrowed = |slot| row.get(slot);
        let mut cow = |slot| row.get(slot).map(Cow::Borrowed);
        let mut owned = |slot| row.get(slot).cloned().map(Cow::Owned);
        let readers: [&dyn CompiledExprValueReader; 3] = [
            &ValueRefSlotReader {
                read_slot: RefCell::new(&mut borrowed),
                field_path_missing_is_null: true,
                paths: RefCell::new(PathCache::new(std::slice::from_ref(&expr))),
            },
            &ValueCowSlotReader {
                read_slot: RefCell::new(&mut cow),
                field_path_missing_is_null: true,
                paths: RefCell::new(PathCache::new(std::slice::from_ref(&expr))),
            },
            &ValueCowSlotReader {
                read_slot: RefCell::new(&mut owned),
                field_path_missing_is_null: true,
                paths: RefCell::new(PathCache::new(std::slice::from_ref(&expr))),
            },
        ];
        for reader in readers {
            assert_eq!(
                expr.evaluate(reader).expect("callback expression").as_ref(),
                &expected
            );
        }
    }
}

fn reader_dispatch_expr() -> CompiledExpr {
    CompiledExpr::Binary {
        op: BinaryOp::Add,
        left: Box::new(CompiledExpr::Add {
            left_slot: 0,
            right_slot: 1,
        }),
        right: Box::new(CompiledExpr::Mul {
            left_slot: 2,
            right_slot: 3,
        }),
    }
}
