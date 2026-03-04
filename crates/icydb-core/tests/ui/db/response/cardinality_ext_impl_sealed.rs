use icydb_core::{
    db::{ResponseCardinalityExt, ResponseError, Row},
    traits::EntityKind,
};
use std::marker::PhantomData;

struct ExternalCardinality<E: EntityKind>(PhantomData<E>);

impl<E: EntityKind> ResponseCardinalityExt<E> for ExternalCardinality<E> {
    fn require_one(&self) -> Result<(), ResponseError> {
        Ok(())
    }

    fn require_some(&self) -> Result<(), ResponseError> {
        Ok(())
    }

    fn try_row(self) -> Result<Option<Row<E>>, ResponseError> {
        Ok(None)
    }

    fn row(self) -> Result<Row<E>, ResponseError> {
        unreachable!()
    }

    fn try_entity(self) -> Result<Option<E>, ResponseError> {
        Ok(None)
    }

    fn entity(self) -> Result<E, ResponseError> {
        unreachable!()
    }

    fn require_id(self) -> Result<icydb_core::types::Id<E>, ResponseError> {
        unreachable!()
    }
}

fn main() {}
