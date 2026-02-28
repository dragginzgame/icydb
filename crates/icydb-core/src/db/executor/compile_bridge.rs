use crate::{
    db::{executor::ExecutablePlan, query::intent::CompiledQuery},
    traits::EntityKind,
};

impl<E: EntityKind> From<CompiledQuery<E>> for ExecutablePlan<E> {
    fn from(value: CompiledQuery<E>) -> Self {
        Self::new(value.into_inner())
    }
}

impl<E: EntityKind> CompiledQuery<E> {
    #[must_use]
    pub(in crate::db) fn into_executable(self) -> ExecutablePlan<E> {
        ExecutablePlan::from(self)
    }
}
