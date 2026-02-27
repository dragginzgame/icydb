use crate::{
    db::{
        executor::ExecutablePlan,
        query::{
            fluent::{delete::FluentDeleteQuery, load::FluentLoadQuery},
            intent::{PlannedQuery, Query, QueryError},
        },
    },
    traits::EntityKind,
};

impl<E: EntityKind> From<PlannedQuery<E>> for ExecutablePlan<E> {
    fn from(value: PlannedQuery<E>) -> Self {
        Self::new(value.into_inner())
    }
}

impl<E: EntityKind> Query<E> {
    /// Compile this logical planned query into executor runtime state.
    pub fn plan(&self) -> Result<ExecutablePlan<E>, QueryError> {
        self.planned().map(ExecutablePlan::from)
    }
}

impl<E: EntityKind> FluentLoadQuery<'_, E> {
    /// Compile this fluent load intent into executor runtime state.
    pub fn plan(&self) -> Result<ExecutablePlan<E>, QueryError> {
        self.planned().map(ExecutablePlan::from)
    }
}

impl<E: EntityKind> FluentDeleteQuery<'_, E> {
    /// Compile this fluent delete intent into executor runtime state.
    pub fn plan(&self) -> Result<ExecutablePlan<E>, QueryError> {
        self.planned().map(ExecutablePlan::from)
    }
}
