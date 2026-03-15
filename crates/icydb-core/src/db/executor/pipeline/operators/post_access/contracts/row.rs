use crate::{traits::EntityKind, types::Id};

///
/// PlanRow
///
/// Row abstraction for applying plan semantics to executor rows.
///

pub(in crate::db::executor) trait PlanRow<E: EntityKind> {
    fn entity(&self) -> &E;
}

impl<E: EntityKind> PlanRow<E> for (Id<E>, E) {
    fn entity(&self) -> &E {
        &self.1
    }
}
