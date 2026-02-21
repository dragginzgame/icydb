use crate::{
    db::executor::mutation::save::SaveExecutor,
    error::InternalError,
    sanitize::sanitize,
    traits::{EntityKind, EntityValue},
    validate::validate,
};

impl<E: EntityKind + EntityValue> SaveExecutor<E> {
    // Execute the canonical save preflight pipeline before commit planning.
    pub(super) fn preflight_entity(&self, entity: &mut E) -> Result<(), InternalError> {
        sanitize(entity)?;
        validate(entity)?;
        Self::ensure_entity_invariants(entity)?;
        self.validate_strong_relations(entity)?;

        Ok(())
    }
}
