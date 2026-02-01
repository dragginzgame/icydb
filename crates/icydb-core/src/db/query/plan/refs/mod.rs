#[cfg(test)]
pub mod tests;

use crate::{
    db::store::EntityRef,
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::field::EntityFieldKind,
    traits::EntityKind,
    value::Value,
};

///
/// EntityReferences
///
/// Extract typed entity references from a concrete entity instance.
/// This is a pure helper for pre-commit planning and RI checks.
/// Only direct `Ref<T>` and `Option<Ref<T>>` fields are strong in 0.6.
/// Nested and collection references are treated as weak and ignored.
/// This is a shallow walk over entity fields only; no recursive traversal occurs.
///

pub trait EntityReferences {
    /// Return all concrete references currently present on this entity.
    fn entity_refs(&self) -> Result<Vec<EntityRef>, InternalError>;
}

impl<E> EntityReferences for E
where
    E: EntityKind,
{
    fn entity_refs(&self) -> Result<Vec<EntityRef>, InternalError> {
        let mut refs = Vec::with_capacity(E::MODEL.fields.len());

        for field in E::MODEL.fields {
            // Phase 1: identify strong reference fields; weak shapes are ignored.
            let target_path = match &field.kind {
                &EntityFieldKind::Ref { target_path, .. } => target_path,
                &EntityFieldKind::List(inner) | &EntityFieldKind::Set(inner) => {
                    if matches!(inner, &EntityFieldKind::Ref { .. }) {
                        // Weak references: collection refs are allowed but not validated in 0.6.
                        continue;
                    }
                    continue;
                }
                &EntityFieldKind::Map { key, value } => {
                    if matches!(key, &EntityFieldKind::Ref { .. })
                        || matches!(value, &EntityFieldKind::Ref { .. })
                    {
                        // Weak references: map refs are allowed but not validated in 0.6.
                        continue;
                    }
                    continue;
                }
                _ => continue,
            };

            // Phase 2: fetch the field value and skip absent references.
            let Some(value) = self.get_value(field.name) else {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!("reference field missing: {} field={}", E::PATH, field.name),
                ));
            };

            if matches!(value, Value::None) {
                continue;
            }

            if matches!(value, Value::Unsupported) {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!(
                        "reference field value is unsupported: {} field={}",
                        E::PATH,
                        field.name
                    ),
                ));
            }

            // Phase 3: normalize into a concrete key and record the reference.
            let Some(key) = value.as_storage_key() else {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!(
                        "reference field value is not a key: {} field={}",
                        E::PATH,
                        field.name
                    ),
                ));
            };

            refs.push(EntityRef::from_storage_key(target_path, key));
        }

        Ok(refs)
    }
}
