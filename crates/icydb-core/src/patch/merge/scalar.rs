use crate::{
    patch::merge::error::MergePatchError,
    traits::{Atomic, UpdateView},
};

/// Apply full-replacement semantics for atomic update payloads.
pub fn merge_atomic<T>(value: &mut T, patch: T) -> Result<(), MergePatchError>
where
    T: Atomic + UpdateView<UpdateViewType = T>,
{
    *value = patch;

    Ok(())
}

/// Apply optional update payloads with create-on-update semantics.
pub fn merge_option<T>(
    value: &mut Option<T>,
    patch: Option<T::UpdateViewType>,
) -> Result<(), MergePatchError>
where
    T: UpdateView + Default,
{
    match patch {
        None => {
            // Explicit delete
            *value = None;
        }
        Some(inner_patch) => {
            if let Some(inner_value) = value.as_mut() {
                inner_value
                    .merge(inner_patch)
                    .map_err(|err| err.with_field("value"))?;
            } else {
                let mut new_value = T::default();
                new_value
                    .merge(inner_patch)
                    .map_err(|err| err.with_field("value"))?;
                *value = Some(new_value);
            }
        }
    }

    Ok(())
}
