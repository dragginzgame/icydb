use crate::{
    error::{Error, ErrorKind, ErrorOrigin, UpdateErrorKind},
    traits::UpdateView,
};

// re-exports
pub use icydb_core::patch::{ListPatch, MapPatch, MergePatch, MergePatchError, SetPatch};

/// Apply a merge patch to an already-loaded entity, translating
/// core patch errors into interface-level errors.
pub fn apply_patch<E>(entity: &mut E, patch: <E as UpdateView>::UpdateViewType) -> Result<(), Error>
where
    E: MergePatch,
{
    entity.merge(patch).map_err(|err| {
        let message = err.to_string();
        Error::new(
            ErrorKind::Update(UpdateErrorKind::Patch(err.into())),
            ErrorOrigin::Interface,
            message,
        )
    })
}
