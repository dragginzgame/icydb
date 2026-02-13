use crate::{
    patch::{ListPatch, merge::error::MergePatchError},
    traits::UpdateView,
};

/// Apply ordered list patch operations in sequence.
pub fn merge_vec<T>(
    values: &mut Vec<T>,
    patches: Vec<ListPatch<T::UpdateViewType>>,
) -> Result<(), MergePatchError>
where
    T: UpdateView + Default,
{
    for patch in patches {
        match patch {
            ListPatch::Update { index, patch } => {
                if let Some(elem) = values.get_mut(index) {
                    elem.merge(patch).map_err(|err| err.with_index(index))?;
                }
            }

            ListPatch::Insert { index, value } => {
                let idx = index.min(values.len());
                let mut elem = T::default();
                elem.merge(value).map_err(|err| err.with_index(idx))?;
                values.insert(idx, elem);
            }

            ListPatch::Push { value } => {
                let idx = values.len();
                let mut elem = T::default();
                elem.merge(value).map_err(|err| err.with_index(idx))?;
                values.push(elem);
            }

            ListPatch::Overwrite {
                values: next_values,
            } => {
                values.clear();
                values.reserve(next_values.len());

                for (index, value) in next_values.into_iter().enumerate() {
                    let mut elem = T::default();
                    elem.merge(value).map_err(|err| err.with_index(index))?;
                    values.push(elem);
                }
            }

            ListPatch::Remove { index } => {
                if index < values.len() {
                    values.remove(index);
                }
            }

            ListPatch::Clear => values.clear(),
        }
    }

    Ok(())
}
