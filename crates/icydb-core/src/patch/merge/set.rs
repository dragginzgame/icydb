use crate::{
    patch::{SetPatch, merge::error::MergePatchError},
    traits::UpdateView,
};
use std::{
    collections::{BTreeSet, HashSet},
    hash::{BuildHasher, Hash},
    marker::PhantomData,
};

/// Storage adapter for set merge operations.
trait SetAdapter<T> {
    type Set;

    fn insert(values: &mut Self::Set, value: T);
    fn remove(values: &mut Self::Set, value: &T);
    fn clear(values: &mut Self::Set);
}

/// HashSet-backed set adapter.
struct HashSetAdapter<S>(PhantomData<S>);

impl<T, S> SetAdapter<T> for HashSetAdapter<S>
where
    T: Eq + Hash,
    S: BuildHasher + Default,
{
    type Set = HashSet<T, S>;

    fn insert(values: &mut Self::Set, value: T) {
        values.insert(value);
    }

    fn remove(values: &mut Self::Set, value: &T) {
        values.remove(value);
    }

    fn clear(values: &mut Self::Set) {
        values.clear();
    }
}

/// BTreeSet-backed set adapter.
struct BTreeSetAdapter;

impl<T> SetAdapter<T> for BTreeSetAdapter
where
    T: Ord,
{
    type Set = BTreeSet<T>;

    fn insert(values: &mut Self::Set, value: T) {
        values.insert(value);
    }

    fn remove(values: &mut Self::Set, value: &T) {
        values.remove(value);
    }

    fn clear(values: &mut Self::Set) {
        values.clear();
    }
}

// Shared set merge pipeline for all set backends.
fn merge_set<T, A>(
    values: &mut A::Set,
    patches: Vec<SetPatch<T::UpdateViewType>>,
) -> Result<(), MergePatchError>
where
    T: UpdateView + Clone + Default,
    A: SetAdapter<T>,
{
    for patch in patches {
        match patch {
            SetPatch::Insert(value) => {
                let mut elem = T::default();
                elem.merge(value).map_err(|err| err.with_field("insert"))?;
                A::insert(values, elem);
            }
            SetPatch::Remove(value) => {
                let mut elem = T::default();
                elem.merge(value).map_err(|err| err.with_field("remove"))?;
                A::remove(values, &elem);
            }
            SetPatch::Overwrite {
                values: next_values,
            } => {
                A::clear(values);

                for (index, value) in next_values.into_iter().enumerate() {
                    let mut elem = T::default();
                    elem.merge(value)
                        .map_err(|err| err.with_field("overwrite").with_index(index))?;
                    A::insert(values, elem);
                }
            }
            SetPatch::Clear => A::clear(values),
        }
    }

    Ok(())
}

/// Apply set patch operations for hash sets.
pub fn merge_hash_set<T, S>(
    values: &mut HashSet<T, S>,
    patches: Vec<SetPatch<T::UpdateViewType>>,
) -> Result<(), MergePatchError>
where
    T: UpdateView + Clone + Default + Eq + Hash,
    S: BuildHasher + Default,
{
    merge_set::<T, HashSetAdapter<S>>(values, patches)
}

/// Apply set patch operations for ordered sets.
pub fn merge_btree_set<T>(
    values: &mut BTreeSet<T>,
    patches: Vec<SetPatch<T::UpdateViewType>>,
) -> Result<(), MergePatchError>
where
    T: UpdateView + Clone + Default + Ord,
{
    merge_set::<T, BTreeSetAdapter>(values, patches)
}
