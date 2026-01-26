use crate::{
    error::{ErrorClass, ErrorOrigin, InternalError},
    view::{ListPatch, MapPatch, SetPatch},
};
use candid::CandidType;
use std::{
    collections::{HashMap, HashSet, hash_map::Entry as HashMapEntry},
    hash::{BuildHasher, Hash},
    iter::IntoIterator,
};
use thiserror::Error as ThisError;

///
/// View
/// Recursive for all field/value nodes
/// `from_view` is fallible and must reject invalid view values.
///

pub trait View: Sized {
    type ViewType: Default;

    fn to_view(&self) -> Self::ViewType;
    fn from_view(view: Self::ViewType) -> Result<Self, ViewError>;
}

///
/// ViewError
/// Errors returned when converting view types into core values.
///

#[derive(Debug, ThisError)]
pub enum ViewError {
    #[error("Float32 view value must be finite (got {value})")]
    Float32NonFinite { value: f32 },

    #[error("Float64 view value must be finite (got {value})")]
    Float64NonFinite { value: f64 },
}

impl From<ViewError> for InternalError {
    fn from(err: ViewError) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Interface,
            err.to_string(),
        )
    }
}

impl View for String {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Result<Self, ViewError> {
        Ok(view)
    }
}

// Make Box<T> *not* appear in the view type
impl<T: View> View for Box<T> {
    type ViewType = T::ViewType;

    fn to_view(&self) -> Self::ViewType {
        // Delegate to inner value
        T::to_view(self.as_ref())
    }

    fn from_view(view: Self::ViewType) -> Result<Self, ViewError> {
        // Re-box after reconstructing inner
        Ok(Self::new(T::from_view(view)?))
    }
}

impl<T: View> View for Option<T> {
    type ViewType = Option<T::ViewType>;

    fn to_view(&self) -> Self::ViewType {
        self.as_ref().map(View::to_view)
    }

    fn from_view(view: Self::ViewType) -> Result<Self, ViewError> {
        match view {
            Some(inner) => Ok(Some(T::from_view(inner)?)),
            None => Ok(None),
        }
    }
}

impl<T: View> View for Vec<T> {
    type ViewType = Vec<T::ViewType>;

    fn to_view(&self) -> Self::ViewType {
        self.iter().map(View::to_view).collect()
    }

    fn from_view(view: Self::ViewType) -> Result<Self, ViewError> {
        view.into_iter().map(T::from_view).collect()
    }
}

impl<T, S> View for HashSet<T, S>
where
    T: View + Eq + Hash + Clone,
    S: BuildHasher + Default,
{
    type ViewType = Vec<T::ViewType>;

    fn to_view(&self) -> Self::ViewType {
        self.iter().map(View::to_view).collect()
    }

    fn from_view(view: Self::ViewType) -> Result<Self, ViewError> {
        view.into_iter().map(T::from_view).collect()
    }
}

impl<K, V, S> View for HashMap<K, V, S>
where
    K: View + Eq + Hash + Clone,
    V: View,
    S: BuildHasher + Default,
{
    type ViewType = Vec<(K::ViewType, V::ViewType)>;

    fn to_view(&self) -> Self::ViewType {
        self.iter()
            .map(|(k, v)| (k.to_view(), v.to_view()))
            .collect()
    }

    fn from_view(view: Self::ViewType) -> Result<Self, ViewError> {
        view.into_iter()
            .map(|(k, v)| Ok((K::from_view(k)?, V::from_view(v)?)))
            .collect()
    }
}

#[macro_export]
macro_rules! impl_view {
    ($($type:ty),*) => {
        $(
            impl View for $type {
                type ViewType = Self;

                fn to_view(&self) -> Self::ViewType {
                    *self
                }

                fn from_view(view: Self::ViewType) -> Result<Self, ViewError> {
                    Ok(view)
                }
            }
        )*
    };
}

impl_view!(bool, i8, i16, i32, i64, u8, u16, u32, u64);

impl View for f32 {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Result<Self, ViewError> {
        if view.is_finite() {
            Ok(view)
        } else {
            Err(ViewError::Float32NonFinite { value: view })
        }
    }
}

impl View for f64 {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Result<Self, ViewError> {
        if view.is_finite() {
            Ok(view)
        } else {
            Err(ViewError::Float64NonFinite { value: view })
        }
    }
}

///
/// CreateView
///

pub trait CreateView {
    type CreateViewType: CandidType + Default;
}

///
/// UpdateView
///

pub trait UpdateView {
    type UpdateViewType: CandidType + Default;

    /// merge the updateview into self
    fn merge(&mut self, _: Self::UpdateViewType) -> Result<(), ViewError> {
        Ok(())
    }
}

impl<T> UpdateView for Option<T>
where
    T: UpdateView + Default,
{
    type UpdateViewType = Option<T::UpdateViewType>;

    fn merge(&mut self, update: Self::UpdateViewType) -> Result<(), ViewError> {
        match update {
            None => {
                // Field was provided (outer Some), inner None means explicit delete
                *self = None;
            }
            Some(inner_update) => {
                if let Some(inner_value) = self.as_mut() {
                    inner_value.merge(inner_update)?;
                } else {
                    let mut new_value = T::default();
                    new_value.merge(inner_update)?;
                    *self = Some(new_value);
                }
            }
        }

        Ok(())
    }
}

impl<T> UpdateView for Vec<T>
where
    T: UpdateView + Default,
{
    // Payload is T::UpdateViewType, which *is* CandidType
    type UpdateViewType = Vec<ListPatch<T::UpdateViewType>>;

    fn merge(&mut self, patches: Self::UpdateViewType) -> Result<(), ViewError> {
        for patch in patches {
            match patch {
                ListPatch::Update { index, patch } => {
                    if let Some(elem) = self.get_mut(index) {
                        elem.merge(patch)?;
                    }
                }
                ListPatch::Insert { index, value } => {
                    let mut elem = T::default();
                    elem.merge(value)?;
                    let idx = index.min(self.len());
                    self.insert(idx, elem);
                }
                ListPatch::Push { value } => {
                    let mut elem = T::default();
                    elem.merge(value)?;
                    self.push(elem);
                }
                ListPatch::Overwrite { values } => {
                    self.clear();
                    self.reserve(values.len());

                    for value in values {
                        let mut elem = T::default();
                        elem.merge(value)?;
                        self.push(elem);
                    }
                }
                ListPatch::Remove { index } => {
                    if index < self.len() {
                        self.remove(index);
                    }
                }
                ListPatch::Clear => self.clear(),
            }
        }

        Ok(())
    }
}

impl<T, S> UpdateView for HashSet<T, S>
where
    T: UpdateView + Default + Eq + Hash,
    S: BuildHasher + Default,
{
    type UpdateViewType = Vec<SetPatch<T::UpdateViewType>>;

    fn merge(&mut self, patches: Self::UpdateViewType) -> Result<(), ViewError> {
        for patch in patches {
            match patch {
                SetPatch::Insert(value) => {
                    let mut elem = T::default();
                    elem.merge(value)?;
                    self.insert(elem);
                }
                SetPatch::Remove(value) => {
                    let mut elem = T::default();
                    elem.merge(value)?;
                    self.remove(&elem);
                }
                SetPatch::Overwrite { values } => {
                    self.clear();

                    for value in values {
                        let mut elem = T::default();
                        elem.merge(value)?;
                        self.insert(elem);
                    }
                }
                SetPatch::Clear => self.clear(),
            }
        }

        Ok(())
    }
}

impl<K, V, S> UpdateView for HashMap<K, V, S>
where
    K: UpdateView + Default + Eq + Hash,
    V: UpdateView + Default,
    S: BuildHasher + Default,
{
    type UpdateViewType = Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>;

    fn merge(&mut self, patches: Self::UpdateViewType) -> Result<(), ViewError> {
        for patch in patches {
            match patch {
                MapPatch::Upsert { key, value } => {
                    let mut key_value = K::default();
                    key_value.merge(key)?;

                    match self.entry(key_value) {
                        HashMapEntry::Occupied(mut slot) => {
                            slot.get_mut().merge(value)?;
                        }
                        HashMapEntry::Vacant(slot) => {
                            let mut value_value = V::default();
                            value_value.merge(value)?;
                            slot.insert(value_value);
                        }
                    }
                }
                MapPatch::Remove { key } => {
                    let mut key_value = K::default();
                    key_value.merge(key)?;
                    self.remove(&key_value);
                }
                MapPatch::Overwrite { entries } => {
                    self.clear();
                    self.reserve(entries.len());

                    for (key, value) in entries {
                        let mut key_value = K::default();
                        key_value.merge(key)?;

                        let mut value_value = V::default();
                        value_value.merge(value)?;
                        self.insert(key_value, value_value);
                    }
                }
                MapPatch::Clear => self.clear(),
            }
        }

        Ok(())
    }
}

macro_rules! impl_update_view {
    ($($type:ty),*) => {
        $(
            impl UpdateView for $type {
                type UpdateViewType = Self;

                fn merge(&mut self, update: Self::UpdateViewType) -> Result<(), ViewError> {
                    *self = update;
                    Ok(())
                }
            }
        )*
    };
}

impl_update_view!(bool, i8, i16, i32, i64, u8, u16, u32, u64, String);
