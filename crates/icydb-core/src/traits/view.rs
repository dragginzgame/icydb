use crate::{
    patch::{ListPatch, MapPatch, MergePatchError, SetPatch, merge},
    traits::Atomic,
};
use candid::CandidType;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::{BuildHasher, Hash},
};

///
/// AsView
///
/// Recursive for all field/value nodes
/// `from_view` is infallible; view values are treated as canonical.
///

pub trait AsView: Sized {
    type ViewType: Default;

    fn as_view(&self) -> Self::ViewType;
    fn from_view(view: Self::ViewType) -> Self;
}

impl AsView for () {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {}
    fn from_view((): Self::ViewType) -> Self {}
}

impl AsView for String {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

// Make Box<T> *not* appear in the view type
impl<T: AsView> AsView for Box<T> {
    type ViewType = T::ViewType;

    fn as_view(&self) -> Self::ViewType {
        // Delegate to inner value
        T::as_view(self.as_ref())
    }

    fn from_view(view: Self::ViewType) -> Self {
        // Re-box after reconstructing inner
        Self::new(T::from_view(view))
    }
}

impl<T: AsView> AsView for Option<T> {
    type ViewType = Option<T::ViewType>;

    fn as_view(&self) -> Self::ViewType {
        self.as_ref().map(AsView::as_view)
    }

    fn from_view(view: Self::ViewType) -> Self {
        view.map(T::from_view)
    }
}

impl<T: AsView> AsView for Vec<T> {
    type ViewType = Vec<T::ViewType>;

    fn as_view(&self) -> Self::ViewType {
        self.iter().map(AsView::as_view).collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view.into_iter().map(T::from_view).collect()
    }
}

impl<T, S> AsView for HashSet<T, S>
where
    T: AsView + Eq + Hash + Clone,
    S: BuildHasher + Default,
{
    type ViewType = Vec<T::ViewType>;

    fn as_view(&self) -> Self::ViewType {
        self.iter().map(AsView::as_view).collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view.into_iter().map(T::from_view).collect()
    }
}

impl<K, V, S> AsView for HashMap<K, V, S>
where
    K: AsView + Eq + Hash + Clone,
    V: AsView,
    S: BuildHasher + Default,
{
    type ViewType = Vec<(K::ViewType, V::ViewType)>;

    fn as_view(&self) -> Self::ViewType {
        self.iter()
            .map(|(k, v)| (k.as_view(), v.as_view()))
            .collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view.into_iter()
            .map(|(k, v)| (K::from_view(k), V::from_view(v)))
            .collect()
    }
}

impl<T> AsView for BTreeSet<T>
where
    T: AsView + Ord + Clone,
{
    type ViewType = Vec<T::ViewType>;

    fn as_view(&self) -> Self::ViewType {
        self.iter().map(AsView::as_view).collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view.into_iter().map(T::from_view).collect()
    }
}

impl<K, V> AsView for BTreeMap<K, V>
where
    K: AsView + Ord + Clone,
    V: AsView,
{
    type ViewType = Vec<(K::ViewType, V::ViewType)>;

    fn as_view(&self) -> Self::ViewType {
        self.iter()
            .map(|(k, v)| (k.as_view(), v.as_view()))
            .collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view.into_iter()
            .map(|(k, v)| (K::from_view(k), V::from_view(v)))
            .collect()
    }
}

#[macro_export]
macro_rules! impl_view {
    ($($type:ty),*) => {
        $(
            impl AsView for $type {
                type ViewType = Self;

                fn as_view(&self) -> Self::ViewType {
                    *self
                }

                fn from_view(view: Self::ViewType) -> Self {
                    view
                }
            }
        )*
    };
}

impl_view!(bool, i8, i16, i32, i64, u8, u16, u32, u64);

impl AsView for f32 {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        if view.is_finite() {
            if view == 0.0 { 0.0 } else { view }
        } else {
            0.0
        }
    }
}

impl AsView for f64 {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        if view.is_finite() {
            if view == 0.0 { 0.0 } else { view }
        } else {
            0.0
        }
    }
}

///
/// CreateView
///

pub trait CreateView: AsView {
    /// Payload accepted when creating this value.
    ///
    /// This is often equal to ViewType, but may differ
    /// (e.g. Option<T>, defaults, omissions).
    type CreateViewType: CandidType + Default;

    fn from_create_view(view: Self::CreateViewType) -> Self;
}

///
/// UpdateView
///

pub trait UpdateView: AsView {
    /// A view payload that may be applied to `Self`.
    type UpdateViewType: CandidType + Default;
    fn merge(&mut self, patch: Self::UpdateViewType) -> Result<(), MergePatchError>;
}

impl<T> UpdateView for T
where
    T: Atomic + AsView + CandidType + Default,
{
    type UpdateViewType = Self;

    fn merge(&mut self, patch: Self::UpdateViewType) -> Result<(), MergePatchError> {
        merge::merge_atomic(self, patch)
    }
}

impl<T> UpdateView for Option<T>
where
    T: UpdateView + Default,
{
    type UpdateViewType = Option<T::UpdateViewType>;

    fn merge(&mut self, patch: Self::UpdateViewType) -> Result<(), MergePatchError> {
        merge::merge_option(self, patch)
    }
}

impl<T> UpdateView for Vec<T>
where
    T: UpdateView + Default,
{
    type UpdateViewType = Vec<ListPatch<T::UpdateViewType>>;

    fn merge(&mut self, patch: Self::UpdateViewType) -> Result<(), MergePatchError> {
        merge::merge_vec(self, patch)
    }
}

impl<T, S> UpdateView for HashSet<T, S>
where
    T: UpdateView + Clone + Default + Eq + Hash,
    S: BuildHasher + Default,
{
    type UpdateViewType = Vec<SetPatch<T::UpdateViewType>>;

    fn merge(&mut self, patch: Self::UpdateViewType) -> Result<(), MergePatchError> {
        merge::merge_hash_set(self, patch)
    }
}

impl<K, V, S> UpdateView for HashMap<K, V, S>
where
    K: UpdateView + Clone + Default + Eq + Hash,
    V: UpdateView + Default,
    S: BuildHasher + Default,
{
    type UpdateViewType = Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>;

    fn merge(&mut self, patch: Self::UpdateViewType) -> Result<(), MergePatchError> {
        merge::merge_hash_map(self, patch)
    }
}

impl<T> UpdateView for BTreeSet<T>
where
    T: UpdateView + Clone + Default + Ord,
{
    type UpdateViewType = Vec<SetPatch<T::UpdateViewType>>;

    fn merge(&mut self, patch: Self::UpdateViewType) -> Result<(), MergePatchError> {
        merge::merge_btree_set(self, patch)
    }
}

impl<K, V> UpdateView for BTreeMap<K, V>
where
    K: UpdateView + Clone + Default + Ord,
    V: UpdateView + Default,
{
    type UpdateViewType = Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>;

    fn merge(&mut self, patch: Self::UpdateViewType) -> Result<(), MergePatchError> {
        merge::merge_btree_map(self, patch)
    }
}
