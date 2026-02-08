use crate::{
    error::{ErrorClass, ErrorDetail, ErrorOrigin, InternalError},
    view::{ListPatch, MapPatch, SetPatch},
};
use candid::CandidType;
use std::{
    collections::{
        BTreeMap, BTreeSet, HashMap, HashSet, btree_map::Entry as BTreeMapEntry,
        hash_map::Entry as HashMapEntry,
    },
    hash::{BuildHasher, Hash},
    iter::IntoIterator,
};
use thiserror::Error as ThisError;

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
/// ViewPatchError
///
/// Structured failures for user-driven patch application.
///
#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum ViewPatchError {
    #[error("invalid patch shape: expected {expected}, found {actual}")]
    InvalidPatchShape {
        expected: &'static str,
        actual: &'static str,
    },

    #[error("missing key for map operation: {operation}")]
    MissingKey { operation: &'static str },

    #[error("invalid patch cardinality: expected {expected}, found {actual}")]
    CardinalityViolation { expected: usize, actual: usize },

    #[error("patch merge failed at {path}: {source}")]
    Context {
        path: String,
        #[source]
        source: Box<Self>,
    },
}

impl ViewPatchError {
    /// Prepend a field segment to the merge error path.
    #[must_use]
    pub fn with_field(self, field: impl AsRef<str>) -> Self {
        self.with_path_segment(field.as_ref())
    }

    /// Prepend an index segment to the merge error path.
    #[must_use]
    pub fn with_index(self, index: usize) -> Self {
        self.with_path_segment(format!("[{index}]"))
    }

    /// Return the full contextual path, if available.
    #[must_use]
    pub const fn path(&self) -> Option<&str> {
        match self {
            Self::Context { path, .. } => Some(path.as_str()),
            _ => None,
        }
    }

    /// Return the innermost, non-context merge error variant.
    #[must_use]
    pub fn leaf(&self) -> &Self {
        match self {
            Self::Context { source, .. } => source.leaf(),
            _ => self,
        }
    }

    #[must_use]
    fn with_path_segment(self, segment: impl Into<String>) -> Self {
        let segment = segment.into();
        match self {
            Self::Context { path, source } => Self::Context {
                path: Self::join_segments(segment.as_str(), path.as_str()),
                source,
            },
            source => Self::Context {
                path: segment,
                source: Box::new(source),
            },
        }
    }

    #[must_use]
    fn join_segments(prefix: &str, suffix: &str) -> String {
        if suffix.starts_with('[') {
            format!("{prefix}{suffix}")
        } else {
            format!("{prefix}.{suffix}")
        }
    }
}

///
/// Error
///
/// Stable merge error returned by `UpdateView::merge`.
/// Preserves structured patch detail through `ErrorDetail::ViewPatch`.
///
pub type Error = InternalError;

impl From<ViewPatchError> for Error {
    fn from(err: ViewPatchError) -> Self {
        let class = match err.leaf() {
            ViewPatchError::MissingKey { .. } => ErrorClass::NotFound,
            _ => ErrorClass::Unsupported,
        };

        Self {
            class,
            origin: ErrorOrigin::Interface,
            message: err.to_string(),
            detail: Some(ErrorDetail::ViewPatch(err)),
        }
    }
}

impl InternalError {
    /// Prepend a field segment when the error contains view-patch detail.
    #[must_use]
    pub fn with_field(self, field: impl AsRef<str>) -> Self {
        let field = field.as_ref().to_string();
        self.with_view_patch_context(|err| err.with_field(field))
    }

    /// Prepend an index segment when the error contains view-patch detail.
    #[must_use]
    pub fn with_index(self, index: usize) -> Self {
        self.with_view_patch_context(|err| err.with_index(index))
    }

    /// Return the contextual patch path when available.
    #[must_use]
    pub const fn path(&self) -> Option<&str> {
        match &self.detail {
            Some(ErrorDetail::ViewPatch(err)) => err.path(),
            _ => None,
        }
    }

    /// Return the innermost patch leaf when available.
    #[must_use]
    pub fn leaf(&self) -> Option<&ViewPatchError> {
        match &self.detail {
            Some(ErrorDetail::ViewPatch(err)) => Some(err.leaf()),
            _ => None,
        }
    }

    #[must_use]
    fn with_view_patch_context(self, map: impl FnOnce(ViewPatchError) -> ViewPatchError) -> Self {
        // Preserve non-patch errors as-is; only rewrite structured patch detail.
        let Self {
            class,
            origin,
            message,
            detail,
        } = self;

        match detail {
            Some(ErrorDetail::ViewPatch(err)) => Self::from(map(err)),
            detail => Self {
                class,
                origin,
                message,
                detail,
            },
        }
    }
}

///
/// UpdateView
///

pub trait UpdateView: AsView {
    /// Payload accepted when updating this value.
    type UpdateViewType: CandidType + Default;

    /// Merge the update payload into self.
    fn merge(&mut self, _update: Self::UpdateViewType) -> Result<(), Error> {
        Ok(())
    }
}

impl<T> UpdateView for Option<T>
where
    T: UpdateView + Default,
{
    type UpdateViewType = Option<T::UpdateViewType>;

    fn merge(&mut self, update: Self::UpdateViewType) -> Result<(), Error> {
        match update {
            None => {
                // Field was provided (outer Some), inner None means explicit delete
                *self = None;
            }
            Some(inner_update) => {
                if let Some(inner_value) = self.as_mut() {
                    inner_value
                        .merge(inner_update)
                        .map_err(|err| err.with_field("value"))?;
                } else {
                    let mut new_value = T::default();
                    new_value
                        .merge(inner_update)
                        .map_err(|err| err.with_field("value"))?;
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

    fn merge(&mut self, patches: Self::UpdateViewType) -> Result<(), Error> {
        for patch in patches {
            match patch {
                ListPatch::Update { index, patch } => {
                    if let Some(elem) = self.get_mut(index) {
                        elem.merge(patch).map_err(|err| err.with_index(index))?;
                    }
                }
                ListPatch::Insert { index, value } => {
                    let idx = index.min(self.len());
                    let mut elem = T::default();
                    elem.merge(value).map_err(|err| err.with_index(idx))?;
                    self.insert(idx, elem);
                }
                ListPatch::Push { value } => {
                    let idx = self.len();
                    let mut elem = T::default();
                    elem.merge(value).map_err(|err| err.with_index(idx))?;
                    self.push(elem);
                }
                ListPatch::Overwrite { values } => {
                    self.clear();
                    self.reserve(values.len());

                    for (index, value) in values.into_iter().enumerate() {
                        let mut elem = T::default();
                        elem.merge(value).map_err(|err| err.with_index(index))?;
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
    T: UpdateView + Clone + Default + Eq + Hash,
    S: BuildHasher + Default,
{
    type UpdateViewType = Vec<SetPatch<T::UpdateViewType>>;

    fn merge(&mut self, patches: Self::UpdateViewType) -> Result<(), Error> {
        for patch in patches {
            match patch {
                SetPatch::Insert(value) => {
                    let mut elem = T::default();
                    elem.merge(value).map_err(|err| err.with_field("insert"))?;
                    self.insert(elem);
                }
                SetPatch::Remove(value) => {
                    let mut elem = T::default();
                    elem.merge(value).map_err(|err| err.with_field("remove"))?;
                    self.remove(&elem);
                }
                SetPatch::Overwrite { values } => {
                    self.clear();

                    for (index, value) in values.into_iter().enumerate() {
                        let mut elem = T::default();
                        elem.merge(value)
                            .map_err(|err| err.with_field("overwrite").with_index(index))?;
                        self.insert(elem);
                    }
                }
                SetPatch::Clear => self.clear(),
            }
        }

        Ok(())
    }
}

/// Internal representation used to normalize map patches before application.
enum MapPatchOp<K, V> {
    Insert { key: K, value: V },
    Remove { key: K },
    Replace { key: K, value: V },
    Clear,
}

impl<K, V, S> UpdateView for HashMap<K, V, S>
where
    K: UpdateView + Clone + Default + Eq + Hash,
    V: UpdateView + Default,
    S: BuildHasher + Default,
{
    type UpdateViewType = Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>;

    #[expect(clippy::too_many_lines)]
    fn merge(&mut self, patches: Self::UpdateViewType) -> Result<(), Error> {
        // Phase 1: decode patch payload into concrete keys.
        let mut ops = Vec::with_capacity(patches.len());
        for patch in patches {
            match patch {
                MapPatch::Insert { key, value } => {
                    let mut key_value = K::default();
                    key_value
                        .merge(key)
                        .map_err(|err| err.with_field("insert").with_field("key"))?;
                    ops.push(MapPatchOp::Insert {
                        key: key_value,
                        value,
                    });
                }
                MapPatch::Remove { key } => {
                    let mut key_value = K::default();
                    key_value
                        .merge(key)
                        .map_err(|err| err.with_field("remove").with_field("key"))?;
                    ops.push(MapPatchOp::Remove { key: key_value });
                }
                MapPatch::Replace { key, value } => {
                    let mut key_value = K::default();
                    key_value
                        .merge(key)
                        .map_err(|err| err.with_field("replace").with_field("key"))?;
                    ops.push(MapPatchOp::Replace {
                        key: key_value,
                        value,
                    });
                }
                MapPatch::Clear => ops.push(MapPatchOp::Clear),
            }
        }

        // Phase 2: reject ambiguous patch batches to keep semantics deterministic.
        let mut saw_clear = false;
        let mut touched = HashSet::with_capacity(ops.len());
        for op in &ops {
            match op {
                MapPatchOp::Clear => {
                    if saw_clear {
                        return Err(ViewPatchError::InvalidPatchShape {
                            expected: "at most one Clear operation per map patch batch",
                            actual: "duplicate Clear operations",
                        }
                        .into());
                    }
                    saw_clear = true;
                    if ops.len() != 1 {
                        return Err(ViewPatchError::CardinalityViolation {
                            expected: 1,
                            actual: ops.len(),
                        }
                        .into());
                    }
                }
                MapPatchOp::Insert { key, .. }
                | MapPatchOp::Remove { key }
                | MapPatchOp::Replace { key, .. } => {
                    if saw_clear {
                        return Err(ViewPatchError::InvalidPatchShape {
                            expected: "Clear must be the only operation in a map patch batch",
                            actual: "Clear combined with key operation",
                        }
                        .into());
                    }
                    if !touched.insert(key.clone()) {
                        return Err(ViewPatchError::InvalidPatchShape {
                            expected: "unique key operations per map patch batch",
                            actual: "duplicate key operation",
                        }
                        .into());
                    }
                }
            }
        }
        if saw_clear {
            self.clear();
            return Ok(());
        }

        // Phase 3: apply deterministic map operations.
        for op in ops {
            match op {
                MapPatchOp::Insert { key, value } => match self.entry(key) {
                    HashMapEntry::Occupied(mut slot) => {
                        slot.get_mut()
                            .merge(value)
                            .map_err(|err| err.with_field("insert").with_field("value"))?;
                    }
                    HashMapEntry::Vacant(slot) => {
                        let mut value_value = V::default();
                        value_value
                            .merge(value)
                            .map_err(|err| err.with_field("insert").with_field("value"))?;
                        slot.insert(value_value);
                    }
                },
                MapPatchOp::Remove { key } => {
                    if self.remove(&key).is_none() {
                        return Err(ViewPatchError::MissingKey {
                            operation: "remove",
                        }
                        .into());
                    }
                }
                MapPatchOp::Replace { key, value } => match self.entry(key) {
                    HashMapEntry::Occupied(mut slot) => {
                        slot.get_mut()
                            .merge(value)
                            .map_err(|err| err.with_field("replace").with_field("value"))?;
                    }
                    HashMapEntry::Vacant(_) => {
                        return Err(ViewPatchError::MissingKey {
                            operation: "replace",
                        }
                        .into());
                    }
                },
                MapPatchOp::Clear => {
                    return Err(ViewPatchError::InvalidPatchShape {
                        expected: "Clear to be handled before apply phase",
                        actual: "Clear reached apply phase",
                    }
                    .into());
                }
            }
        }

        Ok(())
    }
}

impl<T> UpdateView for BTreeSet<T>
where
    T: UpdateView + Clone + Default + Ord,
{
    type UpdateViewType = Vec<SetPatch<T::UpdateViewType>>;

    fn merge(&mut self, patches: Self::UpdateViewType) -> Result<(), Error> {
        for patch in patches {
            match patch {
                SetPatch::Insert(value) => {
                    let mut elem = T::default();
                    elem.merge(value).map_err(|err| err.with_field("insert"))?;
                    self.insert(elem);
                }
                SetPatch::Remove(value) => {
                    let mut elem = T::default();
                    elem.merge(value).map_err(|err| err.with_field("remove"))?;
                    self.remove(&elem);
                }
                SetPatch::Overwrite { values } => {
                    self.clear();

                    for (index, value) in values.into_iter().enumerate() {
                        let mut elem = T::default();
                        elem.merge(value)
                            .map_err(|err| err.with_field("overwrite").with_index(index))?;
                        self.insert(elem);
                    }
                }
                SetPatch::Clear => self.clear(),
            }
        }

        Ok(())
    }
}

impl<K, V> UpdateView for BTreeMap<K, V>
where
    K: UpdateView + Clone + Default + Ord,
    V: UpdateView + Default,
{
    type UpdateViewType = Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>;

    #[expect(clippy::too_many_lines)]
    fn merge(&mut self, patches: Self::UpdateViewType) -> Result<(), Error> {
        // Phase 1: decode patch payload into concrete keys.
        let mut ops = Vec::with_capacity(patches.len());
        for patch in patches {
            match patch {
                MapPatch::Insert { key, value } => {
                    let mut key_value = K::default();
                    key_value
                        .merge(key)
                        .map_err(|err| err.with_field("insert").with_field("key"))?;
                    ops.push(MapPatchOp::Insert {
                        key: key_value,
                        value,
                    });
                }
                MapPatch::Remove { key } => {
                    let mut key_value = K::default();
                    key_value
                        .merge(key)
                        .map_err(|err| err.with_field("remove").with_field("key"))?;
                    ops.push(MapPatchOp::Remove { key: key_value });
                }
                MapPatch::Replace { key, value } => {
                    let mut key_value = K::default();
                    key_value
                        .merge(key)
                        .map_err(|err| err.with_field("replace").with_field("key"))?;
                    ops.push(MapPatchOp::Replace {
                        key: key_value,
                        value,
                    });
                }
                MapPatch::Clear => ops.push(MapPatchOp::Clear),
            }
        }

        // Phase 2: reject ambiguous patch batches to keep semantics deterministic.
        let mut saw_clear = false;
        let mut touched = BTreeSet::new();
        for op in &ops {
            match op {
                MapPatchOp::Clear => {
                    if saw_clear {
                        return Err(ViewPatchError::InvalidPatchShape {
                            expected: "at most one Clear operation per map patch batch",
                            actual: "duplicate Clear operations",
                        }
                        .into());
                    }
                    saw_clear = true;
                    if ops.len() != 1 {
                        return Err(ViewPatchError::CardinalityViolation {
                            expected: 1,
                            actual: ops.len(),
                        }
                        .into());
                    }
                }
                MapPatchOp::Insert { key, .. }
                | MapPatchOp::Remove { key }
                | MapPatchOp::Replace { key, .. } => {
                    if saw_clear {
                        return Err(ViewPatchError::InvalidPatchShape {
                            expected: "Clear must be the only operation in a map patch batch",
                            actual: "Clear combined with key operation",
                        }
                        .into());
                    }
                    if !touched.insert(key.clone()) {
                        return Err(ViewPatchError::InvalidPatchShape {
                            expected: "unique key operations per map patch batch",
                            actual: "duplicate key operation",
                        }
                        .into());
                    }
                }
            }
        }
        if saw_clear {
            self.clear();
            return Ok(());
        }

        // Phase 3: apply deterministic map operations.
        for op in ops {
            match op {
                MapPatchOp::Insert { key, value } => match self.entry(key) {
                    BTreeMapEntry::Occupied(mut slot) => {
                        slot.get_mut()
                            .merge(value)
                            .map_err(|err| err.with_field("insert").with_field("value"))?;
                    }
                    BTreeMapEntry::Vacant(slot) => {
                        let mut value_value = V::default();
                        value_value
                            .merge(value)
                            .map_err(|err| err.with_field("insert").with_field("value"))?;
                        slot.insert(value_value);
                    }
                },
                MapPatchOp::Remove { key } => {
                    if self.remove(&key).is_none() {
                        return Err(ViewPatchError::MissingKey {
                            operation: "remove",
                        }
                        .into());
                    }
                }
                MapPatchOp::Replace { key, value } => match self.entry(key) {
                    BTreeMapEntry::Occupied(mut slot) => {
                        slot.get_mut()
                            .merge(value)
                            .map_err(|err| err.with_field("replace").with_field("value"))?;
                    }
                    BTreeMapEntry::Vacant(_) => {
                        return Err(ViewPatchError::MissingKey {
                            operation: "replace",
                        }
                        .into());
                    }
                },
                MapPatchOp::Clear => {
                    return Err(ViewPatchError::InvalidPatchShape {
                        expected: "Clear to be handled before apply phase",
                        actual: "Clear reached apply phase",
                    }
                    .into());
                }
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

                fn merge(
                    &mut self,
                    update: Self::UpdateViewType,
                ) -> Result<(), Error> {
                    *self = update;

                    Ok(())
                }
            }
        )*
    };
}

impl_update_view!(bool, i8, i16, i32, i64, u8, u16, u32, u64, String);
