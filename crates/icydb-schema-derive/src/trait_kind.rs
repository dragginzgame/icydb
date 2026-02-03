use crate::prelude::*;
use darling::{Error as DarlingError, FromMeta, ast::NestedMeta};
use derive_more::{Deref, DerefMut, Display, FromStr, IntoIterator};
use std::{collections::HashSet, hash::Hash, str::FromStr, sync::LazyLock};

///
/// TraitKind
///

#[derive(
    Clone,
    Copy,
    Debug,
    Display,
    Eq,
    PartialEq,
    FromStr,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
)]
pub enum TraitKind {
    // inherent impl
    Inherent,

    // rust + third party
    CandidType,
    Clone,
    Copy,
    Debug,
    Default,
    Deserialize,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,

    // math
    Add,
    AddAssign,
    Div,
    DivAssign,
    Mul,
    MulAssign,
    Rem,
    Sub,
    SubAssign,
    Sum,

    // kind
    CanisterKind,
    DataStoreKind,
    EntityKind,
    IndexStoreKind,

    // value
    EntityValue,
    EnumValue,
    FieldValue,
    FieldValues,

    // orm
    CreateView,
    UpdateView,
    From,
    Inner,
    NumCast,
    NumFromPrimitive,
    NumToPrimitive,
    Path,
    Sorted,
    SanitizeAuto,
    SanitizeCustom,
    ValidateAuto,
    ValidateCustom,
    View,
    Visitable,
}

static DEFAULT_TRAITS: LazyLock<Vec<TraitKind>> =
    LazyLock::new(|| vec![TraitKind::Clone, TraitKind::Debug, TraitKind::Path]);

static TYPE_TRAITS: LazyLock<Vec<TraitKind>> = LazyLock::new(|| {
    vec![
        TraitKind::Default,
        TraitKind::Deserialize,
        TraitKind::Eq,
        TraitKind::FieldValue,
        TraitKind::From,
        TraitKind::PartialEq,
        TraitKind::SanitizeAuto,
        TraitKind::SanitizeCustom,
        TraitKind::Serialize,
        TraitKind::UpdateView,
        TraitKind::ValidateAuto,
        TraitKind::ValidateCustom,
        TraitKind::View,
        TraitKind::Visitable,
    ]
});

// path_to_string
#[must_use]
fn path_to_string(path: &syn::Path) -> String {
    path.to_token_stream()
        .to_string()
        .replace(' ', "")
        .trim_matches(':')
        .to_string()
}

impl TraitKind {
    #[must_use]
    #[remain::check]
    pub(crate) fn derive_path(self) -> Option<TokenStream> {
        #[remain::sorted]
        match self {
            Self::Add => Some(quote!(::icydb::__reexports::icydb_derive::Add)),
            Self::AddAssign => Some(quote!(::icydb::__reexports::icydb_derive::AddAssign)),
            Self::CandidType => Some(quote!(::candid::CandidType)),
            Self::Clone => Some(quote!(Clone)),
            Self::Copy => Some(quote!(Copy)),
            Self::Debug => Some(quote!(Debug)),
            Self::Default => Some(quote!(Default)),
            Self::Deserialize => Some(quote!(::serde::Deserialize)),
            Self::Div => Some(quote!(::icydb::__reexports::icydb_derive::Div)),
            Self::DivAssign => Some(quote!(::icydb::__reexports::icydb_derive::DivAssign)),
            Self::Eq => Some(quote!(Eq)),
            Self::FieldValues => Some(quote!(::icydb::__reexports::icydb_derive::FieldValues)),
            Self::Hash => Some(quote!(Hash)),
            Self::Inner => Some(quote!(::icydb::__reexports::icydb_derive::Inner)),
            Self::Mul => Some(quote!(::icydb::__reexports::icydb_derive::Mul)),
            Self::MulAssign => Some(quote!(::icydb::__reexports::icydb_derive::MulAssign)),
            Self::Ord => Some(quote!(Ord)),
            Self::Rem => Some(quote!(::icydb::__reexports::icydb_derive::Rem)),
            Self::Serialize => Some(quote!(::serde::Serialize)),
            Self::Sub => Some(quote!(::icydb::__reexports::icydb_derive::Sub)),
            Self::SubAssign => Some(quote!(::icydb::__reexports::icydb_derive::SubAssign)),
            Self::Sum => Some(quote!(::icydb::__reexports::icydb_derive::Sum)),

            _ => None,
        }
    }

    pub(crate) fn derive_attribute(self) -> Option<TokenStream> {
        match self {
            Self::Sorted => Some(quote!(#[::icydb::__reexports::remain::sorted])),
            Self::Default => Some(quote!(#[serde(default)])),
            _ => None,
        }
    }
}

impl FromMeta for TraitKind {
    fn from_nested_meta(item: &NestedMeta) -> Result<Self, DarlingError> {
        match item {
            NestedMeta::Meta(syn::Meta::Path(path)) => {
                let path_str = path_to_string(path);

                Self::from_str(&path_str).map_err(DarlingError::custom)
            }

            _ => Err(DarlingError::custom(format!(
                "expected Meta Path, got {item:?}"
            ))),
        }
    }
}

impl ToTokens for TraitKind {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let trait_name = format_ident!("{}", self.to_string());

        // quote
        quote!(::icydb::traits::#trait_name).to_tokens(tokens);
    }
}

///
/// TraitSet
///

#[derive(Clone, Debug, Default, Deref, DerefMut, Eq, PartialEq)]
pub struct TraitSet(pub HashSet<TraitKind>);

impl TraitSet {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn add(&mut self, tr: TraitKind) {
        self.insert(tr);
    }

    pub(crate) fn extend<I: IntoIterator<Item = TraitKind>>(&mut self, traits: I) {
        self.0.extend(traits);
    }

    pub(crate) fn into_vec(self) -> Vec<TraitKind> {
        self.0.into_iter().collect()
    }
}

impl From<Vec<TraitKind>> for TraitSet {
    fn from(v: Vec<TraitKind>) -> Self {
        Self(v.into_iter().collect())
    }
}

impl FromIterator<TraitKind> for TraitSet {
    fn from_iter<I: IntoIterator<Item = TraitKind>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl ToTokens for TraitSet {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        if !self.0.is_empty() {
            let derive_paths = self.0.iter().filter_map(|tr| tr.derive_path());

            tokens.extend(quote! {
                #[derive(#(#derive_paths),*)]
            });
        }
    }
}

///
/// TraitBuilder
///
/// Collects trait additions/removals from schema attributes.
/// After parsing, it should be treated as immutable and resolved via `.build()`.
///

#[derive(Clone, Debug, Default, FromMeta)]
pub struct TraitBuilder {
    #[darling(default)]
    pub add: TraitListMeta,

    #[darling(default)]
    pub remove: TraitListMeta,
}

impl TraitBuilder {
    pub(crate) fn with_type_traits(&self) -> Self {
        let mut clone = self.clone();
        clone.add.extend(TYPE_TRAITS.to_vec());

        clone
    }

    pub(crate) fn validate(&self) -> Result<(), DarlingError> {
        let mut set = TraitSet::new();
        set.extend(DEFAULT_TRAITS.to_vec());

        for tr in self.add.iter() {
            if !set.insert(*tr) {
                return Err(DarlingError::custom(format!(
                    "adding duplicate trait '{tr}'"
                )));
            }
        }

        for tr in self.remove.iter() {
            if !set.remove(tr) {
                return Err(DarlingError::custom(format!(
                    "cannot remove trait {tr} from {set:?}"
                )));
            }
        }

        Ok(())
    }

    // build
    // generates the TraitList based on the defaults plus traits that have been added or removed
    pub(crate) fn build(&self) -> TraitSet {
        let mut set = TraitSet::new();

        // always set defaults
        set.extend(DEFAULT_TRAITS.to_vec());

        // self.add
        for tr in self.add.iter() {
            assert!(set.insert(*tr), "adding duplicate trait '{tr}'");
        }

        // self.remove
        for tr in self.remove.iter() {
            assert!(set.remove(tr), "cannot remove trait {tr} from {set:?}",);
        }

        set
    }
}

///
/// TraitListMeta
/// Used only for parsing trait lists from schema attributes via darling.
///

#[derive(Clone, Debug, Default, Deref, DerefMut, IntoIterator)]
pub struct TraitListMeta(pub Vec<TraitKind>);

impl FromMeta for TraitListMeta {
    fn from_list(items: &[NestedMeta]) -> Result<Self, DarlingError> {
        let mut traits = Self::default();

        for item in items {
            let tr = TraitKind::from_nested_meta(item)?;
            traits.push(tr);
        }

        Ok(traits)
    }
}
