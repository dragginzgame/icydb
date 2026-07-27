//! Module: trait_kind
//! Responsibility: generated application-trait classification.
//! Does not own: runtime schema semantics.
//! Boundary: macro input to generated tokens.

use crate::prelude::*;
use darling::{Error as DarlingError, FromMeta, ast::NestedMeta};
use derive_more::IntoIterator;
use std::{collections::HashSet, hash::Hash, str::FromStr, sync::LazyLock};

//
// TraitKind
//

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
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
    Deref,
    DerefMut,
    Display,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,

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

    // application model
    Collection,
    From,
    Inner,
    MapCollection,
    NumericValue,
    Path,
    Sorted,
    NormalizeAuto,
    NormalizeCustom,
    ValidateAuto,
    ValidateCustom,
    Visitable,
}

impl FromStr for TraitKind {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Inherent" => Ok(Self::Inherent),
            "CandidType" => Ok(Self::CandidType),
            "Clone" => Ok(Self::Clone),
            "Copy" => Ok(Self::Copy),
            "Debug" => Ok(Self::Debug),
            "Default" => Ok(Self::Default),
            "Deserialize" => Ok(Self::Deserialize),
            "Deref" => Ok(Self::Deref),
            "DerefMut" => Ok(Self::DerefMut),
            "Display" => Ok(Self::Display),
            "Eq" => Ok(Self::Eq),
            "Hash" => Ok(Self::Hash),
            "Ord" => Ok(Self::Ord),
            "PartialEq" => Ok(Self::PartialEq),
            "PartialOrd" => Ok(Self::PartialOrd),
            "Add" => Ok(Self::Add),
            "AddAssign" => Ok(Self::AddAssign),
            "Div" => Ok(Self::Div),
            "DivAssign" => Ok(Self::DivAssign),
            "Mul" => Ok(Self::Mul),
            "MulAssign" => Ok(Self::MulAssign),
            "Rem" => Ok(Self::Rem),
            "Sub" => Ok(Self::Sub),
            "SubAssign" => Ok(Self::SubAssign),
            "Sum" => Ok(Self::Sum),
            "Collection" => Ok(Self::Collection),
            "From" => Ok(Self::From),
            "Inner" => Ok(Self::Inner),
            "MapCollection" => Ok(Self::MapCollection),
            "NumericValue" => Ok(Self::NumericValue),
            "Path" => Ok(Self::Path),
            "Sorted" => Ok(Self::Sorted),
            "NormalizeAuto" => Ok(Self::NormalizeAuto),
            "NormalizeCustom" => Ok(Self::NormalizeCustom),
            "ValidateAuto" => Ok(Self::ValidateAuto),
            "ValidateCustom" => Ok(Self::ValidateCustom),
            "Visitable" => Ok(Self::Visitable),
            _ => Err("unknown TraitKind"),
        }
    }
}

static DEFAULT_TRAITS: LazyLock<Vec<TraitKind>> =
    LazyLock::new(|| vec![TraitKind::Clone, TraitKind::Debug, TraitKind::Path]);

static TYPE_TRAITS: LazyLock<Vec<TraitKind>> = LazyLock::new(|| {
    vec![
        TraitKind::CandidType,
        TraitKind::Deserialize,
        TraitKind::Eq,
        TraitKind::From,
        TraitKind::PartialEq,
        TraitKind::NormalizeAuto,
        TraitKind::NormalizeCustom,
        TraitKind::ValidateAuto,
        TraitKind::ValidateCustom,
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
    /// NOTE: even if we have our own impl versions, the derives may still
    /// be used by other types (PartialEq for instance)
    #[must_use]
    #[remain::check]
    pub(crate) fn derive_path(self) -> Option<TokenStream> {
        #[remain::sorted]
        match self {
            Self::Add => Some(quote!(::icydb_model::__reexports::icydb_model_macros::Add)),
            Self::AddAssign => Some(quote!(
                ::icydb_model::__reexports::icydb_model_macros::AddAssign
            )),
            Self::CandidType => Some(quote!(::icydb_model::__reexports::candid::CandidType)),
            Self::Clone => Some(quote!(Clone)),
            Self::Copy => Some(quote!(Copy)),
            Self::Debug => Some(quote!(Debug)),
            Self::Default => Some(quote!(Default)),
            Self::Deref => Some(quote!(::icydb_model::__reexports::derive_more::Deref)),
            Self::DerefMut => Some(quote!(::icydb_model::__reexports::derive_more::DerefMut)),
            Self::Deserialize => Some(quote!(::icydb_model::__reexports::serde::Deserialize)),
            Self::Display => Some(quote!(
                ::icydb_model::__reexports::icydb_model_macros::Display
            )),
            Self::Div => Some(quote!(::icydb_model::__reexports::icydb_model_macros::Div)),
            Self::DivAssign => Some(quote!(
                ::icydb_model::__reexports::icydb_model_macros::DivAssign
            )),
            Self::Eq => Some(quote!(Eq)),
            Self::Hash => Some(quote!(Hash)),
            Self::Inner => Some(quote!(
                ::icydb_model::__reexports::icydb_model_macros::Inner
            )),
            Self::Mul => Some(quote!(::icydb_model::__reexports::icydb_model_macros::Mul)),
            Self::MulAssign => Some(quote!(
                ::icydb_model::__reexports::icydb_model_macros::MulAssign
            )),
            Self::Ord => Some(quote!(Ord)),
            Self::PartialEq => Some(quote!(PartialEq)),
            Self::PartialOrd => Some(quote!(PartialOrd)),
            Self::Rem => Some(quote!(::icydb_model::__reexports::icydb_model_macros::Rem)),
            Self::Sub => Some(quote!(::icydb_model::__reexports::icydb_model_macros::Sub)),
            Self::SubAssign => Some(quote!(
                ::icydb_model::__reexports::icydb_model_macros::SubAssign
            )),
            Self::Sum => Some(quote!(::icydb_model::__reexports::icydb_model_macros::Sum)),

            _ => {
                // NOTE: Not all TraitKind variants have derive paths.
                None
            }
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
        match self {
            Self::Clone => quote!(::core::clone::Clone).to_tokens(tokens),
            Self::Copy => quote!(::core::marker::Copy).to_tokens(tokens),
            Self::Debug => quote!(::core::fmt::Debug).to_tokens(tokens),
            Self::Default => quote!(::core::default::Default).to_tokens(tokens),
            Self::Eq => quote!(::core::cmp::Eq).to_tokens(tokens),
            Self::Ord => quote!(::core::cmp::Ord).to_tokens(tokens),
            Self::PartialEq => quote!(::core::cmp::PartialEq).to_tokens(tokens),
            Self::PartialOrd => quote!(::core::cmp::PartialOrd).to_tokens(tokens),
            Self::Add
            | Self::AddAssign
            | Self::Deref
            | Self::DerefMut
            | Self::Div
            | Self::DivAssign
            | Self::Mul
            | Self::MulAssign
            | Self::Rem
            | Self::Sub
            | Self::SubAssign => {
                let trait_name = format_ident!("{self:?}");
                quote!(::std::ops::#trait_name).to_tokens(tokens);
            }
            Self::Display => quote!(::std::fmt::Display).to_tokens(tokens),
            Self::From => quote!(::std::convert::From).to_tokens(tokens),
            Self::Hash => quote!(::std::hash::Hash).to_tokens(tokens),
            Self::Collection | Self::Inner | Self::MapCollection | Self::Path => {
                let trait_name = format_ident!("{self:?}");
                quote!(::icydb_model::#trait_name).to_tokens(tokens);
            }
            Self::NumericValue => {
                quote!(::icydb_model::schema::NumericValue).to_tokens(tokens);
            }
            Self::Sum => quote!(::std::iter::Sum).to_tokens(tokens),
            Self::CandidType => {
                quote!(::icydb_model::__reexports::candid::CandidType).to_tokens(tokens);
            }
            Self::Deserialize => {
                quote!(::icydb_model::__reexports::serde::Deserialize).to_tokens(tokens);
            }
            Self::NormalizeAuto
            | Self::NormalizeCustom
            | Self::ValidateAuto
            | Self::ValidateCustom
            | Self::Visitable => {
                let trait_name = format_ident!("{self:?}");
                quote!(::icydb_model::visitor::#trait_name).to_tokens(tokens);
            }
            Self::Inherent | Self::Sorted => {}
        }
    }
}

//
// TraitSet
//

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TraitSet(pub HashSet<TraitKind>);

impl TraitSet {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn add(&mut self, tr: TraitKind) {
        self.insert(tr);
    }

    pub(crate) fn insert(&mut self, tr: TraitKind) -> bool {
        self.0.insert(tr)
    }

    pub(crate) fn remove(&mut self, tr: TraitKind) -> bool {
        self.0.remove(&tr)
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

//
// TraitBuilder
//
// Collects trait additions/removals from schema attributes.
// After parsing, it should be treated as immutable and resolved via `.build()`.
//

#[derive(Clone, Debug, Default, FromMeta)]
pub struct TraitBuilder {
    #[darling(default)]
    pub(crate) add: TraitListMeta,

    #[darling(default)]
    pub(crate) remove: TraitListMeta,
}

impl TraitBuilder {
    pub(crate) fn explicitly_adds(&self, tr: TraitKind) -> bool {
        self.add.iter().any(|candidate| *candidate == tr)
    }

    pub(crate) fn with_type_traits(&self) -> Self {
        let mut clone = self.clone();
        clone.add.extend(TYPE_TRAITS.iter().copied());

        clone
    }

    pub(crate) fn validate(&self) -> Result<(), DarlingError> {
        let mut set = TraitSet::new();
        set.extend(DEFAULT_TRAITS.iter().copied());

        for tr in self.add.iter() {
            if !set.insert(*tr) {
                return Err(DarlingError::custom(format!(
                    "adding duplicate trait '{tr:?}'"
                )));
            }
        }

        for tr in self.remove.iter() {
            if !set.remove(*tr) {
                return Err(DarlingError::custom(format!(
                    "cannot remove trait '{tr:?}' because it is not enabled"
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
        set.extend(DEFAULT_TRAITS.iter().copied());

        // self.add
        for tr in self.add.iter() {
            assert!(set.insert(*tr), "adding duplicate trait '{tr:?}'");
        }

        // self.remove
        for tr in self.remove.iter() {
            assert!(set.remove(*tr), "cannot remove trait {tr:?} from {set:?}");
        }

        set
    }
}

//
// TraitListMeta
// Used only for parsing trait lists from schema attributes via darling.
//

#[derive(Clone, Debug, Default, IntoIterator)]
pub struct TraitListMeta(pub Vec<TraitKind>);

impl TraitListMeta {
    pub(crate) fn push(&mut self, tr: TraitKind) {
        self.0.push(tr);
    }

    pub(crate) fn extend<I: IntoIterator<Item = TraitKind>>(&mut self, traits: I) {
        self.0.extend(traits);
    }

    pub(crate) fn iter(&self) -> std::slice::Iter<'_, TraitKind> {
        self.0.iter()
    }
}

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
