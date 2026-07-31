//! Module: trait_kind
//! Responsibility: generated application-trait classification.
//! Does not own: runtime schema semantics.
//! Boundary: macro input to generated tokens.

use crate::prelude::*;
use darling::{Error as DarlingError, FromMeta, ast::NestedMeta};
use derive_more::IntoIterator;
use std::{collections::HashSet, hash::Hash, str::FromStr};

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

const DEFAULT_TRAITS: &[TraitKind] = &[TraitKind::Clone, TraitKind::Debug, TraitKind::Path];

const DEFAULT_CONFIGURABLE_TYPE_TRAITS: &[TraitKind] = &[
    TraitKind::From,
    TraitKind::NormalizeCustom,
    TraitKind::ValidateCustom,
];

const REQUIRED_TYPE_TRAITS: &[TraitKind] = &[
    TraitKind::CandidType,
    TraitKind::Clone,
    TraitKind::Debug,
    TraitKind::Deserialize,
    TraitKind::Eq,
    TraitKind::PartialEq,
    TraitKind::Path,
    TraitKind::NormalizeAuto,
    TraitKind::ValidateAuto,
    TraitKind::Visitable,
];

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
            Self::Deref => Some(quote!(
                ::icydb_model::__reexports::icydb_model_macros::Deref
            )),
            Self::DerefMut => Some(quote!(
                ::icydb_model::__reexports::icydb_model_macros::DerefMut
            )),
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

    /// Validate author directives for a generated application value type.
    ///
    /// Required traits are compiler-owned output rather than configurable
    /// defaults. Optional and override traits continue through the maintained
    /// generic add/remove validation.
    pub(crate) fn validate_for_type(&self) -> Result<(), DarlingError> {
        for tr in self.add.iter() {
            if REQUIRED_TYPE_TRAITS.contains(tr) {
                return Err(DarlingError::custom(format!(
                    "required trait '{tr:?}' is generated automatically and cannot be added explicitly"
                )));
            }
        }

        for tr in self.remove.iter() {
            if REQUIRED_TYPE_TRAITS.contains(tr) {
                return Err(DarlingError::custom(format!(
                    "required trait '{tr:?}' is generated automatically and cannot be removed"
                )));
            }
        }

        self.resolve(type_trait_set()).map(|_| ())
    }

    fn resolve(&self, mut set: TraitSet) -> Result<TraitSet, DarlingError> {
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

        Ok(set)
    }

    /// Resolve the required and default traits for an application value type.
    ///
    /// This is called only after [`Self::validate_for_type`] succeeds.
    pub(crate) fn build_for_type(&self) -> TraitSet {
        self.resolve(type_trait_set())
            .unwrap_or_else(|_| panic!("validated application type traits must resolve"))
    }

    // Generates the TraitList based on the defaults plus traits that have been
    // added or removed. This is called only after validation succeeds.
    pub(crate) fn build(&self) -> TraitSet {
        self.resolve(default_trait_set())
            .unwrap_or_else(|_| panic!("validated generated traits must resolve"))
    }
}

fn default_trait_set() -> TraitSet {
    DEFAULT_TRAITS.iter().copied().collect()
}

fn type_trait_set() -> TraitSet {
    REQUIRED_TYPE_TRAITS
        .iter()
        .chain(DEFAULT_CONFIGURABLE_TYPE_TRAITS.iter())
        .copied()
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_type_traits_are_emitted_without_author_directives() {
        let traits = TraitBuilder::default().build_for_type();

        for required in REQUIRED_TYPE_TRAITS {
            assert!(traits.0.contains(required));
        }
    }

    #[test]
    fn required_type_traits_reject_explicit_addition_and_removal() {
        for required in REQUIRED_TYPE_TRAITS.iter().copied() {
            let adds_required = TraitBuilder {
                add: TraitListMeta(vec![required]),
                remove: TraitListMeta::default(),
            };
            assert!(adds_required.validate_for_type().is_err());

            let removes_required = TraitBuilder {
                add: TraitListMeta::default(),
                remove: TraitListMeta(vec![required]),
            };
            assert!(removes_required.validate_for_type().is_err());
        }
    }

    #[test]
    fn optional_and_override_type_traits_remain_configurable() {
        for configurable in [
            TraitKind::From,
            TraitKind::NormalizeCustom,
            TraitKind::ValidateCustom,
        ] {
            let builder = TraitBuilder {
                add: TraitListMeta::default(),
                remove: TraitListMeta(vec![configurable]),
            };
            assert!(builder.validate_for_type().is_ok());
            assert!(!builder.build_for_type().0.contains(&configurable));
        }

        let default = TraitBuilder {
            add: TraitListMeta(vec![TraitKind::Default]),
            remove: TraitListMeta::default(),
        };
        assert!(default.validate_for_type().is_ok());
        assert!(default.build_for_type().0.contains(&TraitKind::Default));
    }
}
