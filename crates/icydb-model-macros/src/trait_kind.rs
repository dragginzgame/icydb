//! Module: trait_kind
//! Responsibility: generated application-trait classification.
//! Does not own: runtime schema semantics.
//! Boundary: macro input to generated tokens.

use crate::prelude::*;
use darling::{Error as DarlingError, FromMeta, ast::NestedMeta};
use std::{collections::BTreeSet, str::FromStr};

//
// TraitKind
//

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TraitKind {
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
    Neg,
    Product,
    Rem,
    RemAssign,
    Sub,
    SubAssign,
    Sum,

    // application model
    From,
    FromIterator,
    Inner,
    IntoIterator,
    NumericValue,
    Path,
    NormalizeAuto,
    NormalizeCustom,
    ValidateAuto,
    ValidateCustom,
    Visitable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationTypeKind {
    Entity,
    Enum,
    List,
    Map,
    Newtype,
    Record,
    Set,
    Tuple,
}

impl ApplicationTypeKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Entity => "entity",
            Self::Enum => "enum",
            Self::List => "list",
            Self::Map => "map",
            Self::Newtype => "newtype",
            Self::Record => "record",
            Self::Set => "set",
            Self::Tuple => "tuple",
        }
    }

    fn configurable_trait_policy(
        self,
        trait_kind: TraitKind,
        baseline: &TraitSet,
    ) -> Option<ConfigurableTraitPolicy> {
        match trait_kind {
            TraitKind::Copy | TraitKind::Hash | TraitKind::Ord | TraitKind::PartialOrd => Some(
                ConfigurableTraitPolicy::from_baseline(baseline.contains(trait_kind)),
            ),
            TraitKind::Default => Some(if matches!(self, Self::List | Self::Map | Self::Set) {
                ConfigurableTraitPolicy::GeneratedOverrideable
            } else {
                ConfigurableTraitPolicy::OptIn
            }),
            TraitKind::Deref | TraitKind::DerefMut => {
                Some(if matches!(self, Self::List | Self::Map | Self::Set) {
                    ConfigurableTraitPolicy::GeneratedOverrideable
                } else if matches!(self, Self::Newtype) {
                    ConfigurableTraitPolicy::OptIn
                } else {
                    ConfigurableTraitPolicy::Unsupported
                })
            }
            TraitKind::Display => Some(if matches!(self, Self::Newtype) {
                ConfigurableTraitPolicy::OptIn
            } else {
                ConfigurableTraitPolicy::Unsupported
            }),
            TraitKind::Add
            | TraitKind::AddAssign
            | TraitKind::Div
            | TraitKind::DivAssign
            | TraitKind::Mul
            | TraitKind::MulAssign
            | TraitKind::Neg
            | TraitKind::Product
            | TraitKind::Rem
            | TraitKind::RemAssign
            | TraitKind::Sub
            | TraitKind::SubAssign
            | TraitKind::Sum => Some(if matches!(self, Self::Newtype) {
                ConfigurableTraitPolicy::from_baseline(baseline.contains(trait_kind))
            } else {
                ConfigurableTraitPolicy::Unsupported
            }),
            TraitKind::From => Some(
                if matches!(self, Self::List | Self::Map | Self::Newtype | Self::Set) {
                    ConfigurableTraitPolicy::GeneratedOverrideable
                } else {
                    ConfigurableTraitPolicy::Unsupported
                },
            ),
            TraitKind::FromIterator | TraitKind::IntoIterator => {
                Some(if matches!(self, Self::List | Self::Map | Self::Set) {
                    ConfigurableTraitPolicy::GeneratedOverrideable
                } else {
                    ConfigurableTraitPolicy::Unsupported
                })
            }
            TraitKind::Inner => Some(if matches!(self, Self::Newtype) {
                ConfigurableTraitPolicy::GeneratedOverrideable
            } else {
                ConfigurableTraitPolicy::Unsupported
            }),
            TraitKind::NormalizeCustom | TraitKind::ValidateCustom => {
                Some(ConfigurableTraitPolicy::GeneratedOverrideable)
            }
            TraitKind::NumericValue => Some(if !matches!(self, Self::Newtype) {
                ConfigurableTraitPolicy::Unsupported
            } else if baseline.contains(TraitKind::NumericValue) {
                ConfigurableTraitPolicy::GeneratedOverrideable
            } else {
                ConfigurableTraitPolicy::OptIn
            }),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConfigurableTraitPolicy {
    GeneratedOverrideable,
    OptIn,
    Unsupported,
}

impl ConfigurableTraitPolicy {
    const fn from_baseline(generated: bool) -> Self {
        if generated {
            Self::GeneratedOverrideable
        } else {
            Self::OptIn
        }
    }
}

impl FromStr for TraitKind {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
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
            "Neg" => Ok(Self::Neg),
            "Product" => Ok(Self::Product),
            "Rem" => Ok(Self::Rem),
            "RemAssign" => Ok(Self::RemAssign),
            "Sub" => Ok(Self::Sub),
            "SubAssign" => Ok(Self::SubAssign),
            "Sum" => Ok(Self::Sum),
            "From" => Ok(Self::From),
            "FromIterator" => Ok(Self::FromIterator),
            "Inner" => Ok(Self::Inner),
            "IntoIterator" => Ok(Self::IntoIterator),
            "NumericValue" => Ok(Self::NumericValue),
            "Path" => Ok(Self::Path),
            "NormalizeAuto" => Ok(Self::NormalizeAuto),
            "NormalizeCustom" => Ok(Self::NormalizeCustom),
            "ValidateAuto" => Ok(Self::ValidateAuto),
            "ValidateCustom" => Ok(Self::ValidateCustom),
            "Visitable" => Ok(Self::Visitable),
            _ => Err("unknown TraitKind"),
        }
    }
}

const GENERATED_NODE_TRAITS: &[TraitKind] = &[TraitKind::Clone, TraitKind::Debug, TraitKind::Path];

const DEFAULT_CONFIGURABLE_TYPE_TRAITS: &[TraitKind] =
    &[TraitKind::NormalizeCustom, TraitKind::ValidateCustom];

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

const CONFIGURABLE_TYPE_TRAITS: &[TraitKind] = &[
    TraitKind::Copy,
    TraitKind::Default,
    TraitKind::Deref,
    TraitKind::DerefMut,
    TraitKind::Display,
    TraitKind::Hash,
    TraitKind::Ord,
    TraitKind::PartialOrd,
    TraitKind::Add,
    TraitKind::AddAssign,
    TraitKind::Div,
    TraitKind::DivAssign,
    TraitKind::Mul,
    TraitKind::MulAssign,
    TraitKind::Neg,
    TraitKind::Product,
    TraitKind::Rem,
    TraitKind::RemAssign,
    TraitKind::Sub,
    TraitKind::SubAssign,
    TraitKind::Sum,
    TraitKind::From,
    TraitKind::FromIterator,
    TraitKind::Inner,
    TraitKind::IntoIterator,
    TraitKind::NumericValue,
    TraitKind::NormalizeCustom,
    TraitKind::ValidateCustom,
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
            Self::Neg => Some(quote!(::icydb_model::__reexports::icydb_model_macros::Neg)),
            Self::Ord => Some(quote!(Ord)),
            Self::PartialEq => Some(quote!(PartialEq)),
            Self::PartialOrd => Some(quote!(PartialOrd)),
            Self::Product => Some(quote!(
                ::icydb_model::__reexports::icydb_model_macros::Product
            )),
            Self::Rem => Some(quote!(::icydb_model::__reexports::icydb_model_macros::Rem)),
            Self::RemAssign => Some(quote!(
                ::icydb_model::__reexports::icydb_model_macros::RemAssign
            )),
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
            | Self::Neg
            | Self::Rem
            | Self::RemAssign
            | Self::Sub
            | Self::SubAssign => {
                let trait_name = format_ident!("{self:?}");
                quote!(::std::ops::#trait_name).to_tokens(tokens);
            }
            Self::Display => quote!(::std::fmt::Display).to_tokens(tokens),
            Self::From => quote!(::std::convert::From).to_tokens(tokens),
            Self::FromIterator => quote!(::std::iter::FromIterator).to_tokens(tokens),
            Self::Hash => quote!(::std::hash::Hash).to_tokens(tokens),
            Self::Inner | Self::Path => {
                let trait_name = format_ident!("{self:?}");
                quote!(::icydb_model::#trait_name).to_tokens(tokens);
            }
            Self::IntoIterator => quote!(::std::iter::IntoIterator).to_tokens(tokens),
            Self::NumericValue => {
                quote!(::icydb_model::schema::NumericValue).to_tokens(tokens);
            }
            Self::Product => quote!(::std::iter::Product).to_tokens(tokens),
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
        }
    }
}

//
// TraitSet
//

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TraitSet(BTreeSet<TraitKind>);

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

    pub(crate) fn contains(&self, tr: TraitKind) -> bool {
        self.0.contains(&tr)
    }

    pub(crate) fn extend<I: IntoIterator<Item = TraitKind>>(&mut self, traits: I) {
        self.0.extend(traits);
    }

    pub(crate) fn into_vec(self) -> Vec<TraitKind> {
        self.0.into_iter().collect()
    }
}

impl FromIterator<TraitKind> for TraitSet {
    fn from_iter<I: IntoIterator<Item = TraitKind>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

//
// TraitBuilder
//
// Collects trait additions/removals from schema attributes.
// After parsing, it is immutable and resolves only against a complete node baseline.
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
    pub(crate) fn validate_for_type(
        &self,
        node_kind: ApplicationTypeKind,
        baseline: TraitSet,
    ) -> Result<(), DarlingError> {
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

        Self::validate_configurable_baseline(node_kind, &baseline)?;
        self.validate_configurable_traits(node_kind, &baseline)?;

        self.resolve(baseline).map(|_| ())
    }

    fn validate_configurable_baseline(
        node_kind: ApplicationTypeKind,
        baseline: &TraitSet,
    ) -> Result<(), DarlingError> {
        for trait_kind in CONFIGURABLE_TYPE_TRAITS {
            let policy = node_kind
                .configurable_trait_policy(*trait_kind, baseline)
                .ok_or_else(|| missing_trait_policy_error(node_kind, *trait_kind))?;
            let generated = baseline.contains(*trait_kind);
            let baseline_is_valid = matches!(
                (policy, generated),
                (ConfigurableTraitPolicy::GeneratedOverrideable, true)
                    | (
                        ConfigurableTraitPolicy::OptIn | ConfigurableTraitPolicy::Unsupported,
                        false
                    )
            );
            if !baseline_is_valid {
                return Err(DarlingError::custom(format!(
                    "internal {} trait baseline disagrees with the '{trait_kind:?}' policy",
                    node_kind.as_str(),
                )));
            }
        }

        Ok(())
    }

    fn validate_configurable_traits(
        &self,
        node_kind: ApplicationTypeKind,
        baseline: &TraitSet,
    ) -> Result<(), DarlingError> {
        for trait_kind in self.add.iter() {
            if self.remove.iter().any(|removed| removed == trait_kind) {
                return Err(DarlingError::custom(format!(
                    "trait '{trait_kind:?}' cannot appear in both traits(add(...)) and traits(remove(...)) for {} application values",
                    node_kind.as_str(),
                )));
            }

            match node_kind.configurable_trait_policy(*trait_kind, baseline) {
                Some(ConfigurableTraitPolicy::GeneratedOverrideable) => {
                    return Err(DarlingError::custom(format!(
                        "trait '{trait_kind:?}' is generated automatically for {} application values and cannot be added explicitly",
                        node_kind.as_str(),
                    )));
                }
                Some(ConfigurableTraitPolicy::Unsupported) => {
                    return Err(unsupported_trait_error(node_kind, *trait_kind));
                }
                Some(ConfigurableTraitPolicy::OptIn) => {}
                None => return Err(missing_trait_policy_error(node_kind, *trait_kind)),
            }
        }

        for trait_kind in self.remove.iter() {
            match node_kind.configurable_trait_policy(*trait_kind, baseline) {
                Some(ConfigurableTraitPolicy::OptIn) => {
                    return Err(DarlingError::custom(format!(
                        "trait '{trait_kind:?}' is not generated for {} application values and cannot be removed",
                        node_kind.as_str(),
                    )));
                }
                Some(ConfigurableTraitPolicy::Unsupported) => {
                    return Err(unsupported_trait_error(node_kind, *trait_kind));
                }
                Some(ConfigurableTraitPolicy::GeneratedOverrideable) => {}
                None => return Err(missing_trait_policy_error(node_kind, *trait_kind)),
            }
        }

        Ok(())
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
    pub(crate) fn build_for_type(&self, baseline: TraitSet) -> TraitSet {
        self.resolve(baseline)
            .unwrap_or_else(|_| panic!("validated application type traits must resolve"))
    }
}

fn unsupported_trait_error(node_kind: ApplicationTypeKind, trait_kind: TraitKind) -> DarlingError {
    DarlingError::custom(format!(
        "trait '{trait_kind:?}' is not supported by {} application values",
        node_kind.as_str(),
    ))
}

fn missing_trait_policy_error(
    node_kind: ApplicationTypeKind,
    trait_kind: TraitKind,
) -> DarlingError {
    DarlingError::custom(format!(
        "internal trait '{trait_kind:?}' has no policy for {} application values",
        node_kind.as_str(),
    ))
}

pub fn generated_node_trait_set() -> TraitSet {
    GENERATED_NODE_TRAITS.iter().copied().collect()
}

pub fn application_type_trait_set() -> TraitSet {
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

#[derive(Clone, Debug, Default)]
pub struct TraitListMeta(Vec<TraitKind>);

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
        let traits = TraitBuilder::default().build_for_type(application_type_trait_set());

        for required in REQUIRED_TYPE_TRAITS {
            assert!(traits.contains(*required));
        }
    }

    #[test]
    fn required_type_traits_reject_explicit_addition_and_removal() {
        for required in REQUIRED_TYPE_TRAITS.iter().copied() {
            let adds_required = TraitBuilder {
                add: TraitListMeta(vec![required]),
                remove: TraitListMeta::default(),
            };
            assert!(
                adds_required
                    .validate_for_type(ApplicationTypeKind::Entity, application_type_trait_set(),)
                    .is_err()
            );

            let removes_required = TraitBuilder {
                add: TraitListMeta::default(),
                remove: TraitListMeta(vec![required]),
            };
            assert!(
                removes_required
                    .validate_for_type(ApplicationTypeKind::Entity, application_type_trait_set(),)
                    .is_err()
            );
        }
    }

    #[test]
    fn optional_and_override_type_traits_remain_configurable() {
        for configurable in [TraitKind::NormalizeCustom, TraitKind::ValidateCustom] {
            let builder = TraitBuilder {
                add: TraitListMeta::default(),
                remove: TraitListMeta(vec![configurable]),
            };
            assert!(
                builder
                    .validate_for_type(ApplicationTypeKind::Entity, application_type_trait_set(),)
                    .is_ok()
            );
            assert!(
                !builder
                    .build_for_type(application_type_trait_set())
                    .0
                    .contains(&configurable)
            );
        }

        let default = TraitBuilder {
            add: TraitListMeta(vec![TraitKind::Default]),
            remove: TraitListMeta::default(),
        };
        assert!(
            default
                .validate_for_type(ApplicationTypeKind::Entity, application_type_trait_set(),)
                .is_ok()
        );
        assert!(
            default
                .build_for_type(application_type_trait_set())
                .0
                .contains(&TraitKind::Default)
        );
    }

    #[test]
    fn shape_baseline_is_applied_before_author_directives() {
        let mut baseline = application_type_trait_set();
        baseline.extend([
            TraitKind::Default,
            TraitKind::Deref,
            TraitKind::DerefMut,
            TraitKind::From,
            TraitKind::FromIterator,
            TraitKind::IntoIterator,
        ]);

        let remove_deref = TraitBuilder {
            add: TraitListMeta::default(),
            remove: TraitListMeta(vec![TraitKind::Deref]),
        };
        assert!(
            remove_deref
                .validate_for_type(ApplicationTypeKind::List, baseline.clone())
                .is_ok()
        );
        assert!(
            !remove_deref
                .build_for_type(baseline.clone())
                .0
                .contains(&TraitKind::Deref)
        );

        let add_default = TraitBuilder {
            add: TraitListMeta(vec![TraitKind::Default]),
            remove: TraitListMeta::default(),
        };
        assert!(
            add_default
                .validate_for_type(ApplicationTypeKind::List, baseline)
                .is_err()
        );
    }

    #[test]
    fn generated_custom_policy_is_node_specific() {
        let baseline = application_type_trait_set();
        let application_nodes = [
            ApplicationTypeKind::Entity,
            ApplicationTypeKind::Enum,
            ApplicationTypeKind::List,
            ApplicationTypeKind::Map,
            ApplicationTypeKind::Newtype,
            ApplicationTypeKind::Record,
            ApplicationTypeKind::Set,
            ApplicationTypeKind::Tuple,
        ];

        for node_kind in application_nodes {
            for trait_kind in [TraitKind::NormalizeCustom, TraitKind::ValidateCustom] {
                assert_eq!(
                    node_kind.configurable_trait_policy(trait_kind, &baseline),
                    Some(ConfigurableTraitPolicy::GeneratedOverrideable),
                );
            }

            let wrapper_policy = if matches!(
                node_kind,
                ApplicationTypeKind::List
                    | ApplicationTypeKind::Map
                    | ApplicationTypeKind::Newtype
                    | ApplicationTypeKind::Set
            ) {
                ConfigurableTraitPolicy::GeneratedOverrideable
            } else {
                ConfigurableTraitPolicy::Unsupported
            };
            assert_eq!(
                node_kind.configurable_trait_policy(TraitKind::From, &baseline),
                Some(wrapper_policy),
            );

            let newtype_policy = if matches!(node_kind, ApplicationTypeKind::Newtype) {
                ConfigurableTraitPolicy::GeneratedOverrideable
            } else {
                ConfigurableTraitPolicy::Unsupported
            };
            assert_eq!(
                node_kind.configurable_trait_policy(TraitKind::Inner, &baseline),
                Some(newtype_policy),
            );

            let numeric_policy = if matches!(node_kind, ApplicationTypeKind::Newtype) {
                ConfigurableTraitPolicy::OptIn
            } else {
                ConfigurableTraitPolicy::Unsupported
            };
            assert_eq!(
                node_kind.configurable_trait_policy(TraitKind::NumericValue, &baseline),
                Some(numeric_policy),
            );
        }

        let mut numeric_baseline = baseline;
        numeric_baseline.add(TraitKind::NumericValue);
        assert_eq!(
            ApplicationTypeKind::Newtype
                .configurable_trait_policy(TraitKind::NumericValue, &numeric_baseline),
            Some(ConfigurableTraitPolicy::GeneratedOverrideable),
        );
    }

    #[test]
    fn configurable_policy_covers_every_non_required_trait() {
        let baseline = application_type_trait_set();
        let application_nodes = [
            ApplicationTypeKind::Entity,
            ApplicationTypeKind::Enum,
            ApplicationTypeKind::List,
            ApplicationTypeKind::Map,
            ApplicationTypeKind::Newtype,
            ApplicationTypeKind::Record,
            ApplicationTypeKind::Set,
            ApplicationTypeKind::Tuple,
        ];

        let complete_inventory: BTreeSet<_> = REQUIRED_TYPE_TRAITS
            .iter()
            .chain(CONFIGURABLE_TYPE_TRAITS.iter())
            .copied()
            .collect();
        assert_eq!(REQUIRED_TYPE_TRAITS.len(), 10);
        assert_eq!(CONFIGURABLE_TYPE_TRAITS.len(), 28);
        assert_eq!(complete_inventory.len(), 38);

        for node_kind in application_nodes {
            for trait_kind in CONFIGURABLE_TYPE_TRAITS {
                assert!(
                    node_kind
                        .configurable_trait_policy(*trait_kind, &baseline)
                        .is_some(),
                    "missing {node_kind:?}/{trait_kind:?} policy",
                );
            }
            for trait_kind in REQUIRED_TYPE_TRAITS {
                assert!(
                    node_kind
                        .configurable_trait_policy(*trait_kind, &baseline)
                        .is_none(),
                    "required {node_kind:?}/{trait_kind:?} entered configurable policy",
                );
            }
        }
    }

    #[test]
    fn fixed_generated_nodes_do_not_enter_application_trait_resolution() {
        assert_eq!(
            generated_node_trait_set().into_vec(),
            vec![TraitKind::Clone, TraitKind::Debug, TraitKind::Path],
        );
    }

    #[test]
    fn derive_and_helper_policies_follow_node_and_baseline() {
        let base = application_type_trait_set();

        for collection in [
            ApplicationTypeKind::List,
            ApplicationTypeKind::Map,
            ApplicationTypeKind::Set,
        ] {
            assert_eq!(
                collection.configurable_trait_policy(TraitKind::Default, &base),
                Some(ConfigurableTraitPolicy::GeneratedOverrideable),
            );
        }
        for opt_in in [
            ApplicationTypeKind::Entity,
            ApplicationTypeKind::Enum,
            ApplicationTypeKind::Newtype,
            ApplicationTypeKind::Record,
            ApplicationTypeKind::Tuple,
        ] {
            assert_eq!(
                opt_in.configurable_trait_policy(TraitKind::Default, &base),
                Some(ConfigurableTraitPolicy::OptIn),
            );
        }

        for standard in [
            TraitKind::Copy,
            TraitKind::Hash,
            TraitKind::Ord,
            TraitKind::PartialOrd,
        ] {
            assert_eq!(
                ApplicationTypeKind::Record.configurable_trait_policy(standard, &base),
                Some(ConfigurableTraitPolicy::OptIn),
            );
            let mut generated = base.clone();
            generated.add(standard);
            assert_eq!(
                ApplicationTypeKind::Enum.configurable_trait_policy(standard, &generated),
                Some(ConfigurableTraitPolicy::GeneratedOverrideable),
            );
        }

        for dereference in [TraitKind::Deref, TraitKind::DerefMut] {
            assert_eq!(
                ApplicationTypeKind::List.configurable_trait_policy(dereference, &base),
                Some(ConfigurableTraitPolicy::GeneratedOverrideable),
            );
            assert_eq!(
                ApplicationTypeKind::Newtype.configurable_trait_policy(dereference, &base),
                Some(ConfigurableTraitPolicy::OptIn),
            );
            assert_eq!(
                ApplicationTypeKind::Record.configurable_trait_policy(dereference, &base),
                Some(ConfigurableTraitPolicy::Unsupported),
            );
        }

        assert_eq!(
            ApplicationTypeKind::Newtype.configurable_trait_policy(TraitKind::Display, &base),
            Some(ConfigurableTraitPolicy::OptIn),
        );
        assert_eq!(
            ApplicationTypeKind::Entity.configurable_trait_policy(TraitKind::Display, &base),
            Some(ConfigurableTraitPolicy::Unsupported),
        );

        for arithmetic in [
            TraitKind::Add,
            TraitKind::AddAssign,
            TraitKind::Div,
            TraitKind::DivAssign,
            TraitKind::Mul,
            TraitKind::MulAssign,
            TraitKind::Neg,
            TraitKind::Product,
            TraitKind::Rem,
            TraitKind::RemAssign,
            TraitKind::Sub,
            TraitKind::SubAssign,
            TraitKind::Sum,
        ] {
            assert_eq!(
                ApplicationTypeKind::Newtype.configurable_trait_policy(arithmetic, &base),
                Some(ConfigurableTraitPolicy::OptIn),
            );
            let mut generated = base.clone();
            generated.add(arithmetic);
            assert_eq!(
                ApplicationTypeKind::Newtype.configurable_trait_policy(arithmetic, &generated),
                Some(ConfigurableTraitPolicy::GeneratedOverrideable),
            );
            assert_eq!(
                ApplicationTypeKind::Tuple.configurable_trait_policy(arithmetic, &base),
                Some(ConfigurableTraitPolicy::Unsupported),
            );
        }
    }

    #[test]
    fn collection_protocols_are_generated_only_for_collection_nodes() {
        let base = application_type_trait_set();
        let protocols = [TraitKind::FromIterator, TraitKind::IntoIterator];

        for collection in [
            ApplicationTypeKind::List,
            ApplicationTypeKind::Map,
            ApplicationTypeKind::Set,
        ] {
            let mut baseline = base.clone();
            baseline.extend(protocols);
            for protocol in protocols {
                assert_eq!(
                    collection.configurable_trait_policy(protocol, &baseline),
                    Some(ConfigurableTraitPolicy::GeneratedOverrideable),
                );
            }
        }

        for other in [
            ApplicationTypeKind::Entity,
            ApplicationTypeKind::Enum,
            ApplicationTypeKind::Newtype,
            ApplicationTypeKind::Record,
            ApplicationTypeKind::Tuple,
        ] {
            for protocol in protocols {
                assert_eq!(
                    other.configurable_trait_policy(protocol, &base),
                    Some(ConfigurableTraitPolicy::Unsupported),
                );
            }
        }
    }

    #[test]
    fn generated_custom_directives_follow_the_closed_policy() {
        let mut newtype_baseline = application_type_trait_set();
        newtype_baseline.extend([TraitKind::From, TraitKind::Inner]);

        let remove_generated = TraitBuilder {
            add: TraitListMeta::default(),
            remove: TraitListMeta(vec![
                TraitKind::From,
                TraitKind::Inner,
                TraitKind::NormalizeCustom,
                TraitKind::ValidateCustom,
            ]),
        };
        assert!(
            remove_generated
                .validate_for_type(ApplicationTypeKind::Newtype, newtype_baseline.clone())
                .is_ok()
        );

        let add_opt_in_numeric = TraitBuilder {
            add: TraitListMeta(vec![TraitKind::NumericValue]),
            remove: TraitListMeta::default(),
        };
        assert!(
            add_opt_in_numeric
                .validate_for_type(ApplicationTypeKind::Newtype, newtype_baseline.clone())
                .is_ok()
        );

        let remove_opt_in_numeric = TraitBuilder {
            add: TraitListMeta::default(),
            remove: TraitListMeta(vec![TraitKind::NumericValue]),
        };
        let error = remove_opt_in_numeric
            .validate_for_type(ApplicationTypeKind::Newtype, newtype_baseline)
            .expect_err("an absent opt-in cannot be removed");
        assert!(error.to_string().contains("is not generated for newtype"));

        let add_unsupported_inner = TraitBuilder {
            add: TraitListMeta(vec![TraitKind::Inner]),
            remove: TraitListMeta::default(),
        };
        let error = add_unsupported_inner
            .validate_for_type(ApplicationTypeKind::Record, application_type_trait_set())
            .expect_err("record Inner should be unsupported");
        assert!(error.to_string().contains("is not supported by record"));

        let conflicting_numeric = TraitBuilder {
            add: TraitListMeta(vec![TraitKind::NumericValue]),
            remove: TraitListMeta(vec![TraitKind::NumericValue]),
        };
        let mut newtype_baseline = application_type_trait_set();
        newtype_baseline.extend([TraitKind::From, TraitKind::Inner]);
        let error = conflicting_numeric
            .validate_for_type(ApplicationTypeKind::Newtype, newtype_baseline)
            .expect_err("one trait cannot be added and removed together");
        assert!(error.to_string().contains("cannot appear in both"));
    }
}
