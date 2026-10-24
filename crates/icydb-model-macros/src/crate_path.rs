//! Generated crate-path resolution.
//!
//! Macro output names package owners rather than assuming the dependency key
//! used by the consuming crate. Attribute macros may override the model path
//! explicitly; runtime paths and helper derives use Cargo package resolution.

use darling::ast::NestedMeta;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{Expr, Lit, Path};

const ICYDB_PACKAGE: &str = "icydb";
const MODEL_PACKAGE: &str = "icydb-model";

pub(crate) fn icydb_runtime_path() -> Result<TokenStream, darling::Error> {
    resolve_path(ICYDB_PACKAGE)
}

/// Optional dependency paths consumed before node-specific argument parsing.
#[derive(Default)]
pub(crate) struct CratePathOverrides {
    model: Option<Path>,
}

impl CratePathOverrides {
    /// Remove crate-path options from one attribute's node arguments.
    pub(crate) fn extract(args: &mut Vec<NestedMeta>) -> Result<Self, darling::Error> {
        let mut overrides = Self::default();
        let mut retained = Vec::with_capacity(args.len());

        for arg in args.drain(..) {
            let NestedMeta::Meta(syn::Meta::NameValue(name_value)) = &arg else {
                retained.push(arg);
                continue;
            };

            let target = if name_value.path.is_ident("model_crate") {
                &mut overrides.model
            } else {
                retained.push(arg);
                continue;
            };

            if target.is_some() {
                return Err(darling::Error::custom(format!(
                    "{} may be specified only once",
                    name_value.path.to_token_stream()
                ))
                .with_span(&name_value.path));
            }
            let Expr::Lit(literal) = &name_value.value else {
                return Err(darling::Error::custom(
                    "crate-path overrides require a quoted Rust path",
                )
                .with_span(&name_value.value));
            };
            let Lit::Str(path) = &literal.lit else {
                return Err(darling::Error::custom(
                    "crate-path overrides require a quoted Rust path",
                )
                .with_span(&literal.lit));
            };
            *target = Some(path.parse().map_err(|_| {
                darling::Error::custom("crate-path override is not a valid Rust path")
                    .with_span(path)
            })?);
        }

        *args = retained;
        Ok(overrides)
    }

    /// Return whether this invocation can emit runtime-owned entity adapters.
    ///
    /// Schema-only packages deliberately have no `icydb` dependency and keep
    /// producing model declarations without runtime code. Adding the runtime
    /// facade makes entity adapters automatic; it is not a schema option.
    pub(crate) fn has_icydb_runtime() -> bool {
        crate_name(ICYDB_PACKAGE).is_ok()
    }
}

/// Resolved dependencies passed explicitly to generated token construction.
#[derive(Debug)]
pub struct CratePaths {
    pub(crate) model: TokenStream,
    pub(crate) runtime: TokenStream,
}

impl CratePathOverrides {
    pub(crate) fn resolve(&self) -> Result<CratePaths, darling::Error> {
        Ok(CratePaths {
            model: resolve_model_path(self.model.as_ref())?,
            // Schema-only nodes never emit runtime references.
            runtime: crate_name(ICYDB_PACKAGE)
                .map(found_crate_path)
                .unwrap_or_default(),
        })
    }
}

#[cfg(test)]
impl CratePaths {
    pub(crate) fn test() -> Self {
        Self {
            model: quote!(::icydb_model),
            runtime: quote!(::icydb),
        }
    }
}

fn resolve_model_path(explicit: Option<&Path>) -> Result<TokenStream, darling::Error> {
    if let Some(path) = explicit {
        return Ok(quote!(#path));
    }

    if let Ok(found) = crate_name(MODEL_PACKAGE) {
        return Ok(found_crate_path(found));
    }

    let facade = crate_name(ICYDB_PACKAGE).map_err(|error| {
        darling::Error::custom(format!(
            "generated output requires a direct `{MODEL_PACKAGE}` dependency, a direct `{ICYDB_PACKAGE}` facade dependency, or an explicit `model_crate = \"...\"` override: {error}"
        ))
    })?;
    let facade = found_crate_path(facade);
    Ok(quote!(#facade::model))
}

fn resolve_path(package: &str) -> Result<TokenStream, darling::Error> {
    crate_name(package).map(found_crate_path).map_err(|error| {
        darling::Error::custom(format!(
            "generated output requires a direct `{package}` dependency: {error}"
        ))
    })
}

fn found_crate_path(found: FoundCrate) -> TokenStream {
    match found {
        FoundCrate::Itself => quote!(crate),
        FoundCrate::Name(name) => {
            let ident = syn::Ident::new(name.as_str(), proc_macro2::Span::call_site());
            quote!(::#ident)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CratePathOverrides, found_crate_path};
    use darling::ast::NestedMeta;
    use proc_macro_crate::FoundCrate;
    use quote::quote;

    #[test]
    fn explicit_model_path_is_resolved_without_consuming_authored_arguments() {
        let mut args = NestedMeta::parse_meta_list(quote!(
            model_crate = "crate::model_bridge",
            name = "icydb_model"
        ))
        .unwrap();
        let paths = CratePathOverrides::extract(&mut args)
            .unwrap()
            .resolve()
            .unwrap();
        assert_eq!(
            paths.model.to_string(),
            quote!(crate::model_bridge).to_string()
        );
        let expected = NestedMeta::parse_meta_list(quote!(name = "icydb_model")).unwrap();
        assert_eq!(args, expected);
    }

    #[test]
    fn duplicate_and_malformed_overrides_reject() {
        for args in [
            quote!(model_crate = "model_api", model_crate = "other"),
            quote!(model_crate = 3),
            quote!(model_crate = "bad::"),
        ] {
            let mut args = NestedMeta::parse_meta_list(args).unwrap();
            assert!(CratePathOverrides::extract(&mut args).is_err());
        }
    }

    #[test]
    fn package_resolution_distinguishes_self_from_renamed_dependencies() {
        assert_eq!(found_crate_path(FoundCrate::Itself).to_string(), "crate");
        assert_eq!(
            found_crate_path(FoundCrate::Name("model_api".into())).to_string(),
            quote!(::model_api).to_string()
        );
    }
}
