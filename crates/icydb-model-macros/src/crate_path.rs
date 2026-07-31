//! Generated crate-path resolution.
//!
//! Macro output names package owners rather than assuming the dependency key
//! used by the consuming crate. Attribute macros may override the model path
//! explicitly; runtime paths and helper derives use Cargo package resolution.

use darling::ast::NestedMeta;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Group, TokenStream, TokenTree};
use quote::{ToTokens, quote};
use syn::{Expr, Lit, Path};

const ICYDB_PACKAGE: &str = "icydb";
const MODEL_PACKAGE: &str = "icydb-model";

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

/// Rewrite package-owner sentinels in generated output.
pub(crate) fn rewrite_generated_paths(
    tokens: TokenStream,
    overrides: &CratePathOverrides,
) -> Result<TokenStream, darling::Error> {
    let model = contains_ident(&tokens, "icydb_model")
        .then(|| resolve_path(MODEL_PACKAGE, overrides.model.as_ref(), Some("model_crate")))
        .transpose()?;
    let icydb = contains_ident(&tokens, "icydb")
        .then(|| resolve_path(ICYDB_PACKAGE, None, None))
        .transpose()?;

    Ok(rewrite_stream(tokens, model.as_ref(), icydb.as_ref()))
}

fn resolve_path(
    package: &str,
    explicit: Option<&Path>,
    override_name: Option<&str>,
) -> Result<TokenStream, darling::Error> {
    if let Some(path) = explicit {
        return Ok(quote!(#path));
    }

    match crate_name(package).map_err(|error| {
        let override_hint = override_name.map_or_else(String::new, |name| {
            format!(" or an explicit `{name} = \"...\"` override")
        });
        darling::Error::custom(format!(
            "generated output requires a direct `{package}` dependency{override_hint}: {error}"
        ))
    })? {
        FoundCrate::Itself => Ok(quote!(crate)),
        FoundCrate::Name(name) => {
            let ident = syn::Ident::new(name.as_str(), proc_macro2::Span::call_site());
            Ok(quote!(::#ident))
        }
    }
}

fn contains_ident(tokens: &TokenStream, needle: &str) -> bool {
    tokens.clone().into_iter().any(|token| match token {
        TokenTree::Group(group) => contains_ident(&group.stream(), needle),
        TokenTree::Ident(ident) => ident == needle,
        TokenTree::Literal(_) | TokenTree::Punct(_) => false,
    })
}

fn rewrite_stream(
    tokens: TokenStream,
    model: Option<&TokenStream>,
    icydb: Option<&TokenStream>,
) -> TokenStream {
    let input = tokens.into_iter().collect::<Vec<_>>();
    let mut output = TokenStream::new();
    let mut index = 0;

    while index < input.len() {
        if index + 2 < input.len()
            && is_colon(&input[index])
            && is_colon(&input[index + 1])
            && let TokenTree::Ident(ident) = &input[index + 2]
            && let Some(path) = replacement_path(ident.to_string().as_str(), model, icydb)
        {
            output.extend(path.clone());
            index += 3;
            continue;
        }

        match &input[index] {
            TokenTree::Group(group) => {
                let mut rewritten = Group::new(
                    group.delimiter(),
                    rewrite_stream(group.stream(), model, icydb),
                );
                rewritten.set_span(group.span());
                output.extend([TokenTree::Group(rewritten)]);
            }
            TokenTree::Ident(ident) => {
                if let Some(path) = replacement_path(ident.to_string().as_str(), model, icydb) {
                    output.extend(path.clone());
                } else {
                    output.extend([input[index].clone()]);
                }
            }
            TokenTree::Literal(literal) => {
                output.extend([TokenTree::Literal(
                    rewrite_path_literal(literal, model, icydb).unwrap_or_else(|| literal.clone()),
                )]);
            }
            TokenTree::Punct(_) => {
                output.extend([input[index].clone()]);
            }
        }
        index += 1;
    }

    output
}

fn rewrite_path_literal(
    literal: &proc_macro2::Literal,
    model: Option<&TokenStream>,
    icydb: Option<&TokenStream>,
) -> Option<proc_macro2::Literal> {
    let span = literal.span();
    let tokens = TokenStream::from(TokenTree::Literal(literal.clone()));
    let value = syn::parse2::<syn::LitStr>(tokens).ok()?.value();
    let rewritten = rewrite_path_string(value.as_str(), "::icydb_model", model)
        .or_else(|| rewrite_path_string(value.as_str(), "::icydb", icydb))?;
    let mut literal = proc_macro2::Literal::string(rewritten.as_str());
    literal.set_span(span);
    Some(literal)
}

fn rewrite_path_string(
    value: &str,
    sentinel: &str,
    replacement: Option<&TokenStream>,
) -> Option<String> {
    let suffix = value.strip_prefix(sentinel)?;
    let replacement = replacement?.to_string().replace(' ', "");
    Some(format!("{replacement}{suffix}"))
}

fn replacement_path<'a>(
    ident: &str,
    model: Option<&'a TokenStream>,
    icydb: Option<&'a TokenStream>,
) -> Option<&'a TokenStream> {
    match ident {
        "icydb_model" => model,
        "icydb" => icydb,
        _ => None,
    }
}

fn is_colon(token: &TokenTree) -> bool {
    matches!(token, TokenTree::Punct(punct) if punct.as_char() == ':')
}

#[cfg(test)]
mod tests {
    use darling::ast::NestedMeta;
    use quote::quote;

    use super::{CratePathOverrides, rewrite_generated_paths};

    fn args(tokens: proc_macro2::TokenStream) -> Vec<NestedMeta> {
        darling::ast::NestedMeta::parse_meta_list(tokens)
            .expect("crate path test arguments should parse")
    }

    #[test]
    fn explicit_model_path_is_removed_from_node_arguments_and_rewrites_output() {
        let mut input = args(quote!(model_crate = "model_api", name = "fixture"));
        let overrides = CratePathOverrides::extract(&mut input).expect("overrides should validate");
        let rewritten = rewrite_generated_paths(
            quote!(
                ::icydb_model::Path::PATH;
                #[candid_path("::icydb_model::__reexports::candid")]
                struct Value;
            ),
            &overrides,
        )
        .expect("explicit paths should rewrite")
        .to_string();

        assert_eq!(input.len(), 1);
        assert!(rewritten.contains("model_api :: Path"));
        assert!(
            rewritten.contains("\"model_api::__reexports::candid\""),
            "unexpected rewritten attributes: {rewritten}",
        );
        assert!(!rewritten.contains("icydb_model"));
    }

    #[test]
    fn duplicate_explicit_paths_reject() {
        let mut input = args(quote!(
            model_crate = "model_api",
            model_crate = "other_model"
        ));

        let Err(error) = CratePathOverrides::extract(&mut input) else {
            panic!("duplicate model paths must reject");
        };

        assert!(error.to_string().contains("may be specified only once"));
    }
}
