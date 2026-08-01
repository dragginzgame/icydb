//! Module: node::traits
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::case::{Case, Casing};
use crate::prelude::*;

///
/// HasDef
///

pub trait HasDef {
    fn def(&self) -> &Def;

    fn current_name_literal(&self, name: Option<&LitStr>) -> LitStr {
        name.cloned().unwrap_or_else(|| {
            let ident = self.def().ident();
            LitStr::new(&ident.to_string(), ident.span())
        })
    }
}

///
/// ValidateNode
///
/// Runs input validation for macro arguments before code generation.
///

pub trait ValidateNode {
    fn validate(&self) -> Result<(), DarlingError>;

    /// Fatal validation errors that must short-circuit macro expansion.
    fn fatal_errors(&self) -> Vec<syn::Error> {
        Vec::new()
    }
}

///
/// TraitTokens
///
/// Result of trait resolution — combines derived traits and manual impls.
///

pub struct TraitTokens {
    pub(crate) derive: TokenStream,
    pub(crate) impls: TokenStream,
}

///
/// HasMacro
///
/// High-level entrypoint for procedural code generation.
/// Coordinates schema emission, type emission, and trait impl generation.
///

pub trait HasMacro: HasSchema + HasTraits + HasType + ToTokens {
    /// Generate all Rust tokens for this node: schema consts, derives, main type, and impls.
    fn all_tokens(&self) -> TokenStream {
        let TraitTokens { derive, impls } = self.resolve_trait_tokens();
        let schema = self.schema_tokens();
        let type_part = self.type_part();

        quote! {
            // SCHEMA CONSTANT
            #schema

            // MAIN TYPE
            #derive
            #type_part

            // IMPLEMENTATIONS
            #impls
        }
    }

    /// Resolve all derive + impl traits for this node, returning combined code.
    fn resolve_trait_tokens(&self) -> TraitTokens {
        let mut derive_traits = Vec::new();
        let mut attrs = Vec::new();
        let mut impls = TokenStream::new();
        let mut has_serde_deserialize = false;

        for tr in self.traits() {
            let Some(strategy) = self.trait_strategy(tr) else {
                impls.extend(
                    self.missing_trait_strategy_error(self.application_type_kind(), tr)
                        .write_errors(),
                );
                continue;
            };

            if let Some(ts) = strategy.imp {
                impls.extend(ts);
            }

            if let Some(derive_tr) = strategy.derive
                && let Some(path) = derive_tr.derive_path()
            {
                if matches!(derive_tr, TraitKind::Deserialize) {
                    has_serde_deserialize = true;
                }
                if matches!(derive_tr, TraitKind::CandidType) {
                    attrs.push(quote!(
                        #[candid_path("::icydb_model::__reexports::candid")]
                    ));
                }
                derive_traits.push(path);
            }
        }

        let mut derive = if derive_traits.is_empty() {
            quote!()
        } else {
            quote!(#[derive(#(#derive_traits),*)])
        };

        if has_serde_deserialize {
            attrs.push(quote!(#[serde(crate = "::icydb_model::__reexports::serde")]));
        }

        derive.extend(attrs);

        TraitTokens { derive, impls }
    }
}

/// Blanket implementation so any node that satisfies the constraints
/// automatically gets full macro generation.
impl<T> HasMacro for T where T: HasDef + HasSchema + HasTraits + HasType + ToTokens {}

///
/// HasType
///
/// A node that emits a Rust type definition.
///

pub trait HasType: HasDef {
    /// Emit the main Rust type definition (struct, enum, etc.)
    fn type_part(&self) -> TokenStream {
        quote!()
    }
}

///
/// HasTypeExpr
///

pub trait HasTypeExpr {
    fn type_expr(&self) -> TokenStream {
        quote!()
    }
}

///
/// HasTraits
///
/// Describes which traits a schema node implements or derives,
/// and provides default strategies for common trait patterns.
///
/// This layer is responsible only for *trait selection* and *impl generation logic*,
/// not for assembling the final macro output.
///

pub trait HasTraits: HasType {
    /// Application node kind used by the node-aware trait resolver.
    fn application_type_kind(&self) -> Option<ApplicationTypeKind> {
        None
    }

    /// Authored trait directives for an application value node.
    fn trait_builder(&self) -> Option<&TraitBuilder> {
        None
    }

    /// Compiler- and shape-owned traits before authored directives are applied.
    fn trait_baseline(&self) -> TraitSet {
        application_type_trait_set()
    }

    /// List of traits this node participates in (either derived or implemented).
    fn traits(&self) -> Vec<TraitKind> {
        let Some(builder) = self.trait_builder() else {
            return Vec::new();
        };

        builder.build_for_type(self.trait_baseline()).into_vec()
    }

    /// Map a specific trait to a custom implementation.
    /// Return `None` to use the `default_strategy` fallback.
    fn map_trait(&self, _: TraitKind) -> Option<TraitStrategy> {
        None
    }

    /// Provides built-in fallback strategies for common trait types.
    ///
    /// Most schema nodes rely on these automatically unless overridden in `map_trait`.
    fn default_strategy(&self, t: TraitKind) -> Option<TraitStrategy> {
        let def = self.def();
        let ident = def.ident();

        match t {
            // ─────────────────────────────
            // Inline constant path metadata
            // ─────────────────────────────
            TraitKind::Path => {
                let q = quote! {
                    const PATH: &'static str = concat!(module_path!(), "::", stringify!(#ident));
                };
                let tokens = Implementor::new(def, t).set_tokens(q).to_token_stream();

                Some(TraitStrategy::from_impl(tokens))
            }

            // ─────────────────────────────
            // Marker traits — empty impls
            // ─────────────────────────────
            TraitKind::NormalizeAuto
            | TraitKind::NormalizeCustom
            | TraitKind::ValidateAuto
            | TraitKind::ValidateCustom
            | TraitKind::Visitable => {
                let tokens = Implementor::new(def, t).to_token_stream();
                Some(TraitStrategy::from_impl(tokens))
            }

            _ => None,
        }
    }

    /// Resolve a selected trait to its sole derive or implementation strategy.
    fn trait_strategy(&self, trait_kind: TraitKind) -> Option<TraitStrategy> {
        self.map_trait(trait_kind)
            .or_else(|| self.default_strategy(trait_kind))
            .or_else(|| {
                trait_kind
                    .derive_path()
                    .map(|_| TraitStrategy::from_derive(trait_kind))
            })
    }

    /// Validate directives against the complete node/shape baseline and prove
    /// that every selected trait has an emission strategy.
    fn validate_traits(&self) -> Result<(), DarlingError> {
        let Some(node_kind) = self.application_type_kind() else {
            return Ok(());
        };
        let Some(builder) = self.trait_builder() else {
            return Err(DarlingError::custom(format!(
                "internal {} trait resolver has no authored directive owner",
                node_kind.as_str(),
            )));
        };

        let baseline = self.trait_baseline();
        builder.validate_for_type(node_kind, baseline.clone())?;
        let selected = builder.build_for_type(baseline).into_vec();
        for trait_kind in selected {
            let Some(strategy) = self.trait_strategy(trait_kind) else {
                return Err(self.missing_trait_strategy_error(Some(node_kind), trait_kind));
            };
            let has_impl = strategy
                .imp
                .as_ref()
                .is_some_and(|tokens| !tokens.is_empty());
            let has_derive = strategy
                .derive
                .is_some_and(|derived| derived.derive_path().is_some());
            if !has_impl && !has_derive {
                return Err(self.missing_trait_strategy_error(Some(node_kind), trait_kind));
            }
        }

        Ok(())
    }

    fn missing_trait_strategy_error(
        &self,
        node_kind: Option<ApplicationTypeKind>,
        trait_kind: TraitKind,
    ) -> DarlingError {
        let node_kind = node_kind.map_or("generated node", ApplicationTypeKind::as_str);
        DarlingError::custom(format!(
            "generated trait '{trait_kind:?}' for {} {} has no derive or implementation strategy",
            node_kind,
            self.def().ident(),
        ))
        .with_span(&self.def().ident())
    }
}

///
/// HasSchema
///
/// Anything that can emit a schema constant.
///

pub trait HasSchema: HasSchemaPart + HasDef {
    /// The kind of schema node this represents (Entity, Enum, etc.)
    fn schema_node_kind() -> SchemaNodeKind;

    /// The uppercase snake-case constant name used in the generated schema file.
    fn schema_const(&self) -> Ident {
        let ident_s = self.def().ident().to_string().to_case(Case::UpperSnake);
        format_ident!("{ident_s}_CONST")
    }

    /// Emits the full schema constant + registration constructor.
    fn schema_tokens(&self) -> TokenStream {
        let schema_expr = self.schema_part();
        if schema_expr.is_empty() {
            return quote!();
        }

        let const_var = self.schema_const();
        let ctor = format_ident!(
            "__icydb_register_{}",
            const_var.to_string().to_case(Case::Snake)
        );
        let kind = Self::schema_node_kind();

        quote! {
            const #const_var: ::icydb_model::node::#kind = #schema_expr;

            #[cfg(not(target_arch = "wasm32"))]
            #[::icydb_model::__reexports::ctor::ctor(
                unsafe,
                anonymous,
                crate_path = ::icydb_model::__reexports::ctor
            )]
            fn #ctor() {
                ::icydb_model::build::register_node(
                    ::icydb_model::node::SchemaNode::#kind(#const_var)
                );
            }
        }
    }
}

#[derive(Debug)]
#[remain::sorted]
pub enum SchemaNodeKind {
    Canister,
    Entity,
    Enum,
    List,
    Map,
    Newtype,
    Normalizer,
    Record,
    Set,
    Store,
    Tuple,
    Validator,
}

impl ToTokens for SchemaNodeKind {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        format_ident!("{self:?}").to_tokens(tokens);
    }
}

///
/// HasSchemaPart
///
/// Low-level helper for schema fragments.
///

pub trait HasSchemaPart {
    fn schema_part(&self) -> TokenStream {
        quote!()
    }
}
