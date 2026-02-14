use crate::prelude::*;
use canic_utils::case::{Case, Casing};

///
/// HasDef
///

pub trait HasDef {
    fn def(&self) -> &Def;
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
    pub derive: TokenStream,
    pub impls: TokenStream,
}

///
/// HasMacro
///
/// High-level entrypoint for procedural code generation.
/// Coordinates schema emission, type emission, trait impls, and view generation.
///

pub trait HasMacro: HasSchema + HasTraits + HasType + ToTokens {
    /// Generate all Rust tokens for this node: schema consts, derives, impls, and view structs.
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
        let mut has_serde_derive = false;

        for tr in self.traits() {
            // Each trait can either have an explicit map or fallback to default.
            let strat = self.map_trait(tr).or_else(|| self.default_strategy(tr));
            let attr = self.map_attribute(tr);

            if let Some(strategy) = strat {
                if let Some(ts) = strategy.imp {
                    impls.extend(ts);
                }

                if let Some(derive_tr) = strategy.derive
                    && let Some(path) = derive_tr.derive_path()
                {
                    if matches!(derive_tr, TraitKind::Deserialize | TraitKind::Serialize) {
                        has_serde_derive = true;
                    }
                    derive_traits.push(path);
                }
            } else if let Some(path) = tr.derive_path() {
                if matches!(tr, TraitKind::Deserialize | TraitKind::Serialize) {
                    has_serde_derive = true;
                }
                derive_traits.push(path);
            }

            if let Some(attr_tokens) = attr {
                attrs.push(attr_tokens);
            }
        }

        let mut derive = if derive_traits.is_empty() {
            quote!()
        } else {
            quote!(#[derive(#(#derive_traits),*)])
        };

        if has_serde_derive {
            attrs.push(quote!(#[serde(crate = "::icydb::__reexports::serde")]));
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

    /// Module that contains generated View/Create/Update companion types.
    fn views_mod_ident(&self) -> Ident {
        let base = self.def().ident().to_string().to_case(Case::Snake);
        format_ident!("{base}_views")
    }

    /// Naming shortcuts for companion types.
    fn view_ident(&self) -> Ident {
        format_ident!("View")
    }

    fn create_ident(&self) -> Ident {
        format_ident!("Create")
    }

    fn update_ident(&self) -> Ident {
        format_ident!("Update")
    }

    /// Fully-qualified path to the generated `View` type.
    fn view_path(&self) -> TokenStream {
        let views_mod_ident = self.views_mod_ident();
        let view_ident = self.view_ident();

        quote!(#views_mod_ident::#view_ident)
    }

    /// Fully-qualified path to the generated `Create` type.
    fn create_path(&self) -> TokenStream {
        let views_mod_ident = self.views_mod_ident();
        let create_ident = self.create_ident();

        quote!(#views_mod_ident::#create_ident)
    }

    /// Fully-qualified path to the generated `Update` type.
    fn update_path(&self) -> TokenStream {
        let views_mod_ident = self.views_mod_ident();
        let update_ident = self.update_ident();

        quote!(#views_mod_ident::#update_ident)
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
    /// List of traits this node participates in (either derived or implemented).
    fn traits(&self) -> Vec<TraitKind> {
        vec![]
    }

    /// Map a specific trait to a custom implementation.
    /// Return `None` to use the `default_strategy` fallback.
    fn map_trait(&self, _: TraitKind) -> Option<TraitStrategy> {
        None
    }

    /// Emit a custom `#[attribute(...)]` for this trait.
    fn map_attribute(&self, _: TraitKind) -> Option<TokenStream> {
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
            TraitKind::CanisterKind
            | TraitKind::SanitizeAuto
            | TraitKind::SanitizeCustom
            | TraitKind::ValidateAuto
            | TraitKind::ValidateCustom
            | TraitKind::Visitable => {
                let tokens = Implementor::new(def, t).to_token_stream();
                Some(TraitStrategy::from_impl(tokens))
            }

            _ => None,
        }
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
        let kind = Self::schema_node_kind();

        quote! {
            const #const_var: ::icydb::schema::node::#kind = #schema_expr;

            #[cfg(not(target_arch = "wasm32"))]
            #[::icydb::__reexports::ctor::ctor(anonymous, crate_path = ::icydb::__reexports::ctor)]
            fn __ctor() {
                ::icydb::schema::build::schema_write().insert_node(
                    ::icydb::schema::node::SchemaNode::#kind(#const_var)
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
    Record,
    Sanitizer,
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
