//! Module: node::enum
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::case::{Case, Casing};
use crate::{imp::*, prelude::*};

///
/// Enum
///

#[derive(Debug, FromMeta)]
pub struct Enum {
    #[darling(default, skip)]
    pub(crate) def: Def,

    #[darling(default)]
    pub(crate) name: Option<LitStr>,

    #[darling(multiple, rename = "variant")]
    pub(crate) variants: Vec<EnumVariant>,

    #[darling(default)]
    pub(crate) ty: Type,

    #[darling(default)]
    pub(crate) sorted: bool,

    #[darling(default)]
    pub(crate) traits: TraitBuilder,
}

impl Enum {
    pub fn is_unit_enum(&self) -> bool {
        self.variants.iter().all(|v| v.value.is_none())
    }

    pub fn default_variant(&self) -> Option<&EnumVariant> {
        self.variants.iter().find(|v| v.default)
    }
}

impl HasDef for Enum {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Enum {
    fn validate(&self) -> Result<(), DarlingError> {
        // Phase 1: validate trait configuration and variant shapes.
        self.validate_traits()?;

        for variant in &self.variants {
            variant.validate_variant(self.def.paths())?;
        }

        // Phase 2: validate Rust default selection rules.
        let mut default_count = 0;
        for variant in &self.variants {
            if variant.default {
                default_count += 1;
                if default_count > 1 {
                    return Err(DarlingError::custom(format!(
                        "exactly one variant must be marked as default, found {default_count}"
                    ))
                    .with_span(&variant.name));
                }
            }
        }

        let default_requested = self.traits.explicitly_adds(TraitKind::Default);
        if default_requested && self.default_variant().is_none() {
            return Err(DarlingError::custom(format!(
                "Default was requested for enum {}, but no variant is marked `default`",
                self.def.ident()
            ))
            .with_span(&self.def.ident()));
        }
        if !default_requested && let Some(default_variant) = self.default_variant() {
            return Err(DarlingError::custom(format!(
                "enum {} marks a Rust default variant but does not enable `traits(add(Default))`",
                self.def.ident()
            ))
            .with_span(&default_variant.name));
        }

        Ok(())
    }
}

impl HasSchema for Enum {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Enum
    }
}

impl HasSchemaPart for Enum {
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let def = &self.def.schema_part(paths);
        let name = self.current_name_literal(self.name.as_ref());
        let variants = self
            .variants
            .iter()
            .map(|value| EnumVariant::schema_part(value, paths));
        let ty = &self.ty.schema_part(paths);

        // quote
        quote! {
            #model::node::Enum::new(#def, #name, &[#(#variants),*], #ty)
        }
    }
}

impl HasTraits for Enum {
    fn application_type_kind(&self) -> Option<ApplicationTypeKind> {
        Some(ApplicationTypeKind::Enum)
    }

    fn trait_builder(&self) -> Option<&TraitBuilder> {
        Some(&self.traits)
    }

    fn trait_baseline(&self) -> TraitSet {
        let mut traits = application_type_trait_set();
        if self.is_unit_enum() {
            traits.extend([
                TraitKind::Copy,
                TraitKind::Hash,
                TraitKind::Ord,
                TraitKind::PartialOrd,
            ]);
        }

        traits
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        crate::node::traits::application_value_trait_strategy!(self, t)
    }
}

impl HasType for Enum {
    fn type_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let ident = self.def.ident();
        let variants = self
            .variants
            .iter()
            .map(|value| HasTypeExpr::type_expr(value, paths));
        let sorted = self
            .sorted
            .then(|| quote!(#[#model::__reexports::remain::sorted]));

        quote! {
            #sorted
            pub enum #ident {
                #(#variants),*
            }
        }
    }
}

///
/// EnumVariant
///

#[derive(Clone, Debug, FromMeta)]
pub struct EnumVariant {
    pub(crate) name: Ident,

    #[darling(default)]
    pub(crate) value: Option<Value>,

    #[darling(default)]
    pub(crate) default: bool,
}

impl EnumVariant {
    pub fn validate_variant(&self, paths: &CratePaths) -> Result<(), DarlingError> {
        // Enforce variant naming before validating value payloads.
        let name = self.name.to_string();
        if !name.is_case(Case::UpperCamel) {
            return Err(DarlingError::custom(format!(
                "variant name '{name}' must be in UpperCamelCase",
            ))
            .with_span(&self.name));
        }

        if let Some(value) = &self.value {
            value.validate()?;

            if value.cardinality() == Cardinality::Many
                && !value.item.indirect
                && value.item.relation.is_none()
            {
                let item_ty = value.item.type_expr(paths).to_string().replace(' ', "");
                let message = format!(
                    "Vec<{item_ty}> does not implement the generated value surface. If this list holds a recursive or complex value type, use item(indirect, ...) to store Vec<Box<{item_ty}>>."
                );
                return Err(DarlingError::custom(message).with_span(&self.name));
            }
        }

        Ok(())
    }
}

impl HasSchemaPart for EnumVariant {
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let name = quote_one(&self.name, to_str_lit);
        let value = quote_option(self.value.as_ref(), |value| {
            Value::schema_part(value, paths)
        });

        // quote
        quote! {
            #model::node::EnumVariant::new(
                #name,
                #value,
            )
        }
    }
}

impl HasTypeExpr for EnumVariant {
    fn type_expr(&self, paths: &CratePaths) -> TokenStream {
        let name = &self.name;

        let body = if let Some(value) = &self.value {
            let value = value.type_expr(paths);
            quote!(#name(#value))
        } else {
            quote!(#name)
        };

        quote! {
            #body
        }
    }
}

impl Enum {
    pub(crate) fn generate_tokens(&self, paths: &CratePaths) -> TokenStream {
        let mut tokens = TokenStream::new();
        let base = self.all_tokens(paths);
        let typed_adapter = crate::node::typed_adapter::enum_adapter_tokens(self, paths);
        tokens.extend(quote! {
            #base
            #typed_adapter
        });
        tokens
    }
}

#[cfg(test)]
mod tests {
    use super::Enum;
    use crate::prelude::*;
    use darling::{FromMeta, ast::NestedMeta};
    use quote::quote;

    #[test]
    fn top_level_sorted_flag_emits_enum_order_guard() {
        let args =
            NestedMeta::parse_meta_list(quote!(sorted, variant(name = "A"), variant(name = "B")))
                .expect("enum args should parse");
        let mut node = Enum::from_list(&args).expect("sorted enum should lower");
        node.def = Def::new(
            syn::parse2(quote!(
                pub struct SortedExample {}
            ))
            .expect("enum input should parse as a struct"),
            crate::crate_path::CratePaths::test(),
        );

        assert!(node.sorted);
        assert!(
            node.type_part(&crate::crate_path::CratePaths::test())
                .to_string()
                .contains("remain :: sorted")
        );
    }

    #[test]
    fn unit_enum_baseline_includes_total_rust_ordering() {
        let args = NestedMeta::parse_meta_list(quote!(variant(name = "A"), variant(name = "B")))
            .expect("enum args should parse");
        let mut node = Enum::from_list(&args).expect("enum should lower");
        node.def = Def::new(
            syn::parse2(quote!(
                pub struct OrderedUnitEnum {}
            ))
            .expect("enum input should parse as a struct"),
            crate::crate_path::CratePaths::test(),
        );

        let traits = node.traits();
        for trait_kind in [
            TraitKind::Copy,
            TraitKind::Hash,
            TraitKind::Ord,
            TraitKind::PartialOrd,
        ] {
            assert!(traits.contains(&trait_kind));
        }
    }
}
