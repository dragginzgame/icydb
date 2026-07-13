//! Module: node::enum
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{imp::*, prelude::*};
use icydb_utils::{Case, Casing};

///
/// Enum
///

#[derive(Debug, FromMeta)]
pub struct Enum {
    #[darling(default, skip)]
    pub(crate) def: Def,

    #[darling(multiple, rename = "variant")]
    pub(crate) variants: Vec<EnumVariant>,

    #[darling(default)]
    pub(crate) ty: Type,

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
        self.traits.with_type_traits().validate()?;

        for variant in &self.variants {
            variant.validate()?;
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
                    .with_span(&variant.ident));
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
            .with_span(&default_variant.ident));
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
    fn schema_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let def = &self.def.schema_part();
        let variants = self
            .variants
            .iter()
            .map(|variant| variant.schema_part_for_enum(&ident));
        let ty = &self.ty.schema_part();

        // quote
        quote! {
            ::icydb::schema::node::Enum::new(#def, &[#(#variants),*], #ty)
        }
    }
}

impl HasTraits for Enum {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = self.traits.with_type_traits().build();
        traits.add(TraitKind::Inherent);
        traits.add(TraitKind::PersistedStructuralValueCodec);
        traits.add(TraitKind::RuntimeValue);

        // extra traits
        if self.is_unit_enum() {
            traits.extend([TraitKind::Copy, TraitKind::Hash, TraitKind::PartialOrd]);
        }

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::Inherent => InherentTrait::strategy(self),
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::PersistedStructuralValueCodec => {
                PersistedStructuralValueCodecTrait::strategy(self)
            }
            TraitKind::RuntimeValue => RuntimeValueTrait::strategy(self),
            TraitKind::SanitizeAuto => SanitizeAutoTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => None,
        }
    }
}

impl HasType for Enum {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let variants = self.variants.iter().map(HasTypeExpr::type_expr);

        quote! {
            pub enum #ident {
                #(#variants),*
            }
        }
    }
}

impl ToTokens for Enum {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}

///
/// EnumVariant
///

#[derive(Clone, Debug, FromMeta)]
pub struct EnumVariant {
    pub(crate) ident: Ident,

    #[darling(default)]
    pub(crate) value: Option<Value>,

    #[darling(default)]
    pub(crate) default: bool,
}

impl EnumVariant {
    pub(crate) fn name_const_ident(&self) -> Ident {
        let constant = self.ident.to_string().to_case(Case::Constant);
        let variant_ident = self.ident.to_string();
        let constant = if constant == variant_ident {
            format!("{constant}_NAME")
        } else {
            constant
        };

        format_ident!("{constant}")
    }

    pub fn validate(&self) -> Result<(), DarlingError> {
        // Enforce variant naming before validating value payloads.
        let ident_str = self.ident.to_string();
        if !ident_str.is_case(Case::UpperCamel) {
            return Err(DarlingError::custom(format!(
                "variant ident '{ident_str}' must be in UpperCamelCase",
            ))
            .with_span(&self.ident));
        }

        if let Some(value) = &self.value {
            value.validate()?;

            if value.cardinality() == Cardinality::Many
                && !value.item.indirect
                && value.item.relation.is_none()
            {
                let item_ty = value.item.type_expr().to_string().replace(' ', "");
                let message = format!(
                    "Vec<{item_ty}> does not implement the generated value surface. If this list holds a recursive or complex value type, use item(indirect, ...) to store Vec<Box<{item_ty}>>."
                );
                return Err(DarlingError::custom(message).with_span(&self.ident));
            }
        }

        Ok(())
    }
}

impl HasSchemaPart for EnumVariant {
    fn schema_part(&self) -> TokenStream {
        let ident = quote_one(&self.ident, to_str_lit);
        let value = quote_option(self.value.as_ref(), Value::schema_part);

        // quote
        quote! {
            ::icydb::schema::node::EnumVariant::new(
                #ident,
                #value,
            )
        }
    }
}

impl EnumVariant {
    fn schema_part_for_enum(&self, enum_ident: &Ident) -> TokenStream {
        let name_const_ident = self.name_const_ident();
        let value = quote_option(self.value.as_ref(), Value::schema_part);

        // quote
        quote! {
            ::icydb::schema::node::EnumVariant::new(
                #enum_ident::#name_const_ident,
                #value,
            )
        }
    }
}

impl HasTypeExpr for EnumVariant {
    fn type_expr(&self) -> TokenStream {
        let ident = &self.ident;

        let body = if let Some(value) = &self.value {
            let value = value.type_expr();
            quote!(#ident(#value))
        } else {
            quote!(#ident)
        };

        quote! {
            #body
        }
    }
}
