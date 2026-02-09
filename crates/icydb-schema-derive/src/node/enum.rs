use crate::{imp::*, prelude::*};
use canic_utils::case::{Case, Casing};

///
/// Enum
///

#[derive(Debug, FromMeta)]
pub struct Enum {
    #[darling(default, skip)]
    pub def: Def,

    #[darling(multiple, rename = "variant")]
    pub variants: Vec<EnumVariant>,

    #[darling(default)]
    pub ty: Type,

    #[darling(default)]
    pub traits: TraitBuilder,
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

        // Phase 2: validate unspecified/default ordering rules.
        let mut unspecified_index = None;
        let mut default_count = 0;
        for (index, variant) in self.variants.iter().enumerate() {
            if variant.unspecified {
                if unspecified_index.is_none() {
                    unspecified_index = Some((index, variant));
                } else {
                    return Err(DarlingError::custom(
                        "there should not be more than one unspecified variant",
                    )
                    .with_span(&variant.ident));
                }
            }
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

        if let Some((index, variant)) = unspecified_index
            && index != 0
        {
            return Err(DarlingError::custom(
                "the unspecified variant must be the first in the list",
            )
            .with_span(&variant.ident));
        }

        let traits = self.traits.with_type_traits().build();
        if traits.contains(&TraitKind::Default) && self.default_variant().is_none() {
            return Err(DarlingError::custom(
                "default variant is required when Default is enabled",
            ));
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
        let def = &self.def.schema_part();
        let variants = quote_slice(&self.variants, EnumVariant::schema_part);
        let ty = &self.ty.schema_part();

        // quote
        quote! {
            ::icydb::schema::node::Enum {
                def: #def,
                variants: #variants,
                ty: #ty,
            }
        }
    }
}

impl HasTraits for Enum {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = self.traits.with_type_traits().build();
        traits.add(TraitKind::Inherent);
        traits.add(TraitKind::FieldValue);

        // extra traits
        if self.is_unit_enum() {
            traits.extend(vec![
                TraitKind::Copy,
                TraitKind::Hash,
                TraitKind::PartialOrd,
            ]);
        }

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::AsView => AsViewTrait::strategy(self),
            TraitKind::Inherent => InherentTrait::strategy(self),
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::FieldValue => FieldValueTrait::strategy(self),
            TraitKind::SanitizeAuto => SanitizeAutoTrait::strategy(self),
            TraitKind::MergePatch => MergePatchTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => {
                // NOTE: Unsupported traits are intentionally ignored for Enum nodes.
                None
            }
        }
    }

    fn map_attribute(&self, t: TraitKind) -> Option<TokenStream> {
        match t {
            TraitKind::Sorted => TraitKind::Sorted.derive_attribute(),
            _ => {
                // NOTE: Only Sorted emits a derive attribute for enums.
                None
            }
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
    #[darling(default = EnumVariant::unspecified_ident)]
    pub ident: Ident,

    #[darling(default)]
    pub value: Option<Value>,

    #[darling(default)]
    pub default: bool,

    #[darling(default)]
    pub unspecified: bool,
}

impl EnumVariant {
    fn unspecified_ident() -> Ident {
        format_ident!("Unspecified")
    }

    /// Pick the effective identifier for codegen
    pub fn effective_ident(&self) -> Ident {
        if self.unspecified {
            Self::unspecified_ident()
        } else {
            self.ident.clone()
        }
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
                    "OrderedList<{item_ty}> does not implement FieldValue. If this list holds a recursive or complex value type, use item(indirect, ...) to store OrderedList<Box<{item_ty}>>."
                );
                return Err(DarlingError::custom(message).with_span(&self.ident));
            }
        }

        Ok(())
    }
}

impl HasSchemaPart for EnumVariant {
    fn schema_part(&self) -> TokenStream {
        let Self {
            default,
            unspecified,
            ..
        } = self;
        let ident = quote_one(&self.ident, to_str_lit);
        let value = quote_option(self.value.as_ref(), Value::schema_part);

        // quote
        quote! {
            ::icydb::schema::node::EnumVariant {
                ident: #ident,
                value : #value,
                default: #default,
                unspecified: #unspecified,
            }
        }
    }
}

impl HasTypeExpr for EnumVariant {
    fn type_expr(&self) -> TokenStream {
        let ident = self.effective_ident();

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
