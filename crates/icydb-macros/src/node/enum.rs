use crate::prelude::*;

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
        let sp = paths().schema;
        quote! {
            #sp::node::Enum {
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
        use crate::imp::*;

        match t {
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::FieldValue => FieldValueTrait::strategy(self),
            TraitKind::Filterable => FilterableTrait::strategy(self),
            TraitKind::SanitizeAuto => SanitizeAutoTrait::strategy(self),
            TraitKind::UpdateView => UpdateViewTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::View => ViewTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => None,
        }
    }

    fn map_attribute(&self, t: TraitKind) -> Option<TokenStream> {
        match t {
            TraitKind::Sorted => TraitKind::Sorted.derive_attribute(),
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
        let sp = paths().schema;
        quote! {
            #sp::node::EnumVariant {
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
