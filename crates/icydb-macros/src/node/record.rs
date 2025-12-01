use crate::prelude::*;

///
/// Record
///

#[derive(Debug, FromMeta)]
pub struct Record {
    #[darling(default, skip)]
    pub def: Def,

    #[darling(default)]
    pub fields: FieldList,

    #[darling(default)]
    pub traits: TraitBuilder,

    #[darling(default)]
    pub ty: Type,
}

impl HasDef for Record {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl HasSchema for Record {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Record
    }
}

impl HasSchemaPart for Record {
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();
        let fields = self.fields.schema_part();
        let ty = self.ty.schema_part();

        // quote
        let sp = paths().schema;
        quote! {
            #sp::node::Record {
                def: #def,
                fields: #fields,
                ty: #ty,
            }
        }
    }
}

impl HasTraits for Record {
    fn traits(&self) -> Vec<TraitKind> {
        let traits = self.traits.with_type_traits().build();

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        use crate::imp::*;

        match t {
            TraitKind::Default => DefaultTrait::strategy(self),
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
            TraitKind::Default => TraitKind::Default.derive_attribute(),

            _ => None,
        }
    }
}

impl HasType for Record {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let fields = self.fields.type_expr();

        quote! {
            pub struct #ident {
                #fields
            }
        }
    }
}

impl ToTokens for Record {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
