use crate::prelude::*;

///
/// Map
///

#[derive(Debug, FromMeta)]
pub struct Map {
    #[darling(default, skip)]
    pub def: Def,

    pub key: Item,
    pub value: Value,

    #[darling(default)]
    pub ty: Type,

    #[darling(default)]
    pub traits: TraitBuilder,
}

impl HasDef for Map {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl HasSchema for Map {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Map
    }
}

impl HasSchemaPart for Map {
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();
        let key = self.key.schema_part();
        let value = self.value.schema_part();
        let ty = self.ty.schema_part();

        // quote
        let sp = paths().schema;
        quote! {
            #sp::node::Map {
                def: #def,
                key: #key,
                value: #value,
                ty: #ty,
            }
        }
    }
}

impl HasTraits for Map {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = self.traits.with_type_traits().build();

        traits.extend(vec![
            TraitKind::Deref,
            TraitKind::DerefMut,
            TraitKind::IntoIterator,
        ]);

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        use crate::imp::*;

        match t {
            TraitKind::Filterable => FilterableTrait::strategy(self),
            TraitKind::From => FromTrait::strategy(self),
            TraitKind::SanitizeAuto => SanitizeAutoTrait::strategy(self),
            TraitKind::UpdateView => UpdateViewTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::View => ViewTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => None,
        }
    }
}

impl HasType for Map {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let key = &self.key.type_expr();
        let value = &self.value.type_expr();

        quote! {
            #[repr(transparent)]
            pub struct #ident(pub ::std::collections::HashMap<#key, #value>);
        }
    }
}

impl ToTokens for Map {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
