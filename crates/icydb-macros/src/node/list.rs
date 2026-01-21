use crate::{imp::*, prelude::*};

///
/// List
///

#[derive(Debug, FromMeta)]
pub struct List {
    #[darling(default, skip)]
    pub def: Def,

    pub item: Item,

    #[darling(default)]
    pub ty: Type,

    #[darling(default)]
    pub traits: TraitBuilder,
}

impl HasDef for List {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for List {
    fn validate(&self) -> Result<(), DarlingError> {
        self.traits.with_type_traits().validate()?;
        self.item.validate()
    }
}

impl HasSchema for List {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::List
    }
}

impl HasSchemaPart for List {
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();
        let item = self.item.schema_part();
        let ty = self.ty.schema_part();

        // quote
        quote! {
            ::icydb::schema::node::List {
                def: #def,
                item: #item,
                ty: #ty,
            }
        }
    }
}

impl HasTraits for List {
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
        match t {
            TraitKind::FieldValue => FieldValueTrait::strategy(self),
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

impl HasType for List {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let item = &self.item.type_expr();

        quote! {
            #[repr(transparent)]
            pub struct #ident(pub Vec<#item>);
        }
    }
}

impl ToTokens for List {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
