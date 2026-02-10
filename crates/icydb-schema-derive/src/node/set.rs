use crate::{imp::*, prelude::*};

///
/// Set
///

#[derive(Debug, FromMeta)]
pub struct Set {
    #[darling(default, skip)]
    pub def: Def,

    pub item: Item,

    #[darling(default)]
    pub ty: Type,

    #[darling(default)]
    pub traits: TraitBuilder,
}

impl HasDef for Set {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Set {
    fn validate(&self) -> Result<(), DarlingError> {
        self.traits.with_type_traits().validate()?;
        self.item.validate()?;

        Ok(())
    }
}

impl HasSchema for Set {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Set
    }
}

impl HasSchemaPart for Set {
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();
        let item = self.item.schema_part();
        let ty = self.ty.schema_part();

        // quote
        quote! {
            ::icydb::schema::node::Set {
                def: #def,
                item: #item,
                ty: #ty,
            }
        }
    }
}

impl HasTraits for Set {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = self.traits.with_type_traits().build();
        traits.extend(vec![
            TraitKind::Collection,
            TraitKind::FieldValue,
            TraitKind::Inherent,
        ]);

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::AsView => AsViewTrait::strategy(self),
            TraitKind::Collection => CollectionTrait::strategy(self),
            TraitKind::FieldValue => FieldValueTrait::strategy(self),
            TraitKind::From => FromTrait::strategy(self),
            TraitKind::Inherent => InherentTrait::strategy(self),
            TraitKind::SanitizeAuto => SanitizeAutoTrait::strategy(self),
            TraitKind::UpdateView => UpdateViewTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),
            _ => None,
        }
    }
}

impl HasType for Set {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let item = &self.item.type_expr();

        quote! {
            #[repr(transparent)]
            pub struct #ident(pub ::std::collections::BTreeSet<#item>);
        }
    }
}

impl ToTokens for Set {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
