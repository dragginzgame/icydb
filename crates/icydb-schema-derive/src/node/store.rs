use crate::{imp::*, prelude::*};
use canic_utils::case::{Case, Casing};

///
/// Store
///

#[derive(Debug, FromMeta)]
pub struct Store {
    #[darling(default, skip)]
    pub def: Def,

    pub ident: Ident,
    pub canister: Path,
    pub data_memory_id: u8,
    pub index_memory_id: u8,
}

impl HasDef for Store {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Store {
    fn validate(&self) -> Result<(), DarlingError> {
        let ident_str = self.ident.to_string();
        if !ident_str.is_case(Case::UpperSnake) {
            return Err(DarlingError::custom(format!(
                "ident '{ident_str}' must be UPPER_SNAKE_CASE",
            ))
            .with_span(&self.ident));
        }

        Ok(())
    }
}

impl HasSchema for Store {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Store
    }
}

impl HasSchemaPart for Store {
    fn schema_part(&self) -> TokenStream {
        let def = &self.def.schema_part();
        let ident = quote_one(&self.ident, to_str_lit);
        let canister = quote_one(&self.canister, to_path);
        let data_memory_id = &self.data_memory_id;
        let index_memory_id = &self.index_memory_id;

        // quote
        quote! {
            ::icydb::schema::node::Store{
                def: #def,
                ident: #ident,
                canister: #canister,
                data_memory_id: #data_memory_id,
                index_memory_id: #index_memory_id,
            }
        }
    }
}

impl HasTraits for Store {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = TraitBuilder::default().build();
        traits.add(TraitKind::StoreKind);

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::StoreKind => StoreKindTrait::strategy(self),
            _ => None,
        }
    }
}

impl HasType for Store {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();

        quote! {
            pub struct #ident;
        }
    }
}

impl ToTokens for Store {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
