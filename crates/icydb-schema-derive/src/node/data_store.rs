use crate::{imp::*, prelude::*};
use canic_utils::case::{Case, Casing};

///
/// DataStore
///

#[derive(Debug, FromMeta)]
pub struct DataStore {
    #[darling(default, skip)]
    pub def: Def,

    pub ident: Ident,
    pub canister: Path,
    pub memory_id: u8,
}

impl HasDef for DataStore {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for DataStore {
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

impl HasSchema for DataStore {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::DataStore
    }
}

impl HasSchemaPart for DataStore {
    fn schema_part(&self) -> TokenStream {
        let def = &self.def.schema_part();
        let ident = quote_one(&self.ident, to_str_lit);
        let canister = quote_one(&self.canister, to_path);
        let memory_id = &self.memory_id;

        // quote
        quote! {
            ::icydb::schema::node::DataStore{
                def: #def,
                ident: #ident,
                canister: #canister,
                memory_id: #memory_id,
            }
        }
    }
}

impl HasTraits for DataStore {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = TraitBuilder::default().build();
        traits.add(TraitKind::DataStoreKind);

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::DataStoreKind => DataStoreKindTrait::strategy(self),
            _ => {
                // NOTE: Only DataStoreKind is supported for DataStore nodes.
                None
            }
        }
    }
}

impl HasType for DataStore {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();

        quote! {
            pub struct #ident;
        }
    }
}

impl ToTokens for DataStore {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
