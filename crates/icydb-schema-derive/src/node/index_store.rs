use crate::{imp::*, prelude::*};
use canic_utils::case::{Case, Casing};

///
/// IndexStore
///

#[derive(Debug, FromMeta)]
pub struct IndexStore {
    #[darling(default, skip)]
    pub def: Def,
    pub ident: Ident,
    pub canister: Path,

    /// Stable memory backing index entries
    pub entry_memory_id: u8,

    /// Stable memory backing fingerprints
    pub fingerprint_memory_id: u8,
}

impl HasDef for IndexStore {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for IndexStore {
    fn validate(&self) -> Result<(), DarlingError> {
        let ident_str = self.ident.to_string();
        if !ident_str.is_case(Case::UpperSnake) {
            return Err(DarlingError::custom(format!(
                "ident '{ident_str}' must be UPPER_SNAKE_CASE",
            ))
            .with_span(&self.ident));
        }

        if self.entry_memory_id == self.fingerprint_memory_id {
            return Err(DarlingError::custom(format!(
                "entry_memory_id and fingerprint_memory_id must be distinct (both = {})",
                self.entry_memory_id
            ))
            .with_span(&self.def.ident()));
        }

        Ok(())
    }
}

impl HasSchema for IndexStore {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::IndexStore
    }
}

impl HasSchemaPart for IndexStore {
    fn schema_part(&self) -> TokenStream {
        let def = &self.def.schema_part();
        let ident = quote_one(&self.ident, to_str_lit);
        let canister = quote_one(&self.canister, to_path);
        let entry_memory_id = &self.entry_memory_id;
        let fingerprint_memory_id = &self.fingerprint_memory_id;

        // quote
        quote! {
            ::icydb::schema::node::IndexStore{
                def: #def,
                ident: #ident,
                canister: #canister,
                entry_memory_id: #entry_memory_id,
                fingerprint_memory_id: #fingerprint_memory_id,
            }
        }
    }
}

impl HasTraits for IndexStore {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = TraitBuilder::default().build();
        traits.add(TraitKind::IndexStoreKind);

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::IndexStoreKind => IndexStoreKindTrait::strategy(self),
            _ => {
                // NOTE: Only IndexStoreKind is supported for IndexStore nodes.
                None
            }
        }
    }
}

impl HasType for IndexStore {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();

        quote! {
            pub struct #ident;
        }
    }
}

impl ToTokens for IndexStore {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
