//! Module: node::store
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::prelude::*;
use darling::ast::NestedMeta;

///
/// Store
///

#[derive(Debug)]
pub struct Store {
    pub(crate) def: Def,

    pub(crate) canister: Path,
    pub(crate) storage: ParsedStoreStorage,
}

#[derive(Debug)]
pub(crate) enum ParsedStoreStorage {
    Heap(ParsedStoreHeapConfig),
    Journaled(ParsedStoreJournaledMemoryConfig),
}

impl ParsedStoreStorage {
    const fn journaled(&self) -> Option<&ParsedStoreJournaledMemoryConfig> {
        match self {
            Self::Journaled(journaled) => Some(journaled),
            Self::Heap(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ParsedStoreHeapConfig;

#[derive(Debug, FromMeta)]
pub(crate) struct ParsedStoreJournaledMemoryConfig {
    pub(crate) key: String,
}

impl FromMeta for Store {
    fn from_list(items: &[NestedMeta]) -> Result<Self, DarlingError> {
        let mut canister = None;
        let mut storage = None;

        for item in items {
            match item {
                NestedMeta::Meta(syn::Meta::NameValue(name_value)) => {
                    if name_value.path.is_ident("canister") {
                        set_once(
                            &mut canister,
                            Path::from_expr(&name_value.value)?,
                            "store(...) accepts only one canister = ... argument",
                            &name_value.path,
                        )?;
                        continue;
                    }

                    return Err(
                        DarlingError::custom(STORE_ARGS_MESSAGE).with_span(&name_value.path)
                    );
                }
                NestedMeta::Meta(syn::Meta::List(list)) if list.path.is_ident("storage") => {
                    set_once(
                        &mut storage,
                        parse_store_storage(list)?,
                        "store(...) accepts only one storage(...) argument",
                        &list.path,
                    )?;
                }
                NestedMeta::Meta(syn::Meta::List(list)) => {
                    return Err(DarlingError::custom(STORE_ARGS_MESSAGE).with_span(&list.path));
                }
                NestedMeta::Meta(syn::Meta::Path(path)) => {
                    return Err(DarlingError::custom(STORE_ARGS_MESSAGE).with_span(path));
                }
                _ => return Err(DarlingError::custom(STORE_ARGS_MESSAGE)),
            }
        }

        let canister =
            canister.ok_or_else(|| DarlingError::custom("store(...) requires canister = ..."))?;
        let storage = storage.ok_or_else(|| {
            DarlingError::custom("store(...) requires storage(heap()) or storage(journaled(...))")
        })?;

        Ok(Self {
            def: Def::default(),
            canister,
            storage,
        })
    }
}

const STORE_ARGS_MESSAGE: &str =
    "store(...) supports canister = ... and storage(heap()) or storage(journaled(...))";

fn set_once<T>(
    slot: &mut Option<T>,
    value: T,
    duplicate_message: &'static str,
    span: &syn::Path,
) -> Result<(), DarlingError> {
    if slot.replace(value).is_some() {
        return Err(DarlingError::custom(duplicate_message).with_span(span));
    }

    Ok(())
}

fn parse_store_storage(list: &syn::MetaList) -> Result<ParsedStoreStorage, DarlingError> {
    let items = NestedMeta::parse_meta_list(list.tokens.clone())?;
    let [item] = items.as_slice() else {
        return Err(DarlingError::custom(
            "storage(...) requires exactly one storage mode: heap() or journaled(...)",
        )
        .with_span(&list.path));
    };

    match item {
        NestedMeta::Meta(syn::Meta::List(mode)) if mode.path.is_ident("heap") => {
            parse_heap_config(mode).map(ParsedStoreStorage::Heap)
        }
        NestedMeta::Meta(syn::Meta::List(mode)) if mode.path.is_ident("journaled") => {
            parse_journaled_memory_config(mode).map(ParsedStoreStorage::Journaled)
        }
        NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("heap") => Err(
            DarlingError::custom("storage(heap) must be written as storage(heap())")
                .with_span(path),
        ),
        NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("journaled") => Err(
            DarlingError::custom("storage(journaled) must be written as storage(journaled(...))")
                .with_span(path),
        ),
        NestedMeta::Meta(syn::Meta::List(mode)) => Err(DarlingError::custom(
            "unknown store storage mode; expected storage(heap()) or storage(journaled(...))",
        )
        .with_span(&mode.path)),
        NestedMeta::Meta(syn::Meta::Path(path)) => Err(DarlingError::custom(
            "unknown store storage mode; expected storage(heap()) or storage(journaled(...))",
        )
        .with_span(path)),
        _ => Err(DarlingError::custom(
            "storage(...) requires exactly one storage mode: heap() or journaled(...)",
        )),
    }
}

fn parse_heap_config(list: &syn::MetaList) -> Result<ParsedStoreHeapConfig, DarlingError> {
    let items = NestedMeta::parse_meta_list(list.tokens.clone())?;
    if !items.is_empty() {
        return Err(
            DarlingError::custom("storage(heap()) does not accept arguments").with_span(&list.path),
        );
    }

    Ok(ParsedStoreHeapConfig)
}

fn parse_journaled_memory_config(
    list: &syn::MetaList,
) -> Result<ParsedStoreJournaledMemoryConfig, DarlingError> {
    let items = NestedMeta::parse_meta_list(list.tokens.clone())?;
    ParsedStoreJournaledMemoryConfig::from_list(&items)
}

impl HasDef for Store {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Store {
    fn validate(&self) -> Result<(), DarlingError> {
        if let Some(journaled) = self.storage.journaled()
            && !crate::validate::memory::stable_key_segment_is_canonical(&journaled.key)
        {
            return Err(DarlingError::custom(
                "store key must begin with a lowercase ASCII letter and contain only lowercase ASCII letters, digits, and underscores",
            ).with_span(&self.def.ident()));
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
        let canister = quote_one(&self.canister, to_path);
        match &self.storage {
            ParsedStoreStorage::Heap(_) => {
                quote! {
                    ::icydb_model::node::Store::new_heap(
                        #def,
                        #canister,
                        ::icydb_model::node::StoreHeapConfig::new(),
                    )
                }
            }
            ParsedStoreStorage::Journaled(journaled) => {
                let key = &journaled.key;

                quote! {
                    ::icydb_model::node::Store::new_journaled(
                        #def,
                        #canister,
                        ::icydb_model::node::StoreJournaledMemoryConfig::new(
                            #key,
                        ),
                    )
                }
            }
        }
    }
}

impl HasTraits for Store {
    fn traits(&self) -> Vec<TraitKind> {
        generated_node_trait_set().into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        let _ = t;
        None
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_store(tokens: TokenStream) -> Result<Store, DarlingError> {
        Store::from_list(&NestedMeta::parse_meta_list(tokens).unwrap())
    }

    #[test]
    fn explicit_storage_and_permanent_key_are_required() {
        assert!(parse_store(quote!(canister = "App")).is_err());
        assert!(parse_store(quote!(canister = "App", storage(journaled()))).is_err());
        assert!(
            parse_store(quote!(
                canister = "App",
                storage(journaled(key = "a", key = "b"))
            ))
            .is_err()
        );
    }

    #[test]
    fn heap_is_volatile_and_has_no_configuration() {
        let store = parse_store(quote!(canister = "App", storage(heap()))).unwrap();
        assert!(matches!(store.storage, ParsedStoreStorage::Heap(_)));
        assert!(parse_store(quote!(canister = "App", storage(heap(key = "a")))).is_err());
    }

    #[test]
    fn journaled_key_is_preserved_in_generated_metadata() {
        let store = parse_store(quote!(
            canister = "App",
            storage(journaled(key = "transfers"))
        ))
        .unwrap();
        assert_eq!(store.storage.journaled().unwrap().key, "transfers");
    }
}
