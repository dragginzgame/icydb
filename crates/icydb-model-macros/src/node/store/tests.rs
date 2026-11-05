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
