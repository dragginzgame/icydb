use crate::validate::memory::{
    app_memory_id_error, memory_id_reserved_error, stable_key_segment_is_canonical,
};
use crate::{imp::*, prelude::*};
use darling::ast::NestedMeta;
use icydb_utils::{Case, Casing};

///
/// Store
///

#[derive(Debug)]
pub struct Store {
    pub(crate) def: Def,

    pub(crate) ident: Ident,
    pub(crate) name: String,
    pub(crate) canister: Path,
    pub(crate) storage: StoreStorage,
}

#[derive(Debug)]
pub(crate) enum StoreStorage {
    Stable(StoreStableStorage),
}

impl StoreStorage {
    const fn stable(&self) -> &StoreStableStorage {
        match self {
            Self::Stable(stable) => stable,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct StoreStableStorage {
    pub(crate) data: u8,
    pub(crate) index: u8,
    pub(crate) schema: u8,
}

impl StoreStableStorage {
    const fn new(data_memory_id: u8, index_memory_id: u8, schema_memory_id: u8) -> Self {
        Self {
            data: data_memory_id,
            index: index_memory_id,
            schema: schema_memory_id,
        }
    }
}

impl FromMeta for Store {
    fn from_list(items: &[NestedMeta]) -> Result<Self, DarlingError> {
        let mut ident = None;
        let mut name = None;
        let mut canister = None;
        let mut storage = None;

        for item in items {
            match item {
                NestedMeta::Meta(syn::Meta::NameValue(name_value)) => {
                    if name_value.path.is_ident("ident") {
                        set_once(
                            &mut ident,
                            Ident::from_expr(&name_value.value)?,
                            "store(...) accepts only one ident = ... argument",
                            &name_value.path,
                        )?;
                        continue;
                    }

                    if name_value.path.is_ident("store_name") {
                        set_once(
                            &mut name,
                            String::from_expr(&name_value.value)?,
                            "store(...) accepts only one store_name = \"...\" argument",
                            &name_value.path,
                        )?;
                        continue;
                    }

                    if name_value.path.is_ident("canister") {
                        set_once(
                            &mut canister,
                            Path::from_expr(&name_value.value)?,
                            "store(...) accepts only one canister = ... argument",
                            &name_value.path,
                        )?;
                        continue;
                    }

                    if is_flat_memory_id_arg(&name_value.path) {
                        return Err(DarlingError::custom(
                            "store memory ids must be declared inside storage(stable(...))",
                        )
                        .with_span(&name_value.path));
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

        let ident = ident.ok_or_else(|| DarlingError::custom("store(...) requires ident = ..."))?;
        let name =
            name.ok_or_else(|| DarlingError::custom("store(...) requires store_name = \"...\""))?;
        let canister =
            canister.ok_or_else(|| DarlingError::custom("store(...) requires canister = ..."))?;
        let storage = storage.ok_or_else(|| {
            DarlingError::custom("store(...) requires storage(stable(...)) in 0.167")
        })?;

        Ok(Self {
            def: Def::default(),
            ident,
            name,
            canister,
            storage,
        })
    }
}

const STORE_ARGS_MESSAGE: &str = "store(...) supports ident = ..., store_name = \"...\", canister = ..., and storage(stable(...))";

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

fn is_flat_memory_id_arg(path: &syn::Path) -> bool {
    path.is_ident("data_memory_id")
        || path.is_ident("index_memory_id")
        || path.is_ident("schema_memory_id")
}

fn parse_store_storage(list: &syn::MetaList) -> Result<StoreStorage, DarlingError> {
    let items = NestedMeta::parse_meta_list(list.tokens.clone())?;
    let [item] = items.as_slice() else {
        return Err(DarlingError::custom(
            "storage(...) requires exactly one storage mode: stable(...)",
        )
        .with_span(&list.path));
    };

    match item {
        NestedMeta::Meta(syn::Meta::List(mode)) if mode.path.is_ident("stable") => {
            Ok(StoreStorage::Stable(parse_stable_storage(mode)?))
        }
        NestedMeta::Meta(syn::Meta::List(mode)) if mode.path.is_ident("heap") => {
            Err(DarlingError::custom(
                "storage(heap(...)) is reserved for a future release; use storage(stable(...))",
            )
            .with_span(&mode.path))
        }
        NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("heap") => {
            Err(DarlingError::custom(
                "storage(heap) is reserved for a future release; use storage(stable(...))",
            )
            .with_span(path))
        }
        NestedMeta::Meta(syn::Meta::List(mode)) => Err(DarlingError::custom(
            "unknown store storage mode; expected storage(stable(...))",
        )
        .with_span(&mode.path)),
        NestedMeta::Meta(syn::Meta::Path(path)) => Err(DarlingError::custom(
            "unknown store storage mode; expected storage(stable(...))",
        )
        .with_span(path)),
        _ => Err(DarlingError::custom(
            "storage(...) requires exactly one storage mode: stable(...)",
        )),
    }
}

fn parse_stable_storage(list: &syn::MetaList) -> Result<StoreStableStorage, DarlingError> {
    let items = NestedMeta::parse_meta_list(list.tokens.clone())?;
    let mut data_memory_id = None;
    let mut index_memory_id = None;
    let mut schema_memory_id = None;

    for item in items {
        match item {
            NestedMeta::Meta(syn::Meta::NameValue(name_value)) => {
                if name_value.path.is_ident("data_memory_id") {
                    set_once(
                        &mut data_memory_id,
                        u8::from_expr(&name_value.value)?,
                        "storage(stable(...)) accepts only one data_memory_id = ... argument",
                        &name_value.path,
                    )?;
                    continue;
                }

                if name_value.path.is_ident("index_memory_id") {
                    set_once(
                        &mut index_memory_id,
                        u8::from_expr(&name_value.value)?,
                        "storage(stable(...)) accepts only one index_memory_id = ... argument",
                        &name_value.path,
                    )?;
                    continue;
                }

                if name_value.path.is_ident("schema_memory_id") {
                    set_once(
                        &mut schema_memory_id,
                        u8::from_expr(&name_value.value)?,
                        "storage(stable(...)) accepts only one schema_memory_id = ... argument",
                        &name_value.path,
                    )?;
                    continue;
                }

                return Err(DarlingError::custom(
                    "storage(stable(...)) supports data_memory_id, index_memory_id, and schema_memory_id",
                )
                .with_span(&name_value.path));
            }
            NestedMeta::Meta(syn::Meta::Path(path)) => {
                return Err(DarlingError::custom(
                    "storage(stable(...)) requires named memory id arguments",
                )
                .with_span(&path));
            }
            NestedMeta::Meta(syn::Meta::List(list)) => {
                return Err(DarlingError::custom(
                    "storage(stable(...)) does not support nested storage options",
                )
                .with_span(&list.path));
            }
            _ => {
                return Err(DarlingError::custom(
                    "storage(stable(...)) supports data_memory_id, index_memory_id, and schema_memory_id",
                ));
            }
        }
    }

    let data_memory_id = data_memory_id.ok_or_else(|| {
        DarlingError::custom("storage(stable(...)) requires data_memory_id = ...")
    })?;
    let index_memory_id = index_memory_id.ok_or_else(|| {
        DarlingError::custom("storage(stable(...)) requires index_memory_id = ...")
    })?;
    let schema_memory_id = schema_memory_id.ok_or_else(|| {
        DarlingError::custom("storage(stable(...)) requires schema_memory_id = ...")
    })?;

    Ok(StoreStableStorage::new(
        data_memory_id,
        index_memory_id,
        schema_memory_id,
    ))
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
        if !stable_key_segment_is_canonical(&self.name) {
            return Err(DarlingError::custom(
                "store_name must use lowercase ASCII letters, digits, and underscores",
            )
            .with_span(&self.ident));
        }
        let stable = self.storage.stable();
        if let Some(message) = app_memory_id_error("data_memory_id", stable.data) {
            return Err(DarlingError::custom(message).with_span(&self.ident));
        }
        if let Some(message) = app_memory_id_error("index_memory_id", stable.index) {
            return Err(DarlingError::custom(message).with_span(&self.ident));
        }
        if let Some(message) = app_memory_id_error("schema_memory_id", stable.schema) {
            return Err(DarlingError::custom(message).with_span(&self.ident));
        }
        if let Some(message) = memory_id_reserved_error("data_memory_id", stable.data) {
            return Err(DarlingError::custom(message).with_span(&self.ident));
        }
        if let Some(message) = memory_id_reserved_error("index_memory_id", stable.index) {
            return Err(DarlingError::custom(message).with_span(&self.ident));
        }
        if let Some(message) = memory_id_reserved_error("schema_memory_id", stable.schema) {
            return Err(DarlingError::custom(message).with_span(&self.ident));
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
        let store_name = &self.name;
        let canister = quote_one(&self.canister, to_path);
        let stable = self.storage.stable();
        let data_memory_id = stable.data;
        let index_memory_id = stable.index;
        let schema_memory_id = stable.schema;

        // quote
        quote! {
            ::icydb::schema::node::Store::new_stable(
                #def,
                #ident,
                #store_name,
                #canister,
                ::icydb::schema::node::StoreStableMemoryConfig::new(
                    #data_memory_id,
                    #index_memory_id,
                    #schema_memory_id,
                ),
            )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn args(tokens: TokenStream) -> Vec<NestedMeta> {
        NestedMeta::parse_meta_list(tokens).expect("store args should parse")
    }

    fn parse_store(tokens: TokenStream) -> Result<Store, DarlingError> {
        Store::from_list(&args(tokens))
    }

    #[test]
    fn from_list_accepts_nested_stable_storage() {
        let store = parse_store(quote!(
            ident = "USER_STORE",
            store_name = "users",
            canister = "AppCanister",
            storage(stable(
                data_memory_id = 10,
                index_memory_id = 11,
                schema_memory_id = 12,
            ))
        ))
        .expect("nested stable storage should parse");
        let stable = store.storage.stable();

        assert_eq!(store.ident.to_string(), "USER_STORE");
        assert_eq!(store.name, "users");
        assert_eq!(stable.data, 10);
        assert_eq!(stable.index, 11);
        assert_eq!(stable.schema, 12);
    }

    #[test]
    fn from_list_rejects_missing_storage() {
        let err = parse_store(quote!(
            ident = "USER_STORE",
            store_name = "users",
            canister = "AppCanister"
        ))
        .expect_err("0.167 requires explicit store storage");

        assert!(
            err.to_string().contains("storage(stable"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn from_list_rejects_flat_memory_ids() {
        let err = parse_store(quote!(
            ident = "USER_STORE",
            store_name = "users",
            canister = "AppCanister",
            data_memory_id = 10,
            index_memory_id = 11,
            schema_memory_id = 12
        ))
        .expect_err("flat memory ids should be a hard-cut parse error");

        assert!(
            err.to_string().contains("storage(stable"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn from_list_rejects_reserved_heap_storage() {
        let err = parse_store(quote!(
            ident = "USER_STORE",
            store_name = "users",
            canister = "AppCanister",
            storage(heap())
        ))
        .expect_err("heap storage should be reserved");

        assert!(
            err.to_string().contains("reserved"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn from_list_rejects_unknown_storage_mode() {
        let err = parse_store(quote!(
            ident = "USER_STORE",
            store_name = "users",
            canister = "AppCanister",
            storage(memory())
        ))
        .expect_err("unknown storage mode should reject");

        assert!(
            err.to_string().contains("unknown store storage mode"),
            "unexpected error: {err}",
        );
    }
}
