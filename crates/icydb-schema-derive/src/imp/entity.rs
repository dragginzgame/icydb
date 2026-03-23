use crate::prelude::*;

///
/// EntityKindTrait
///

pub struct EntityKindTrait {}

impl Imp<Entity> for EntityKindTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let pk_entry = node
            .fields
            .get(&node.primary_key.field)
            .expect("primary key field must be validated before derive generation");

        // PK key shape must always follow the declared field type.
        let pk_key_type = pk_entry.value.item.type_expr();
        let store = &node.store;
        let resolved_entity_name = resolved_entity_name(node);
        let relation_key_type_assertions = relation_key_type_assertions(node);
        let ident = node.def.ident();

        Some(TraitStrategy::from_impl(entity_kind_strategy_tokens(
            node,
            pk_entry,
            &pk_key_type,
            store,
            &resolved_entity_name,
            &relation_key_type_assertions,
            &ident,
        )))
    }
}

fn entity_kind_strategy_tokens(
    node: &Entity,
    pk_entry: &Field,
    pk_key_type: &TokenStream,
    store: &Path,
    resolved_entity_name: &str,
    relation_key_type_assertions: &[TokenStream],
    ident: &Ident,
) -> TokenStream {
    let mut tokens = TokenStream::new();
    tokens.extend(entity_key_impl_tokens(ident, pk_key_type));
    tokens.extend(entity_schema_impl_tokens(node, resolved_entity_name, store));
    tokens.extend(entity_placement_impl_tokens(&node.def, store));
    tokens.extend(entity_kind_impl_tokens(&node.def, resolved_entity_name));
    tokens.extend(quote! {
        #(#relation_key_type_assertions)*
    });
    tokens.extend(model_consistency_test_tokens(ident));

    if let Some(singleton) = singleton_entity_tokens(pk_entry, ident) {
        tokens.extend(singleton);
    }

    tokens
}

fn entity_key_impl_tokens(ident: &Ident, pk_key_type: &TokenStream) -> TokenStream {
    quote! {
        impl ::icydb::traits::EntityKey for #ident {
            type Key = #pk_key_type;
        }
    }
}

fn entity_schema_impl_tokens(
    node: &Entity,
    resolved_entity_name: &str,
    _store: &Path,
) -> TokenStream {
    let model_ident = entity_model_ident(&node.def.ident());

    Implementor::new(&node.def, TraitKind::EntitySchema)
        .set_tokens(quote! {
            const NAME: &'static str = #resolved_entity_name;
            const MODEL: &'static ::icydb::model::entity::EntityModel =
                &#model_ident;
        })
        .to_token_stream()
}

fn entity_placement_impl_tokens(def: &Def, store: &Path) -> TokenStream {
    Implementor::new(def, TraitKind::EntityPlacement)
        .set_tokens(quote! {
            type Store = #store;
            type Canister =
                <Self::Store as ::icydb::traits::StoreKind>::Canister;
        })
        .to_token_stream()
}

fn entity_kind_impl_tokens(def: &Def, resolved_entity_name: &str) -> TokenStream {
    let entity_tag = entity_tag_for_name(resolved_entity_name);

    Implementor::new(def, TraitKind::EntityKind)
        .set_tokens(quote! {
            const ENTITY_TAG: ::icydb::types::EntityTag = {
                const RAW_ENTITY_TAG: u64 = #entity_tag;
                ::icydb::types::EntityTag::new(RAW_ENTITY_TAG)
            };
        })
        .to_token_stream()
}

fn singleton_entity_tokens(pk_entry: &Field, ident: &Ident) -> Option<TokenStream> {
    if matches!(
        pk_entry.value.item.target(),
        ItemTarget::Primitive(Primitive::Unit)
    ) {
        Some(quote! {
            impl ::icydb::traits::SingletonEntity for #ident {}
        })
    } else {
        None
    }
}

fn resolved_entity_name(node: &Entity) -> String {
    node.name
        .as_ref()
        .map_or_else(|| node.def.ident().to_string(), LitStr::value)
}

fn model_consistency_test_tokens(ident: &Ident) -> TokenStream {
    let test_mod = format_ident!("__entity_model_test_{ident}");

    quote! {
        #[cfg(test)]
        mod #test_mod {
            use super::*;

            #[test]
            fn model_consistency() {
                let model = <#ident as ::icydb::traits::EntitySchema>::MODEL;

                for field in model.fields() {
                    assert!(
                        !field.name().is_empty(),
                        "generated runtime field names must not be empty",
                    );
                }

                assert!(model
                    .fields()
                    .iter()
                    .any(|field| ::core::ptr::eq(field, model.primary_key())));
            }
        }
    }
}

fn entity_tag_for_name(name: &str) -> u64 {
    const FNV1A_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV1A_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV1A_OFFSET_BASIS;
    for byte in name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV1A_PRIME);
    }

    hash
}

fn relation_key_type_assertions(node: &Entity) -> Vec<TokenStream> {
    node.fields
        .iter()
        .filter_map(|field| {
            let relation = field.value.item.relation.as_ref()?;
            let key_ty = field.value.item.type_expr();

            Some(quote! {
                // Keep relation storage key shape aligned with the related entity key type.
                const _: fn(<#relation as ::icydb::traits::EntityKey>::Key) -> #key_ty = |key| key;
                const _: fn(#key_ty) -> <#relation as ::icydb::traits::EntityKey>::Key = |key| key;
            })
        })
        .collect()
}

fn entity_model_ident(ident: &Ident) -> Ident {
    let ident = ident.to_string().to_ascii_uppercase();
    format_ident!("__{}_ENTITY_MODEL", ident)
}

///
/// EntityValueTrait
///

pub struct EntityValueTrait {}

impl Imp<Entity> for EntityValueTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let pk_ident = &node.primary_key.field;

        let tokens = Implementor::new(&node.def, TraitKind::EntityValue)
            .set_tokens(quote! {
                fn id(&self) -> ::icydb::types::Id<Self> {
                    ::icydb::types::Id::from_key(self.#pk_ident)
                }
            })
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
