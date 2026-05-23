use crate::prelude::*;

///
/// EntityKindTrait
///

pub struct EntityKindTrait {}

impl Imp<Entity> for EntityKindTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        // PK key shape must always follow the declared field type.
        let pk_key_type = primary_key_type_expr(node);
        let store = &node.store;
        let resolved_entity_name = resolved_entity_name(node);
        let relation_key_type_assertions = relation_key_type_assertions(node);
        let ident = node.def.ident();

        Some(TraitStrategy::from_impl(entity_kind_strategy_tokens(
            node,
            &pk_key_type,
            store,
            &resolved_entity_name,
            &relation_key_type_assertions,
            &ident,
        )))
    }
}

fn primary_key_type_expr(node: &Entity) -> TokenStream {
    if node.primary_key.fields().len() == 1 {
        return node
            .fields
            .get(node.primary_key.scalar_field())
            .expect("primary key field must be validated before derive generation")
            .value
            .item
            .type_expr();
    }

    let ident = node.def.ident();
    let key_ident = format_ident!("{ident}Key");

    quote!(#key_ident)
}

fn entity_kind_strategy_tokens(
    node: &Entity,
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
    tokens.extend(model_consistency_test_tokens(node, ident));

    if let Some(singleton) = singleton_entity_tokens(node, ident) {
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

fn singleton_entity_tokens(node: &Entity, ident: &Ident) -> Option<TokenStream> {
    if node.primary_key.fields().len() != 1 {
        return None;
    }

    let pk_entry = node
        .fields
        .get(node.primary_key.scalar_field())
        .expect("primary key field must be validated before derive generation");
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

fn model_consistency_test_tokens(node: &Entity, ident: &Ident) -> TokenStream {
    let test_mod = format_ident!("__entity_model_test_{ident}");
    let primary_key_len = node.primary_key.fields().len();
    let primary_key_len_lit = syn::LitInt::new(&primary_key_len.to_string(), Span::call_site());
    let scalar_assertion = if primary_key_len == 1 {
        quote! {
            assert!(model.primary_key_model().is_scalar());
            assert!(model
                .primary_key_model()
                .fields()
                .iter()
                .any(|field| ::core::ptr::eq(field, model.primary_key())));
        }
    } else {
        quote! {
            assert!(!model.primary_key_model().is_scalar());
        }
    };

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
                assert_eq!(
                    model.primary_key_model().len(),
                    #primary_key_len_lit,
                    "generated entities should expose the declared primary-key field count",
                );
                #scalar_assertion
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
        let key_expr = entity_value_key_expr(node);

        let tokens = Implementor::new(&node.def, TraitKind::EntityValue)
            .set_tokens(quote! {
                fn id(&self) -> ::icydb::types::Id<Self> {
                    ::icydb::types::Id::from_key(#key_expr)
                }
            })
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

fn entity_value_key_expr(node: &Entity) -> TokenStream {
    if node.primary_key.fields().len() == 1 {
        let pk_ident = node.primary_key.scalar_field();
        return quote!(self.#pk_ident);
    }

    let ident = node.def.ident();
    let key_ident = format_ident!("{ident}Key");
    let fields = node.primary_key.fields().iter().map(|field| {
        quote! {
            #field: self.#field
        }
    });

    quote! {
        #key_ident {
            #(#fields),*
        }
    }
}
