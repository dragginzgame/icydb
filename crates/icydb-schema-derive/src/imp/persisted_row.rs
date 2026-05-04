use crate::prelude::*;

///
/// PersistedRowTrait
///

pub struct PersistedRowTrait {}

impl Imp<Entity> for PersistedRowTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let field_codec_assertions = node.fields.iter().map(persisted_field_codec_assertion);

        let field_materializers = node.fields.iter().enumerate().map(|(slot, field)| {
            let slot = syn::Index::from(slot);
            let ident = &field.ident;
            let field_name = ident.to_string();

            let missing_expr = if field.default.is_some() {
                let expr = field.default_expr();
                quote!(#expr)
            } else if field.write_management.is_some() {
                quote!(Default::default())
            } else {
                match field.value.cardinality() {
                    Cardinality::Opt => quote!(None),
                    Cardinality::One | Cardinality::Many => quote! {
                        return Err(::icydb::db::InternalError::missing_persisted_slot(#field_name))
                    },
                }
            };
            let decode_expr = persisted_field_decode_expr(field, field_name.as_str());

            quote! {
                #ident: match slots.get_bytes(#slot) {
                    Some(bytes) => #decode_expr,
                    None => #missing_expr,
                }
            }
        });

        let slot_writes = node.fields.iter().enumerate().map(|(slot, field)| {
            let slot = syn::Index::from(slot);
            let ident = &field.ident;
            let field_name = ident.to_string();
            let encode_expr =
                persisted_field_encode_expr(field, quote!(&self.#ident), field_name.as_str());

            quote! {
                let payload = #encode_expr;
                out.write_slot(#slot, Some(payload.as_slice()))?;
            }
        });

        let impl_tokens = Implementor::new(node.def(), TraitKind::PersistedRow)
            .set_tokens(quote! {
                fn materialize_from_slots(
                    slots: &mut dyn ::icydb::db::SlotReader,
                ) -> Result<Self, ::icydb::db::InternalError> {
                    Ok(Self {
                        #(#field_materializers),*
                    })
                }

                fn write_slots(
                    &self,
                    out: &mut dyn ::icydb::db::SlotWriter,
                ) -> Result<(), ::icydb::db::InternalError> {
                    #(#slot_writes)*

                    Ok(())
                }
            })
            .to_token_stream();

        let tokens = quote! {
            #(#field_codec_assertions)*
            #impl_tokens
        };

        Some(TraitStrategy::from_impl(tokens))
    }
}

// Emit one field-local trait assertion so schema-derived persisted rows fail
// with the owning storage contract name instead of a generic bound mismatch.
fn persisted_field_codec_assertion(field: &Field) -> TokenStream {
    let field_ident = &field.ident;

    if field.value.item.is.is_some() {
        return emit_persisted_trait_assertion(
            field_ident,
            quote!(::icydb::__macro::PersistedFieldMetaCodec),
            field.value.item.type_expr(),
            "PERSISTED_FIELD_META_CODEC",
        );
    }

    let field_ty = persisted_field_slot_asserted_type(field);

    emit_persisted_trait_assertion(
        field_ident,
        quote!(::icydb::__macro::PersistedFieldSlotCodec),
        field_ty,
        "PERSISTED_FIELD_SLOT_CODEC",
    )
}

// Generate a descriptive compile-time assertion symbol for one schema field so
// trait failures point at the persisted storage lane that owns the field.
fn emit_persisted_trait_assertion(
    field_ident: &syn::Ident,
    trait_path: TokenStream,
    asserted_ty: TokenStream,
    trait_label: &str,
) -> TokenStream {
    let assert_ident = format_ident!(
        "__ICYDB_FIELD_{}_MUST_IMPLEMENT_{}_TO_BE_STORED",
        field_ident.to_string().to_ascii_uppercase(),
        trait_label,
    );

    quote! {
        const _: () = {
            fn #assert_ident<T: #trait_path>() {}
            let _ = #assert_ident::<#asserted_ty>;
        };
    }
}

fn persisted_item_field_decode_expr(field: &Field, field_name: &str) -> TokenStream {
    match field.value.cardinality() {
        Cardinality::One => {
            let field_ty = field.value.type_expr();
            quote!(
                ::icydb::__macro::decode_persisted_slot_payload_by_meta::<#field_ty>(
                    bytes,
                    #field_name,
                )?
            )
        }
        Cardinality::Opt => {
            let item_ty = field.value.item.type_expr();
            quote!(
                ::icydb::__macro::decode_persisted_option_slot_payload_by_meta::<#item_ty>(
                    bytes,
                    #field_name,
                )?
            )
        }
        Cardinality::Many => {
            let item_ty = field.value.item.type_expr();
            quote!(
                ::icydb::__macro::decode_persisted_many_slot_payload_by_meta::<#item_ty>(
                    bytes,
                    #field_name,
                )?
            )
        }
    }
}

fn persisted_item_field_encode_expr(
    field: &Field,
    field_expr: TokenStream,
    field_name: &str,
) -> TokenStream {
    match field.value.cardinality() {
        Cardinality::One => quote!(
            ::icydb::__macro::encode_persisted_slot_payload_by_meta(
                #field_expr,
                #field_name,
            )?
        ),
        Cardinality::Opt => quote!(
            ::icydb::__macro::encode_persisted_option_slot_payload_by_meta(
                #field_expr,
                #field_name,
            )?
        ),
        Cardinality::Many => quote!(
            ::icydb::__macro::encode_persisted_many_slot_payload_by_meta(
                (#field_expr).as_slice(),
                #field_name,
            )?
        ),
    }
}

fn persisted_field_decode_expr(field: &Field, field_name: &str) -> TokenStream {
    if field.value.item.is.is_some() {
        return persisted_item_field_decode_expr(field, field_name);
    }

    match field.value.cardinality() {
        Cardinality::Opt => {
            let item_ty = field.value.item.type_expr();
            quote!(
                <#item_ty as ::icydb::__macro::PersistedFieldSlotCodec>::decode_persisted_option_slot(
                    bytes,
                    #field_name,
                )?
            )
        }
        Cardinality::One | Cardinality::Many => {
            let field_ty = field.value.type_expr();
            quote!(
                <#field_ty as ::icydb::__macro::PersistedFieldSlotCodec>::decode_persisted_slot(
                    bytes,
                    #field_name,
                )?
            )
        }
    }
}

fn persisted_field_encode_expr(
    field: &Field,
    field_expr: TokenStream,
    field_name: &str,
) -> TokenStream {
    if field.value.item.is.is_some() {
        return persisted_item_field_encode_expr(field, field_expr, field_name);
    }

    match field.value.cardinality() {
        Cardinality::Opt => {
            let item_ty = field.value.item.type_expr();
            quote!(
                <#item_ty as ::icydb::__macro::PersistedFieldSlotCodec>::encode_persisted_option_slot(
                    #field_expr,
                    #field_name,
                )?
            )
        }
        Cardinality::One | Cardinality::Many => {
            let field_ty = field.value.type_expr();
            quote!(
                <#field_ty as ::icydb::__macro::PersistedFieldSlotCodec>::encode_persisted_slot(
                    #field_expr,
                    #field_name,
                )?
            )
        }
    }
}

fn persisted_field_slot_asserted_type(field: &Field) -> TokenStream {
    match field.value.cardinality() {
        Cardinality::Opt => field.value.item.type_expr(),
        Cardinality::One | Cardinality::Many => field.value.type_expr(),
    }
}
