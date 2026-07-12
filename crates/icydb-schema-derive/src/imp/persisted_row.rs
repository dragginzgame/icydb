//! Module: imp::persisted_row
//! Responsibility: generated implementation tokens.
//! Does not own: runtime trait semantics.
//! Boundary: parsed nodes to impl tokens.

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

            // Generated PersistedRow is a model/proposal bridge. Accepted
            // runtime row decode uses StructuralRowContract defaults from the
            // accepted schema snapshot instead of this Rust construction path.
            let missing_expr = if field.default.is_some() {
                let expr = field
                    .rust_default_expr()
                    .expect("schema default should also be a Rust construction value");
                quote!(#expr)
            } else if field.write_management.is_some() {
                quote!(Default::default())
            } else {
                match field.value.cardinality() {
                    Cardinality::Opt => quote!(None),
                    Cardinality::One | Cardinality::Many => quote! {
                        return Err(::icydb::__macro::InternalError::missing_persisted_slot(#field_name))
                    },
                }
            };
            let field_ty = field.value.type_expr();

            quote! {
                #ident: match slots.get_value(#slot)? {
                    Some(value) => {
                        ::icydb::__macro::decode_generated_runtime_field_value::<#field_ty>(
                            &value,
                            slots.runtime_enum_context(),
                            #field_name,
                        )
                        ?
                    }
                    None => #missing_expr,
                }
            }
        });

        let impl_tokens = Implementor::new(node.def(), TraitKind::PersistedRow)
            .set_tokens(quote! {
                fn materialize_from_slots(
                    slots: &mut dyn ::icydb::__macro::SlotReader,
                ) -> Result<Self, ::icydb::__macro::InternalError> {
                    Ok(Self {
                        #(#field_materializers),*
                    })
                }
            })
            .to_token_stream();

        let projection_arms = node.fields.iter().enumerate().map(|(slot, field)| {
            let slot = syn::Index::from(slot);
            let ident = &field.ident;
            if field.value.item.is.is_some() {
                return quote!(#slot => None);
            }
            let value = match field.value.cardinality() {
                Cardinality::One => quote! {
                    Some(::icydb::__macro::runtime_value_to_value(&self.#ident))
                },
                Cardinality::Opt => quote! {
                    Some(match self.#ident.as_ref() {
                        Some(value) => ::icydb::__macro::runtime_value_to_value(value),
                        None => ::icydb::__macro::Value::Null,
                    })
                },
                Cardinality::Many => quote! {
                    Some(::icydb::__macro::Value::List(
                        self.#ident
                            .iter()
                            .map(::icydb::__macro::runtime_value_to_value)
                            .collect(),
                    ))
                },
            };
            quote!(#slot => #value)
        });
        let ident = node.def.ident();
        let projection_tokens = quote! {
            impl ::icydb::__macro::FieldProjection for #ident {
                fn get_value_by_index(
                    &self,
                    index: usize,
                ) -> Option<::icydb::__macro::Value> {
                    match index {
                        #(#projection_arms),*,
                        _ => None,
                    }
                }
            }
        };

        let tokens = quote! {
            #(#field_codec_assertions)*
            #impl_tokens
            #projection_tokens
        };

        Some(TraitStrategy::from_impl(tokens))
    }
}

// Emit one field-local trait assertion so schema-derived persisted rows fail
// with the owning storage contract name instead of a generic bound mismatch.
fn persisted_field_codec_assertion(field: &Field) -> TokenStream {
    let field_ident = &field.ident;
    let field_ty = field.value.type_expr();

    emit_persisted_trait_assertion(
        field_ident,
        quote!(::icydb::__macro::RuntimeValueDecode),
        field_ty,
        "RUNTIME_VALUE_DECODE",
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
