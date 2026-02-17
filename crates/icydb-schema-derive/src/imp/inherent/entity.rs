use crate::{
    imp::inherent::{
        InherentTrait, model::model_kind_from_value, relation::relation_accessor_tokens,
    },
    prelude::*,
};
use canic_utils::case::{Case, Casing};
use syn::LitInt;

///
/// Entity
///

impl Imp<Entity> for InherentTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        // Emit query-facing field references.
        let field_consts: Vec<TokenStream> = node
            .fields
            .iter()
            .map(|field| {
                let constant = field.ident.to_string().to_case(Case::Constant);
                let ident = format_ident!("{constant}");
                let name_str = field.ident.to_string();

                quote! {
                    pub const #ident: ::icydb::db::query::FieldRef =
                        ::icydb::db::query::FieldRef::new(#name_str);
                }
            })
            .collect();

        // Emit static model field descriptors.
        let model_field_idents = node
            .fields
            .iter()
            .map(model_field_ident)
            .collect::<Vec<_>>();
        let model_field_consts: Vec<TokenStream> = node
            .fields
            .iter()
            .zip(model_field_idents.iter())
            .map(|(field, ident)| {
                let name = field.ident.to_string();
                let kind = model_kind_from_value(&field.value);

                quote! {
                    const #ident: ::icydb::model::field::FieldModel =
                        ::icydb::model::field::FieldModel {
                            name: #name,
                            kind: #kind,
                        };
                }
            })
            .collect();

        // Build a static entity model and primary-key pointer.
        let fields_len = LitInt::new(&node.fields.len().to_string(), Span::call_site());
        let pk_index = node
            .fields
            .iter()
            .position(|field| field.ident == node.primary_key.field)
            .expect("primary key field not found in entity fields");
        let pk_index = LitInt::new(&pk_index.to_string(), Span::call_site());

        let model_fields_ident = format_ident!("__MODEL_FIELDS");
        let model_ident = format_ident!("__ENTITY_MODEL");
        let model_fields = quote! {
            const #model_fields_ident:
                [::icydb::model::field::FieldModel; #fields_len] = [
                    #( Self::#model_field_idents ),*
                ];
        };
        let entity_model = quote! {
            const #model_ident: ::icydb::model::entity::EntityModel =
                ::icydb::model::entity::EntityModel {
                    path: <Self as ::icydb::traits::Path>::PATH,
                    entity_name: <Self as ::icydb::traits::EntityIdentity>::ENTITY_NAME,
                    primary_key: &Self::#model_fields_ident[#pk_index],
                    fields: &Self::#model_fields_ident,
                    indexes: <Self as ::icydb::traits::EntitySchema>::INDEXES,
                };
        };

        // Emit typed relation ID accessors for relation-backed fields.
        let relation_accessors = relation_accessor_tokens(node.fields.iter());

        let tokens = quote! {
            #(#field_consts)*
            #(#model_field_consts)*
            #model_fields
            #entity_model
            #(#relation_accessors)*
        };

        let impl_tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(impl_tokens))
    }
}

fn model_field_ident(field: &Field) -> Ident {
    let constant = field.ident.to_string().to_case(Case::Constant);
    format_ident!("__MODEL_FIELD_{constant}")
}
