use crate::{
    imp::inherent::{InherentTrait, model::model_field_expr},
    prelude::*,
};
use syn::LitInt;

///
/// Entity
///

impl Imp<Entity> for InherentTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let impl_tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(entity_inherent_tokens(node))
            .to_token_stream();

        Some(TraitStrategy::from_impl(impl_tokens))
    }
}

fn entity_inherent_tokens(node: &Entity) -> TokenStream {
    let model_storage = model_storage_tokens(node);
    let entity_model = entity_model_tokens(node);

    quote! {
        #model_storage
        #entity_model
    }
}

fn model_storage_tokens(node: &Entity) -> TokenStream {
    let model_fields_exprs: Vec<TokenStream> = node.fields.iter().map(model_field_expr).collect();
    let resolved_entity_name = node
        .name
        .as_ref()
        .map_or_else(|| node.def.ident().to_string(), LitStr::value);
    let index_exprs = node
        .indexes
        .iter()
        .enumerate()
        .map(|(ordinal, index)| index.runtime_part(&resolved_entity_name, &node.store, ordinal))
        .collect::<Vec<_>>();
    let fields_len = LitInt::new(&node.fields.len().to_string(), Span::call_site());
    let indexes_len = LitInt::new(&index_exprs.len().to_string(), Span::call_site());
    let model_fields_ident = format_ident!("__MODEL_FIELDS");
    let indexes_ident = format_ident!("__ENTITY_INDEXES");

    quote! {
        const #model_fields_ident:
            [::icydb::model::field::FieldModel; #fields_len] = [
                #(#model_fields_exprs),*
            ];
        const #indexes_ident:
            [&'static ::icydb::model::index::IndexModel; #indexes_len] = [
                #(&#index_exprs),*
            ];
    }
}

fn entity_model_tokens(node: &Entity) -> TokenStream {
    let entity_name = node
        .name
        .as_ref()
        .map_or_else(|| node.def.ident().to_string(), LitStr::value);
    let pk_index = node
        .fields
        .iter()
        .position(|field| field.ident == node.primary_key.field)
        .expect("primary key field not found in entity fields");
    let pk_index = LitInt::new(&pk_index.to_string(), Span::call_site());
    let model_fields_ident = format_ident!("__MODEL_FIELDS");
    let model_ident = format_ident!("__ENTITY_MODEL");
    let indexes_ident = format_ident!("__ENTITY_INDEXES");

    quote! {
        const #model_ident: ::icydb::model::entity::EntityModel =
            ::icydb::model::entity::EntityModel::new(
                <Self as ::icydb::traits::Path>::PATH,
                #entity_name,
                &Self::#model_fields_ident[#pk_index],
                &Self::#model_fields_ident,
                &Self::#indexes_ident,
            );
    }
}
