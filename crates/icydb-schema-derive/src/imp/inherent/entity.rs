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
        Some(TraitStrategy::from_impl(entity_model_storage_tokens(node)))
    }
}

fn entity_model_storage_tokens(node: &Entity) -> TokenStream {
    let model_storage = model_storage_tokens(node);
    let entity_model = entity_model_tokens(node);
    let field_name_consts = field_name_const_tokens(node);

    quote! {
        #model_storage
        #entity_model
        #field_name_consts
    }
}

fn model_storage_tokens(node: &Entity) -> TokenStream {
    let ident = node.def.ident();
    let model_fields_exprs: Vec<TokenStream> = node.fields.iter().map(model_field_expr).collect();
    let resolved_entity_name = node
        .name
        .as_ref()
        .map_or_else(|| node.def.ident().to_string(), LitStr::value);
    let index_parts = node
        .indexes
        .iter()
        .enumerate()
        .map(|(ordinal, index)| {
            index.runtime_part(node, &resolved_entity_name, &node.store, ordinal)
        })
        .collect::<Vec<_>>();
    let index_support_items = index_parts
        .iter()
        .flat_map(|(support_items, _)| support_items.iter().cloned())
        .collect::<Vec<_>>();
    let index_exprs = index_parts
        .iter()
        .map(|(_, model_expr)| model_expr.clone())
        .collect::<Vec<_>>();
    let fields_len = LitInt::new(&node.fields.len().to_string(), Span::call_site());
    let indexes_len = LitInt::new(&index_exprs.len().to_string(), Span::call_site());
    let model_fields_ident = model_fields_ident(&ident);
    let indexes_ident = indexes_ident(&ident);

    quote! {
        #(#index_support_items)*
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
    let ident = node.def.ident();
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
    let model_fields_ident = model_fields_ident(&ident);
    let model_ident = model_ident(&ident);
    let indexes_ident = indexes_ident(&ident);

    quote! {
        const #model_ident: ::icydb::model::entity::EntityModel =
            ::icydb::model::entity::EntityModel::generated(
                <#ident as ::icydb::traits::Path>::PATH,
                #entity_name,
                &#model_fields_ident[#pk_index],
                #pk_index,
                &#model_fields_ident,
                &#indexes_ident,
            );
    }
}

fn field_name_const_tokens(node: &Entity) -> TokenStream {
    let field_consts = node.fields.iter().map(|field| {
        let const_ident = field.const_ident();
        let field_name = field.ident.to_string();

        quote! {
            pub const #const_ident: &'static str = #field_name;
        }
    });

    Implementor::new(&node.def, TraitKind::Inherent)
        .set_tokens(quote! {
            #(#field_consts)*
        })
        .to_token_stream()
}

fn model_fields_ident(ident: &Ident) -> Ident {
    let ident = ident.to_string().to_ascii_uppercase();
    format_ident!("__{}_MODEL_FIELDS", ident)
}

fn indexes_ident(ident: &Ident) -> Ident {
    let ident = ident.to_string().to_ascii_uppercase();
    format_ident!("__{}_ENTITY_INDEXES", ident)
}

fn model_ident(ident: &Ident) -> Ident {
    let ident = ident.to_string().to_ascii_uppercase();
    format_ident!("__{}_ENTITY_MODEL", ident)
}
