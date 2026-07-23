//! Module: imp::inherent::entity
//! Responsibility: generated implementation tokens.
//! Does not own: runtime trait semantics.
//! Boundary: parsed nodes to impl tokens.

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
    let index_runtime_outputs = node
        .indexes
        .iter()
        .enumerate()
        .map(|(ordinal, index)| {
            index.runtime_model_tokens(node, &resolved_entity_name, &node.store, ordinal)
        })
        .collect::<Vec<_>>();
    let index_support_items = index_runtime_outputs
        .iter()
        .flat_map(|(support_items, _)| support_items.iter().cloned())
        .collect::<Vec<_>>();
    let index_exprs = index_runtime_outputs
        .iter()
        .map(|(_, model_expr)| model_expr.clone())
        .collect::<Vec<_>>();
    let check_runtime_outputs = node
        .constraints
        .iter()
        .enumerate()
        .map(|(ordinal, constraint)| constraint.runtime_model_tokens(node, ordinal))
        .collect::<Result<Vec<_>, _>>()
        .expect("validated generated check should lower");
    let check_support_items = check_runtime_outputs
        .iter()
        .flat_map(|(support_items, _)| support_items.iter().cloned())
        .collect::<Vec<_>>();
    let check_exprs = check_runtime_outputs
        .iter()
        .map(|(_, model_expr)| model_expr.clone())
        .collect::<Vec<_>>();
    let fields_len = LitInt::new(&node.fields.len().to_string(), Span::call_site());
    let indexes_len = LitInt::new(&index_exprs.len().to_string(), Span::call_site());
    let model_fields_ident = model_fields_ident(&ident);
    let primary_key_fields_ident = primary_key_fields_ident(&ident);
    let indexes_ident = indexes_ident(&ident);
    let relations_ident = relations_ident(&ident);
    let checks_ident = checks_ident(&ident);
    let relation_exprs = relation_model_exprs(node, &model_fields_ident);
    let relations_len = LitInt::new(&relation_exprs.len().to_string(), Span::call_site());
    let checks_len = LitInt::new(&check_exprs.len().to_string(), Span::call_site());
    let primary_key_field_indexes = primary_key_field_indexes(node);
    let primary_key_fields_len = LitInt::new(
        &primary_key_field_indexes.len().to_string(),
        Span::call_site(),
    );
    let primary_key_field_refs = primary_key_field_indexes
        .iter()
        .map(|index| quote!(&#model_fields_ident[#index]))
        .collect::<Vec<_>>();

    quote! {
        #(#index_support_items)*
        #(#check_support_items)*
        const #model_fields_ident:
            [::icydb::model::field::FieldModel; #fields_len] = [
                #(#model_fields_exprs),*
            ];
        const #primary_key_fields_ident:
            [&'static ::icydb::model::field::FieldModel; #primary_key_fields_len] = [
                #(#primary_key_field_refs),*
            ];
        const #indexes_ident:
            [&'static ::icydb::model::index::IndexModel; #indexes_len] = [
                #(&#index_exprs),*
            ];
        const #relations_ident:
            [::icydb::model::entity::RelationEdgeModel; #relations_len] = [
                #(#relation_exprs),*
            ];
        const #checks_ident:
            [::icydb::model::entity::CheckConstraintModel; #checks_len] = [
                #(#check_exprs),*
            ];
    }
}

fn entity_model_tokens(node: &Entity) -> TokenStream {
    let ident = node.def.ident();
    let entity_name = node
        .name
        .as_ref()
        .map_or_else(|| node.def.ident().to_string(), LitStr::value);
    let pk_index = primary_key_field_indexes(node)
        .into_iter()
        .next()
        .expect("primary key field not found in entity fields");
    let pk_index = LitInt::new(&pk_index.to_string(), Span::call_site());
    let model_fields_ident = model_fields_ident(&ident);
    let primary_key_fields_ident = primary_key_fields_ident(&ident);
    let model_ident = model_ident(&ident);
    let indexes_ident = indexes_ident(&ident);
    let relations_ident = relations_ident(&ident);
    let checks_ident = checks_ident(&ident);
    let schema_version = LitInt::new(&node.schema_version.to_string(), Span::call_site());

    quote! {
        const #model_ident: ::icydb::model::entity::EntityModel =
            ::icydb::model::entity::EntityModel::generated_with_primary_key_model_relations_and_checks(
                <#ident as ::icydb::__macro::Path>::PATH,
                #entity_name,
                #schema_version,
                ::icydb::model::entity::PrimaryKeyModel::ordered(
                    &#primary_key_fields_ident
                ),
                #pk_index,
                &#model_fields_ident,
                &#indexes_ident,
                &#relations_ident,
                &#checks_ident,
            );
    }
}

fn relation_model_exprs(node: &Entity, model_fields_ident: &Ident) -> Vec<TokenStream> {
    node.relations
        .iter()
        .map(|relation| {
            let relation_name = relation.ident.value();
            let target = quote_one(&relation.target, to_path);
            let local_field_refs = relation.fields.iter().map(|local_field| {
                let field_ident = syn::parse_str::<Ident>(local_field.value().as_str())
                    .expect("relation fields should validate as field identifiers");
                let index = node
                    .fields
                    .iter()
                    .position(|field| field.ident == field_ident)
                    .expect("relation local field should be validated before model generation");
                let index = LitInt::new(&index.to_string(), Span::call_site());

                quote!(&#model_fields_ident[#index])
            });

            quote! {
                ::icydb::model::entity::RelationEdgeModel::generated(
                    #relation_name,
                    #target,
                    &[#(#local_field_refs),*],
                )
            }
        })
        .collect()
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

fn relations_ident(ident: &Ident) -> Ident {
    let ident = ident.to_string().to_ascii_uppercase();
    format_ident!("__{}_ENTITY_RELATIONS", ident)
}

fn checks_ident(ident: &Ident) -> Ident {
    let ident = ident.to_string().to_ascii_uppercase();
    format_ident!("__{}_ENTITY_CHECK_CONSTRAINTS", ident)
}

fn primary_key_fields_ident(ident: &Ident) -> Ident {
    let ident = ident.to_string().to_ascii_uppercase();
    format_ident!("__{}_PRIMARY_KEY_FIELDS", ident)
}

fn primary_key_field_indexes(node: &Entity) -> Vec<LitInt> {
    node.primary_key
        .fields()
        .iter()
        .map(|primary_key_field| {
            let index = node
                .fields
                .iter()
                .position(|field| field.ident == *primary_key_field)
                .expect("primary key field not found in entity fields");

            LitInt::new(&index.to_string(), Span::call_site())
        })
        .collect()
}

fn model_ident(ident: &Ident) -> Ident {
    let ident = ident.to_string().to_ascii_uppercase();
    format_ident!("__{}_ENTITY_MODEL", ident)
}
