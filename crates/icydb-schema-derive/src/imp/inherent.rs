use crate::prelude::*;
use canic_utils::case::{Case, Casing};
use syn::LitInt;

///
/// InherentTrait
///

pub struct InherentTrait {}

///
/// Entity
///

impl Imp<Entity> for InherentTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        // emit typed field consts
        let field_consts: Vec<TokenStream> = node
            .fields
            .iter()
            .map(|f| {
                let constant = &f.ident.to_string().to_case(Case::Constant);
                let ident = format_ident!("{constant}");
                let name_str = f.ident.to_string();

                // zero-cost field wrapper for predicate helpers
                quote! {
                    pub const #ident: ::icydb::db::query::FieldRef =
                        ::icydb::db::query::FieldRef::new(#name_str);
                }
            })
            .collect();

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
                    const #ident: ::icydb::model::field::EntityFieldModel =
                        ::icydb::model::field::EntityFieldModel {
                            name: #name,
                            kind: #kind,
                        };
                }
            })
            .collect();

        let fields_len = LitInt::new(&node.fields.len().to_string(), Span::call_site());
        let pk_index = node
            .fields
            .iter()
            .position(|field| field.ident == node.primary_key)
            .expect("primary key field not found in entity fields");
        let pk_index = LitInt::new(&pk_index.to_string(), Span::call_site());

        let model_fields_ident = format_ident!("__MODEL_FIELDS");
        let model_ident = format_ident!("__ENTITY_MODEL");

        let model_fields = quote! {
            const #model_fields_ident:
                [::icydb::model::field::EntityFieldModel; #fields_len] = [
                    #( Self::#model_field_idents ),*
                ];
        };

        let entity_model = quote! {
            const #model_ident: ::icydb::model::entity::EntityModel =
                ::icydb::model::entity::EntityModel {
                    path: <Self as ::icydb::traits::Path>::PATH,
                    entity_name: <Self as ::icydb::traits::EntityKind>::ENTITY_NAME,
                    primary_key: &Self::#model_fields_ident[#pk_index],
                    fields: &Self::#model_fields_ident,
                    indexes: <Self as ::icydb::traits::EntityKind>::INDEXES,
                };
        };

        let tokens = quote! {
            #(#field_consts)*
            #(#model_field_consts)*
            #model_fields
            #entity_model
        };

        // IMPORTANT: pass Trait::Inherent so Implementor will do `impl Entity { … }`
        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<Enum> for InherentTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let kind = quote!(::icydb::model::field::EntityFieldKind::Enum);
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::EntityFieldKind = #kind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<Newtype> for InherentTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let kind = model_kind_from_item(&node.item);
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::EntityFieldKind = #kind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<List> for InherentTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let ident = node.def.ident();
        let item = node.item.type_expr();
        let item_kind = model_kind_from_item(&node.item);
        let kind = quote!(::icydb::model::field::EntityFieldKind::List(&#item_kind));
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::EntityFieldKind = #kind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        let collection_impl = quote! {
            impl ::icydb::traits::CollectionValue for #ident {
                type Item = #item;

                fn iter(&self) -> impl ::std::iter::Iterator<Item = &Self::Item> {
                    self.0.iter()
                }

                fn len(&self) -> usize {
                    self.0.len()
                }
            }
        };

        Some(TraitStrategy::from_impl(
            quote! { #tokens #collection_impl },
        ))
    }
}

impl Imp<Set> for InherentTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let ident = node.def.ident();
        let item = node.item.type_expr();
        let item_kind = model_kind_from_item(&node.item);
        let kind = quote!(::icydb::model::field::EntityFieldKind::Set(&#item_kind));
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::EntityFieldKind = #kind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        let collection_impl = quote! {
            impl ::icydb::traits::CollectionValue for #ident {
                type Item = #item;

                fn iter(&self) -> impl ::std::iter::Iterator<Item = &Self::Item> {
                    self.0.iter()
                }

                fn len(&self) -> usize {
                    self.0.len()
                }
            }
        };

        Some(TraitStrategy::from_impl(
            quote! { #tokens #collection_impl },
        ))
    }
}

impl Imp<Map> for InherentTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key_kind = model_kind_from_item(&node.key);
        let value_kind = model_kind_from_value(&node.value);
        let kind = quote! {
            ::icydb::model::field::EntityFieldKind::Map {
                key: &#key_kind,
                value: &#value_kind,
            }
        };
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::EntityFieldKind = #kind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<Record> for InherentTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let kind = quote!(::icydb::model::field::EntityFieldKind::Unsupported);
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::EntityFieldKind = #kind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<Tuple> for InherentTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let kind = quote!(::icydb::model::field::EntityFieldKind::Unsupported);
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::EntityFieldKind = #kind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

fn model_field_ident(field: &Field) -> Ident {
    let constant = field.ident.to_string().to_case(Case::Constant);
    format_ident!("__MODEL_FIELD_{constant}")
}

fn model_kind_from_value(value: &Value) -> TokenStream {
    let base = if let Some(relation) = value.item.relation.as_ref() {
        let key_kind = model_kind_from_item(&value.item);
        let target_path = quote_one(relation, to_path);
        quote! {
            ::icydb::model::field::EntityFieldKind::Ref {
                target_path: #target_path,
                key_kind: &#key_kind,
            }
        }
    } else {
        model_kind_from_item(&value.item)
    };
    match value.cardinality() {
        Cardinality::Many => {
            quote!(::icydb::model::field::EntityFieldKind::List(&#base))
        }
        Cardinality::One | Cardinality::Opt => base,
    }
}

fn model_kind_from_item(item: &Item) -> TokenStream {
    match item.target() {
        ItemTarget::Primitive(prim) => model_kind_from_primitive(prim),
        ItemTarget::Is(path) => quote!(#path::KIND),
    }
}

fn model_kind_from_primitive(prim: Primitive) -> TokenStream {
    match prim {
        Primitive::Account => quote!(::icydb::model::field::EntityFieldKind::Account),
        Primitive::Blob => quote!(::icydb::model::field::EntityFieldKind::Blob),
        Primitive::Bool => quote!(::icydb::model::field::EntityFieldKind::Bool),
        Primitive::Date => quote!(::icydb::model::field::EntityFieldKind::Date),
        Primitive::Decimal => quote!(::icydb::model::field::EntityFieldKind::Decimal),
        Primitive::Duration => quote!(::icydb::model::field::EntityFieldKind::Duration),
        Primitive::E8s => quote!(::icydb::model::field::EntityFieldKind::E8s),
        Primitive::E18s => quote!(::icydb::model::field::EntityFieldKind::E18s),
        Primitive::Float32 => quote!(::icydb::model::field::EntityFieldKind::Float32),
        Primitive::Float64 => quote!(::icydb::model::field::EntityFieldKind::Float64),
        Primitive::Int => quote!(::icydb::model::field::EntityFieldKind::IntBig),
        Primitive::Int8 | Primitive::Int16 | Primitive::Int32 | Primitive::Int64 => {
            quote!(::icydb::model::field::EntityFieldKind::Int)
        }
        Primitive::Int128 => quote!(::icydb::model::field::EntityFieldKind::Int128),
        Primitive::Nat => quote!(::icydb::model::field::EntityFieldKind::UintBig),
        Primitive::Nat8 | Primitive::Nat16 | Primitive::Nat32 | Primitive::Nat64 => {
            quote!(::icydb::model::field::EntityFieldKind::Uint)
        }
        Primitive::Nat128 => quote!(::icydb::model::field::EntityFieldKind::Uint128),
        Primitive::Principal => quote!(::icydb::model::field::EntityFieldKind::Principal),
        Primitive::Subaccount => quote!(::icydb::model::field::EntityFieldKind::Subaccount),
        Primitive::Text => quote!(::icydb::model::field::EntityFieldKind::Text),
        Primitive::Timestamp => quote!(::icydb::model::field::EntityFieldKind::Timestamp),
        Primitive::Ulid => quote!(::icydb::model::field::EntityFieldKind::Ulid),
        Primitive::Unit => quote!(::icydb::model::field::EntityFieldKind::Unit),
    }
}
