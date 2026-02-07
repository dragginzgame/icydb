mod kind;
pub use kind::*;

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
        // Emit typed field consts
        let field_consts: Vec<TokenStream> = node
            .fields
            .iter()
            .map(|f| {
                let constant = f.ident.to_string().to_case(Case::Constant);
                let ident = format_ident!("{constant}");
                let name_str = f.ident.to_string();

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
            .position(|field| field.ident == node.primary_key.field)
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
                    entity_name: <Self as ::icydb::traits::EntityIdentity>::ENTITY_NAME,
                    primary_key: &Self::#model_fields_ident[#pk_index],
                    fields: &Self::#model_fields_ident,
                    indexes: <Self as ::icydb::traits::EntitySchema>::INDEXES,
                };
        };

        let tokens = quote! {
            #(#field_consts)*
            #(#model_field_consts)*
            #model_fields
            #entity_model
        };

        let impl_tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(impl_tokens))
    }
}

///
/// Enum
///

impl Imp<Enum> for InherentTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let kind = quote!(::icydb::model::field::EntityFieldKind::Enum);
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::EntityFieldKind = #kind;
        };

        Some(TraitStrategy::from_impl(
            Implementor::new(node.def(), TraitKind::Inherent)
                .set_tokens(tokens)
                .to_token_stream(),
        ))
    }
}

///
/// Newtype
///

impl Imp<Newtype> for InherentTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let kind = model_kind_from_item(&node.item);
        let mut tokens = quote! {
            pub const KIND: ::icydb::model::field::EntityFieldKind = #kind;
        };

        if let Some(primitive) = node.primitive
            && primitive.supports_arithmetic()
        {
            tokens = quote! {
                #tokens

                /// Saturating addition.
                #[must_use]
                pub fn saturating_add(self, rhs: Self) -> Self {
                    Self(self.0.saturating_add(rhs.0))
                }

                /// Saturating subtraction.
                #[must_use]
                pub fn saturating_sub(self, rhs: Self) -> Self {
                    Self(self.0.saturating_sub(rhs.0))
                }
            };
        }

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// List
///

impl Imp<List> for InherentTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let item_kind = model_kind_from_item(&node.item);
        let kind = quote!(::icydb::model::field::EntityFieldKind::List(&#item_kind));
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::EntityFieldKind = #kind;

            /// Appends an item to the list.
            pub fn push(&mut self, value: #item) {
                self.0.push(value);
            }

            /// Removes and returns the last item, if any.
            pub fn pop(&mut self) -> Option<#item> {
                self.0.pop()
            }

            /// Inserts an item at `index`, clamping out-of-bounds indices to the tail.
            pub fn insert(&mut self, index: usize, value: #item) {
                let idx = index.min(self.0.len());
                self.0.insert(idx, value);
            }

            /// Removes and returns the item at `index` if it exists.
            pub fn remove(&mut self, index: usize) -> Option<#item> {
                if index < self.0.len() {
                    Some(self.0.remove(index))
                } else {
                    None
                }
            }

            /// Clears all items from the list.
            pub fn clear(&mut self) {
                self.0.clear();
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(quote! { #tokens }))
    }
}

///
/// Set
///

impl Imp<Set> for InherentTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let item_kind = model_kind_from_item(&node.item);
        let kind = quote!(::icydb::model::field::EntityFieldKind::Set(&#item_kind));
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::EntityFieldKind = #kind;

            /// Inserts a value into the set. Returns true if it was newly inserted.
            pub fn insert(&mut self, value: #item) -> bool {
                self.0.insert(value)
            }

            /// Removes a value from the set. Returns true if it was present.
            pub fn remove(&mut self, value: &#item) -> bool {
                self.0.remove(value)
            }

            /// Clears all values from the set.
            pub fn clear(&mut self) {
                self.0.clear();
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(quote! { #tokens }))
    }
}

///
/// Map
///

impl Imp<Map> for InherentTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key_kind = model_kind_from_item(&node.key);
        let value_kind = model_kind_from_nested_value(&node.value);
        let key = node.key.type_expr();
        let value = node.value.type_expr();
        let kind = quote! {
            ::icydb::model::field::EntityFieldKind::Map {
                key: &#key_kind,
                value: &#value_kind,
            }
        };

        let tokens = quote! {
            pub const KIND: ::icydb::model::field::EntityFieldKind = #kind;

            /// Returns a reference to the value for `key`, if present.
            pub fn get(&self, key: &#key) -> Option<&#value> {
                self.0.get(key)
            }

            /// Inserts a key/value pair, returning the previous value if any.
            pub fn insert(&mut self, key: #key, value: #value) -> Option<#value> {
                self.0.insert(key, value)
            }

            /// Removes the value for `key`, returning it if present.
            pub fn remove(&mut self, key: &#key) -> Option<#value> {
                self.0.remove(key)
            }

            /// Clears all entries from the map.
            pub fn clear(&mut self) {
                self.0.clear();
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Record
///

impl Imp<Record> for InherentTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let kind = quote!(::icydb::model::field::EntityFieldKind::Structured { queryable: false });
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::EntityFieldKind = #kind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for InherentTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let kind = quote!(::icydb::model::field::EntityFieldKind::Structured { queryable: false });
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
