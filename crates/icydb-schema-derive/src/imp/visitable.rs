use crate::{imp::field_walk::field_walk_bindings, prelude::*};

///
/// VisitableTrait
///

pub struct VisitableTrait {}

///
/// Entity
///

impl Imp<Entity> for VisitableTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        Some(field_list_visitable_strategy(node.def(), &node.fields))
    }
}

///
/// Enum
///

impl Imp<Enum> for VisitableTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        // Collect both immutable and mutable match arms
        let (arms, arms_mut): (TokenStream, TokenStream) =
            node.variants.iter().map(enum_variant).unzip();

        let inner = quote! { match self { #arms } };
        let inner_mut = quote! { match self { #arms_mut } };

        let tokens = Implementor::new(node.def(), TraitKind::Visitable)
            .set_tokens(quote_drives(&inner, &inner_mut))
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// List
///

impl Imp<List> for VisitableTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let inner = immutable_collection_visit_tokens();
        let inner_mut = quote! {
            for (i, v) in self.0.iter_mut().enumerate() {
                perform_visit_mut(visitor, v, i);
            }
        };

        Some(visitable_trait_strategy(
            node.def(),
            quote_drives(&inner, &inner_mut),
        ))
    }
}

///
/// Map
///

impl Imp<Map> for VisitableTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let inner = quote! {
            for (i, (k, v)) in self.0.iter().enumerate() {
                perform_visit(visitor, k, i);
                perform_visit(visitor, v, i);
            }
        };

        let inner_mut = quote! {
            // Keys are not visited mutably to avoid invalidating hash map invariants.
            for (i, (_k, v)) in self.0.iter_mut().enumerate() {
                perform_visit_mut(visitor, v, i);
            }
        };

        let q = quote_drives(&inner, &inner_mut);

        let tokens = Implementor::new(node.def(), TraitKind::Visitable)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Newtype
///

impl Imp<Newtype> for VisitableTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let inner = quote! {
           perform_visit(visitor, &self.0, None);
        };
        let inner_mut = quote! {
           perform_visit_mut(visitor, &mut self.0, None);
        };

        let q = quote_drives(&inner, &inner_mut);

        let tokens = Implementor::new(node.def(), TraitKind::Visitable)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Record
///

impl Imp<Record> for VisitableTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        Some(field_list_visitable_strategy(node.def(), &node.fields))
    }
}

///
/// Set
///

impl Imp<Set> for VisitableTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let inner = immutable_collection_visit_tokens();

        Some(visitable_trait_strategy(
            node.def(),
            quote_drive(&inner), // Only immutable; mutating set entries can break hashing.
        ))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for VisitableTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let mut inner = quote!();
        let mut inner_mut = quote!();

        for (i, _) in node.values.iter().enumerate() {
            let key = LitStr::new(&i.to_string(), Span::call_site());
            let index = syn::Index::from(i);

            inner.extend(quote! {
                perform_visit(visitor, &self.#index, #key);
            });

            inner_mut.extend(quote! {
                perform_visit_mut(visitor, &mut self.#index, #key);
            });
        }

        let q = quote_drives(&inner, &inner_mut);

        let tokens = Implementor::new(node.def(), TraitKind::Visitable)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// SUB TYPES
///
/// Checks the cardinality of a value and prints out the corresponding
/// visitor code
///

// field_list
fn field_list(def: &Def, fields: &FieldList) -> TokenStream {
    let bindings = field_walk_bindings(fields);
    let field_table_ident = format_ident!("__VISITABLE_FIELDS");

    let visit_helpers = bindings.iter().map(|binding| {
        let fn_ident = binding.visit_fn_ident();
        let field_name = binding.field_name().to_string();
        let member_ref = binding.member_ref_from(quote!(node));

        quote! {
            fn #fn_ident(node: &Self, visitor: &mut dyn ::icydb::visitor::VisitorCore) {
                use ::icydb::visitor::perform_visit;

                perform_visit(visitor, #member_ref, #field_name);
            }
        }
    });

    let visit_mut_helpers = bindings.iter().map(|binding| {
        let fn_ident = binding.visit_mut_fn_ident();
        let field_name = binding.field_name().to_string();
        let member_mut = binding.member_mut_from(quote!(node));

        quote! {
            fn #fn_ident(node: &mut Self, visitor: &mut dyn ::icydb::visitor::VisitorMutCore) {
                use ::icydb::visitor::perform_visit_mut;

                perform_visit_mut(visitor, #member_mut, #field_name);
            }
        }
    });

    let descriptors = bindings.iter().map(|binding| {
        let field_name = binding.field_name().to_string();
        let visit_fn = binding.visit_fn_ident();
        let visit_mut_fn = binding.visit_mut_fn_ident();

        quote! {
            ::icydb::visitor::VisitableFieldDescriptor::new(
                #field_name,
                Self::#visit_fn,
                Self::#visit_mut_fn,
            )
        }
    });

    let inherent_tokens = Implementor::new(def, TraitKind::Inherent)
        .set_tokens(quote! {
            #(#visit_helpers)*
            #(#visit_mut_helpers)*

            const #field_table_ident: &'static [::icydb::visitor::VisitableFieldDescriptor<Self>] =
                &[#(#descriptors),*];
        })
        .to_token_stream();

    let trait_tokens = Implementor::new(def, TraitKind::Visitable)
        .set_tokens(quote! {
            fn drive(&self, visitor: &mut dyn ::icydb::visitor::VisitorCore) {
                ::icydb::visitor::drive_visitable_fields(visitor, self, Self::#field_table_ident);
            }

            fn drive_mut(&mut self, visitor: &mut dyn ::icydb::visitor::VisitorMutCore) {
                ::icydb::visitor::drive_visitable_fields_mut(
                    visitor,
                    self,
                    Self::#field_table_ident,
                );
            }
        })
        .to_token_stream();

    quote! {
        #inherent_tokens
        #trait_tokens
    }
}

fn field_list_visitable_strategy(def: &Def, fields: &FieldList) -> TraitStrategy {
    TraitStrategy::from_impl(field_list(def, fields))
}

// enum_variant
fn enum_variant(variant: &EnumVariant) -> (TokenStream, TokenStream) {
    let ident = &variant.ident;

    if variant.value.is_some() {
        let ident_str = variant.name_const_ident();
        (
            quote! { Self::#ident(value) => perform_visit(visitor, value, Self::#ident_str), },
            quote! { Self::#ident(value) => perform_visit_mut(visitor, value, Self::#ident_str), },
        )
    } else {
        (quote! { Self::#ident => {} }, quote! { Self::#ident => {} })
    }
}

///
/// HELPERS
///

fn quote_drives(inner: &TokenStream, inner_mut: &TokenStream) -> TokenStream {
    let q = quote_drive(inner);
    let qm = quote_drive_mut(inner_mut);

    quote! {
        #q
        #qm
    }
}

fn visitable_trait_strategy(def: &Def, tokens: TokenStream) -> TraitStrategy {
    let tokens = Implementor::new(def, TraitKind::Visitable)
        .set_tokens(tokens)
        .to_token_stream();

    TraitStrategy::from_impl(tokens)
}

fn immutable_collection_visit_tokens() -> TokenStream {
    quote! {
        use ::icydb::traits::Collection;

        for (i, v) in self.iter().enumerate() {
            perform_visit(visitor, v, i);
        }
    }
}

fn quote_drive(inner: &TokenStream) -> TokenStream {
    quote! {
        fn drive(&self, visitor: &mut dyn ::icydb::visitor::VisitorCore) {
            use ::icydb::visitor::perform_visit;
            #inner
        }
    }
}

fn quote_drive_mut(inner: &TokenStream) -> TokenStream {
    quote! {
        fn drive_mut(&mut self, visitor: &mut dyn ::icydb::visitor::VisitorMutCore) {
            use ::icydb::visitor::perform_visit_mut;
            #inner
        }
    }
}
