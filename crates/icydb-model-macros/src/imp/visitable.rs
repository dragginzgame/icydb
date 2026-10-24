//! Module: imp::visitable
//! Responsibility: generated implementation tokens.
//! Does not own: runtime trait semantics.
//! Boundary: parsed nodes to impl tokens.

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
        let paths = node.def().paths();
        Some(field_list_visitable_strategy(
            node.def(),
            &node.fields,
            paths,
        ))
    }
}

///
/// Enum
///

impl Imp<Enum> for VisitableTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        // Collect both immutable and mutable match arms
        let (arms, arms_mut): (TokenStream, TokenStream) =
            node.variants.iter().map(enum_variant).unzip();

        let inner = quote! { match self { #arms } };
        let inner_mut = quote! { match self { #arms_mut } };

        let drives = quote_drives(&inner, &inner_mut, paths);
        let tokens = Implementor::new(node.def(), TraitKind::Visitable)
            .set_tokens(drives)
            .impl_tokens(paths);

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// List
///

impl Imp<List> for VisitableTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        let inner = immutable_collection_visit_tokens();
        let inner_mut = quote! {
            for (i, v) in self.0.iter_mut().enumerate() {
                perform_visit_mut(visitor, v, i);
            }
        };

        let drives = quote_drives(&inner, &inner_mut, paths);

        Some(visitable_trait_strategy(node.def(), drives, paths))
    }
}

///
/// Map
///

impl Imp<Map> for VisitableTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let paths = node.def().paths();
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

        let drives = quote_drives(&inner, &inner_mut, paths);

        let tokens = Implementor::new(node.def(), TraitKind::Visitable)
            .set_tokens(drives)
            .impl_tokens(paths);

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Newtype
///

impl Imp<Newtype> for VisitableTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        let inner = quote! {
           perform_visit(visitor, &self.0, None);
        };
        let inner_mut = quote! {
           perform_visit_mut(visitor, &mut self.0, None);
        };

        let drives = quote_drives(&inner, &inner_mut, paths);

        let tokens = Implementor::new(node.def(), TraitKind::Visitable)
            .set_tokens(drives)
            .impl_tokens(paths);

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Record
///

impl Imp<Record> for VisitableTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        Some(field_list_visitable_strategy(
            node.def(),
            &node.fields,
            paths,
        ))
    }
}

///
/// Set
///

impl Imp<Set> for VisitableTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        let inner = immutable_collection_visit_tokens();

        let drive = quote_drive(&inner, paths);

        // Only immutable; mutating set entries can break hashing.
        Some(visitable_trait_strategy(node.def(), drive, paths))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for VisitableTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let paths = node.def().paths();
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

        let drives = quote_drives(&inner, &inner_mut, paths);

        let tokens = Implementor::new(node.def(), TraitKind::Visitable)
            .set_tokens(drives)
            .impl_tokens(paths);

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
fn field_list(def: &Def, fields: &FieldList, paths: &CratePaths) -> TokenStream {
    let model = &paths.model;
    let bindings = field_walk_bindings(fields);
    let field_table_ident = format_ident!("__VISITABLE_FIELDS");

    let visit_helpers = bindings.iter().map(|binding| {
        let fn_ident = binding.visit_fn_ident();
        let field_name = binding.field_name().to_string();
        let member_ref = binding.member_ref_from(quote!(node));

        quote! {
            fn #fn_ident(node: &Self, visitor: &mut dyn #model::visitor::VisitorCore) {
                use #model::visitor::perform_visit;

                perform_visit(visitor, #member_ref, #field_name);
            }
        }
    });

    let visit_mut_helpers = bindings.iter().map(|binding| {
        let fn_ident = binding.visit_mut_fn_ident();
        let field_name = binding.field_name().to_string();
        let member_mut = binding.member_mut_from(quote!(node));

        quote! {
            fn #fn_ident(node: &mut Self, visitor: &mut dyn #model::visitor::VisitorMutCore) {
                use #model::visitor::perform_visit_mut;

                perform_visit_mut(visitor, #member_mut, #field_name);
            }
        }
    });

    let descriptors = bindings.iter().map(|binding| {
        let field_name = binding.field_name().to_string();
        let visit_fn = binding.visit_fn_ident();
        let visit_mut_fn = binding.visit_mut_fn_ident();

        quote! {
            #model::visitor::VisitableFieldDescriptor::new(
                #field_name,
                Self::#visit_fn,
                Self::#visit_mut_fn,
            )
        }
    });

    let inherent_tokens = Implementor::inherent(def)
        .set_tokens(quote! {
            #(#visit_helpers)*
            #(#visit_mut_helpers)*

            const #field_table_ident: &'static [#model::visitor::VisitableFieldDescriptor<Self>] =
                &[#(#descriptors),*];
        })
        .impl_tokens(paths);

    let trait_tokens = Implementor::new(def, TraitKind::Visitable)
        .set_tokens(quote! {
            fn drive(&self, visitor: &mut dyn #model::visitor::VisitorCore) {
                #model::visitor::drive_visitable_fields(visitor, self, Self::#field_table_ident);
            }

            fn drive_mut(&mut self, visitor: &mut dyn #model::visitor::VisitorMutCore) {
                #model::visitor::drive_visitable_fields_mut(
                    visitor,
                    self,
                    Self::#field_table_ident,
                );
            }
        })
        .impl_tokens(paths);

    quote! {
        #inherent_tokens
        #trait_tokens
    }
}

fn field_list_visitable_strategy(
    def: &Def,
    fields: &FieldList,
    paths: &CratePaths,
) -> TraitStrategy {
    TraitStrategy::from_impl(field_list(def, fields, paths))
}

// enum_variant
fn enum_variant(variant: &EnumVariant) -> (TokenStream, TokenStream) {
    let ident = &variant.name;

    if variant.value.is_some() {
        let name = ident.to_string();
        (
            quote! { Self::#ident(value) => perform_visit(visitor, value, #name), },
            quote! { Self::#ident(value) => perform_visit_mut(visitor, value, #name), },
        )
    } else {
        (quote! { Self::#ident => {} }, quote! { Self::#ident => {} })
    }
}

///
/// HELPERS
///

fn quote_drives(inner: &TokenStream, inner_mut: &TokenStream, paths: &CratePaths) -> TokenStream {
    let q = quote_drive(inner, paths);
    let qm = quote_drive_mut(inner_mut, paths);

    quote! {
        #q
        #qm
    }
}

fn visitable_trait_strategy(def: &Def, tokens: TokenStream, paths: &CratePaths) -> TraitStrategy {
    let tokens = Implementor::new(def, TraitKind::Visitable)
        .set_tokens(tokens)
        .impl_tokens(paths);

    TraitStrategy::from_impl(tokens)
}

fn immutable_collection_visit_tokens() -> TokenStream {
    quote! {
        for (i, v) in self.0.iter().enumerate() {
            perform_visit(visitor, v, i);
        }
    }
}

fn quote_drive(inner: &TokenStream, paths: &CratePaths) -> TokenStream {
    let model = &paths.model;
    quote! {
        fn drive(&self, visitor: &mut dyn #model::visitor::VisitorCore) {
            use #model::visitor::perform_visit;
            #inner
        }
    }
}

fn quote_drive_mut(inner: &TokenStream, paths: &CratePaths) -> TokenStream {
    let model = &paths.model;
    quote! {
        fn drive_mut(&mut self, visitor: &mut dyn #model::visitor::VisitorMutCore) {
            use #model::visitor::perform_visit_mut;
            #inner
        }
    }
}
