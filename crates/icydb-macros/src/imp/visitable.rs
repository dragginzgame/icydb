use crate::prelude::*;

///
/// VisitableTrait
///

pub struct VisitableTrait {}

///
/// Entity
///

impl Imp<Entity> for VisitableTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let q = field_list(&node.fields);

        let tokens = Implementor::new(node.def(), TraitKind::Visitable)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
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
        let inner = quote! {
            for (i, v) in self.0.iter().enumerate() {
                perform_visit(visitor, v, i);
            }
        };

        let inner_mut = quote! {
            for (i, v) in self.0.iter_mut().enumerate() {
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
        let q = field_list(&node.fields);

        let tokens = Implementor::new(node.def(), TraitKind::Visitable)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Set
///

impl Imp<Set> for VisitableTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let inner = quote! {
            for (i, v) in self.0.iter().enumerate() {
                perform_visit(visitor, v, i);
            }
        };

        let q = quote_drive(&inner); // only immutable

        let tokens = Implementor::new(node.def(), TraitKind::Visitable)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
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
pub fn field_list(fields: &FieldList) -> TokenStream {
    let mut inner = quote!();
    let mut inner_mut = quote!();

    for f in fields {
        let field_ident = format_ident!("{}", f.ident);
        let field_ident_s = field_ident.to_string();

        inner.extend(quote! {
            perform_visit(visitor, &self.#field_ident, #field_ident_s);
        });

        inner_mut.extend(quote! {
            perform_visit_mut(visitor, &mut self.#field_ident, #field_ident_s);
        });
    }

    quote_drives(&inner, &inner_mut)
}

// enum_variant
pub fn enum_variant(variant: &EnumVariant) -> (TokenStream, TokenStream) {
    let ident = &variant.ident;

    if variant.value.is_some() {
        let ident_str = ident.to_string();
        (
            quote! { Self::#ident(value) => perform_visit(visitor, value, #ident_str), },
            quote! { Self::#ident(value) => perform_visit_mut(visitor, value, #ident_str), },
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

// quote_drive
// (immutable)
fn quote_drive(inner: &TokenStream) -> TokenStream {
    let cp = paths().core;
    quote! {
        fn drive(&self, visitor: &mut dyn #cp::visitor::Visitor) {
            use #cp::visitor::perform_visit;
            #inner
        }
    }
}

// quote_drive_mut
// (mutable)
fn quote_drive_mut(inner: &TokenStream) -> TokenStream {
    let cp = paths().core;
    quote! {
        fn drive_mut(&mut self, visitor: &mut dyn #cp::visitor::VisitorMut) {
            use #cp::visitor::perform_visit_mut;
            #inner
        }
    }
}
