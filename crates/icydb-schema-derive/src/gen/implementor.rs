use crate::prelude::*;
use syn::{GenericParam, WherePredicate, parse2, punctuated::Punctuated, token::Comma};

///
/// Imp
///

pub trait Imp<N> {
    fn strategy(node: &N) -> Option<TraitStrategy>;
}

///
/// Implementor
///

pub struct Implementor<'a> {
    def: &'a Def,
    trait_kind: TraitKind,
    trait_generics: Vec<TokenStream>,
    extra_generics: Vec<GenericParam>,
    extra_where: Vec<WherePredicate>,
    tokens: TokenStream,
}

impl<'a> Implementor<'a> {
    pub fn new(def: &'a Def, trait_kind: TraitKind) -> Self {
        Self {
            def,
            trait_kind,
            trait_generics: Vec::new(),
            extra_generics: Vec::new(),
            extra_where: Vec::new(),
            tokens: quote!(),
        }
    }

    pub fn add_impl_constraint(mut self, tokens: TokenStream) -> Self {
        let predicate: WherePredicate = parse2(tokens).unwrap();
        self.extra_where.push(predicate);
        self
    }

    pub fn add_impl_generic(mut self, tokens: TokenStream) -> Self {
        let generic_param: GenericParam = parse2(tokens).unwrap();
        self.extra_generics.push(generic_param);
        self
    }

    pub fn add_trait_generic(mut self, tokens: TokenStream) -> Self {
        self.trait_generics.push(tokens);
        self
    }

    pub fn add_tokens(mut self, tokens: TokenStream) -> Self {
        self.tokens.extend(tokens);
        self
    }

    pub fn set_tokens(mut self, tokens: TokenStream) -> Self {
        self.tokens = tokens;
        self
    }

    fn impl_header(&self) -> TokenStream {
        let ident = self.def.ident();
        let item = self
            .def
            .item
            .as_ref()
            .expect("Def.item must be Some for impl generation");
        let generics = &item.generics;
        let trait_kind = &self.trait_kind;
        let trait_generics = &self.trait_generics;

        // Split once to get the type-position generics (e.g., `Type<T, 'a>`).
        let (_base_impl_generics, ty_generics, _base_where_unused) = generics.split_for_impl();

        // ---- Merge impl generics: < base , extra ... >
        let mut all_params: Punctuated<GenericParam, Comma> = generics.params.clone(); // base params
        for g in &self.extra_generics {
            all_params.push(g.clone());
        }
        let impl_generics_ts = if all_params.is_empty() {
            quote!()
        } else {
            quote!( < #all_params > )
        };

        // ---- Merge where predicates: where base , extra ...
        let where_tokens = if let Some(mut wc) = generics.where_clause.clone() {
            for p in &self.extra_where {
                wc.predicates.push(p.clone());
            }
            quote!( #wc )
        } else if !self.extra_where.is_empty() {
            let preds = &self.extra_where;
            quote!( where #(#preds),* )
        } else {
            quote!()
        };

        // ---- Trait path with optional generics (avoid `Trait<>`)
        let trait_path = match trait_kind {
            TraitKind::Inherent => quote!(), // not used
            t => {
                if trait_generics.is_empty() {
                    quote!( #t )
                } else {
                    quote!( #t::< #(#trait_generics),* > )
                }
            }
        };

        match trait_kind {
            TraitKind::Inherent => {
                // impl <all_params> Type<ty_params> where ...
                quote! {
                    impl #impl_generics_ts #ident #ty_generics #where_tokens
                }
            }
            _ => {
                // impl <all_params> Trait<trait_generics> for Type<ty_params> where ...
                quote! {
                    impl #impl_generics_ts #trait_path for #ident #ty_generics #where_tokens
                }
            }
        }
    }
}

impl ToTokens for Implementor<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let header = self.impl_header();
        let inner_tokens = &self.tokens;

        tokens.extend(quote! {
            #header {
                #inner_tokens
            }
        });
    }
}
