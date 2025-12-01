use crate::prelude::*;

///
/// NumCastTrait
///

pub struct NumCastTrait {}

///
/// Newtype
///

impl Imp<Newtype> for NumCastTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let primitive = node.primitive.as_ref()?; // bail early if no primitive

        let num_fn = primitive.num_cast_fn();
        let to_method = format_ident!("to_{}", num_fn);
        let from_method = format_ident!("from_{}", num_fn);

        // quote
        let cp = paths().core;
        let q = quote! {
            fn from<T: #cp::traits::NumToPrimitive>(n: T) -> Option<Self> {
                let num = n.#to_method()?;
                <Self as #cp::traits::NumFromPrimitive>::#from_method(num)
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::NumCast)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// NumFromPrimitiveTrait
///

pub struct NumFromPrimitiveTrait {}

///
/// Newtype
///

impl Imp<Newtype> for NumFromPrimitiveTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let primitive = node.primitive.as_ref()?; // bail early if no primitive

        let item = &node.item.type_expr();

        // quote
        let mut q = quote! {
            fn from_i64(n: i64) -> Option<Self> {
                #item::from_i64(n).map(Self)
            }

            fn from_u64(n: u64) -> Option<Self> {
                #item::from_u64(n).map(Self)
            }
        };

        // floats
        if primitive.is_float() {
            q.extend(quote! {
                fn from_f64(n: f64) -> Option<Self> {
                    #item::from_f64(n).map(Self)
                }
            });
        }

        let tokens = Implementor::new(node.def(), TraitKind::NumFromPrimitive)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// NumToPrimitiveTrait
///

pub struct NumToPrimitiveTrait {}

///
/// Newtype
///

impl Imp<Newtype> for NumToPrimitiveTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let cp = paths().core;
        let q = quote! {
            fn to_i64(&self) -> Option<i64> {
                #cp::export::num_traits::NumCast::from(self.0)
            }

            fn to_u64(&self) -> Option<u64> {
                #cp::export::num_traits::NumCast::from(self.0)
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::NumToPrimitive)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
