use crate::{prelude::*, view::traits::ViewExpr};

///
/// ItemView
///

pub struct ItemView<'a>(pub &'a Item);

impl ViewExpr for ItemView<'_> {
    fn expr(&self) -> Option<TokenStream> {
        let node = self.0;
        let ty = node.type_expr();

        quote!(<#ty as ::icydb::traits::AsView>::ViewType).into()
    }
}

///
/// ItemUpdate
///

pub struct ItemUpdate<'a>(pub &'a Item);

impl ViewExpr for ItemUpdate<'_> {
    fn expr(&self) -> Option<TokenStream> {
        let node = self.0;
        let ty = node.type_expr();

        quote!(<#ty as ::icydb::traits::UpdateView>::UpdateViewType).into()
    }
}
