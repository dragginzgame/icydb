use crate::{
    db::primitives::{TextFilterKind, TextListFilterKind},
    traits::Filterable,
    types::Account,
};

impl Filterable for Account {
    type Filter = TextFilterKind;
    type ListFilter = TextListFilterKind;
}
