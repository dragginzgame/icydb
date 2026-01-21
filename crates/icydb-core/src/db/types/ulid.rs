use crate::{
    db::primitives::{TextFilterKind, TextListFilterKind},
    traits::Filterable,
    types::Ulid,
};

impl Filterable for Ulid {
    type Filter = TextFilterKind;
    type ListFilter = TextListFilterKind;
}
