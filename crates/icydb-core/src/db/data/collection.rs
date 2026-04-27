//! Module: data::collection
//! Responsibility: short aliases for list/map collection payload codecs.
//! Boundary: preserves the original structural-field functions while giving
//! callers a semantic namespace.

pub(in crate::db) mod decode {
    pub(in crate::db) use crate::db::data::structural_field::{
        decode_list_field_items as field_items, decode_list_item as item, decode_map_entry as map,
        decode_map_field_entries as field_entries,
    };
}

pub(in crate::db) mod encode {
    pub(in crate::db) use crate::db::data::structural_field::{
        encode_list_field_items as field_items, encode_list_item as item, encode_map_entry as map,
        encode_map_field_entries as field_entries,
    };
}
