use crate::schema::SqlTestStore;
use icydb::design::prelude::*;

///
/// Character
///
/// Fixture RPG character entity used by SQL endpoint and integration harnesses.
///

#[entity(
    store = "SqlTestStore",
    pk(field = "id"),
    index(fields = "name"),
    index(fields = "level, class_name"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text")), default = "String::new"),
        field(
            ident = "description",
            value(item(prim = "Text")),
            default = "String::new"
        ),
        field(
            ident = "class_name",
            value(item(prim = "Text")),
            default = "String::new"
        ),
        field(
            ident = "background",
            value(item(prim = "Text")),
            default = "String::new"
        ),
        field(ident = "level", value(item(prim = "Nat16")), default = "u16::default"),
        field(
            ident = "experience",
            value(item(prim = "Nat64")),
            default = "u64::default"
        ),
        field(
            ident = "strength",
            value(item(prim = "Int16")),
            default = "i16::default"
        ),
        field(
            ident = "dexterity",
            value(item(prim = "Int16")),
            default = "i16::default"
        ),
        field(
            ident = "constitution",
            value(item(prim = "Int16")),
            default = "i16::default"
        ),
        field(
            ident = "intelligence",
            value(item(prim = "Int16")),
            default = "i16::default"
        ),
        field(
            ident = "wisdom",
            value(item(prim = "Int16")),
            default = "i16::default"
        ),
        field(
            ident = "charisma",
            value(item(prim = "Int16")),
            default = "i16::default"
        ),
        field(ident = "hit_points", value(item(prim = "Int32")), default = 0),
        field(
            ident = "armor_class",
            value(item(prim = "Nat8")),
            default = "u8::default"
        ),
        field(
            ident = "spell_slots",
            value(item(prim = "Nat8")),
            default = "u8::default"
        ),
        field(
            ident = "initiative_bonus",
            value(item(prim = "Int8")),
            default = "i8::default"
        ),
        field(
            ident = "gold_pieces",
            value(item(prim = "Nat32")),
            default = "u32::default"
        ),
        field(ident = "critical_chance", value(item(prim = "Float32")), default = 0),
        field(ident = "dodge_chance", value(item(prim = "Float64")), default = 0),
        field(
            ident = "is_npc",
            value(item(prim = "Bool")),
            default = "bool::default"
        ),
        field(ident = "guild_rank", value(opt, item(prim = "Text"))),
        field(ident = "mentor_principal", value(opt, item(prim = "Principal"))),
        field(ident = "resistances", value(many, item(prim = "Text"))),
        field(ident = "inventory_weights", value(many, item(prim = "Nat16"))),
        field(ident = "portrait", value(item(prim = "Blob")), default = "Vec::new"),
        field(
            ident = "last_rest_at",
            value(item(prim = "Timestamp")),
            default = "u64::default"
        ),
        field(
            ident = "respawn_cooldown",
            value(item(prim = "Duration")),
            default = "u64::default"
        )
    )
)]
pub struct Character {}
