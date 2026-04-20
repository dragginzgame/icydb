use crate::schema::DemoRpgStore;
use icydb::design::prelude::*;

///
/// Character
///
/// Fixture RPG character entity used by SQL endpoint and integration harnesses.
///

#[entity(
    store = "DemoRpgStore",
    pk(field = "id"),
    index(fields = "name"),
    index(fields = "level, class_name"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text"))),
        field(ident = "description", value(item(prim = "Text"))),
        field(ident = "class_name", value(item(prim = "Text"))),
        field(ident = "background", value(item(prim = "Text"))),
        field(ident = "homeland", value(item(prim = "Text"))),
        field(ident = "level", value(item(prim = "Nat16"))),
        field(ident = "experience", value(item(prim = "Nat64"))),
        field(ident = "renown", value(item(prim = "Int16"))),
        field(ident = "strength", value(item(prim = "Int16"))),
        field(ident = "dexterity", value(item(prim = "Int16"))),
        field(ident = "constitution", value(item(prim = "Int16"))),
        field(ident = "intelligence", value(item(prim = "Int16"))),
        field(ident = "wisdom", value(item(prim = "Int16"))),
        field(ident = "charisma", value(item(prim = "Int16"))),
        field(ident = "hit_points", value(item(prim = "Int32"))),
        field(ident = "armor_class", value(item(prim = "Nat8"))),
        field(ident = "spell_slots", value(item(prim = "Nat8"))),
        field(ident = "initiative_bonus", value(item(prim = "Int8"))),
        field(ident = "gold_pieces", value(item(prim = "Nat32"))),
        field(ident = "critical_chance", value(item(prim = "Decimal", scale = 2))),
        field(ident = "dodge_chance", value(item(prim = "Float64"))),
        field(ident = "is_npc", value(item(prim = "Bool"))),
        field(ident = "guild_rank", value(opt, item(prim = "Text"))),
        field(ident = "mentor_principal", value(opt, item(prim = "Principal"))),
        field(ident = "resistances", value(many, item(prim = "Text"))),
        field(ident = "inventory_weights", value(many, item(prim = "Nat16"))),
        field(ident = "portrait", value(item(prim = "Blob"))),
        field(ident = "last_rest_at", value(item(prim = "Timestamp"))),
        field(ident = "respawn_cooldown", value(item(prim = "Duration")))
    )
)]
pub struct Character {}
