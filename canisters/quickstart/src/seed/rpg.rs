use icydb::design::prelude::{Decimal, Float64, Principal};
use icydb_testing_quickstart_fixtures::schema::Character;

const DUNGEON_MASTER_CHARACTERS: [&str; 24] = [
    "Iaido Ruyito Chiburi",
    "Zed Duke of Banville",
    "Chani Sayyadina Sihaya",
    "Hawk the Fearless",
    "Boris Wizard of Baldor",
    "Alex Ander",
    "Nabi the Prophet",
    "Hissssa Lizard of Makan",
    "Gothmog",
    "Sonja She-Devil",
    "Leyla Shadowseek",
    "Mophus the Healer",
    "Wuuf the Bika",
    "Stamm Bladecaster",
    "Azizi Johari",
    "Leif the Valiant",
    "Tiggy Tamal",
    "Wu Tse Son of Heaven",
    "Daroou",
    "Halk the Barbarian",
    "Syra Child of Nature",
    "Gando Thurfoot",
    "Linflas",
    "Elija Lion of Yaitopya",
];
const BLOODWYCH_CHARACTERS: [&str; 16] = [
    "Blodwyn Stonemaiden",
    "Astroth Slaemworth",
    "Sir Edward Lion",
    "Ulrich Sternaxe",
    "Zastaph Mantric",
    "Murlock Darkheart",
    "Zothen Runecaster",
    "Megrim of Moonwych",
    "Sethra Bhoaghail",
    "Hengist Meldanash",
    "Eleanor of Avalon",
    "Baldric the Dung",
    "Elfric Falaendor",
    "Mr. Flay Sepulcrast",
    "Thai Chang of Yinn",
    "Rosanne Swifthand",
];
const CHARACTER_ROW_COUNT: usize = DUNGEON_MASTER_CHARACTERS.len() + BLOODWYCH_CHARACTERS.len();
const CLASSES: [&str; 8] = [
    "Fighter", "Wizard", "Rogue", "Cleric", "Ranger", "Paladin", "Druid", "Bard",
];
const BACKGROUNDS: [&str; 8] = [
    "Acolyte",
    "Criminal",
    "Sage",
    "Soldier",
    "Outlander",
    "Noble",
    "Guild Artisan",
    "Hermit",
];
const GUILD_RANKS: [&str; 4] = ["Initiate", "Adept", "Veteran", "Captain"];
const RESISTANCES: [&str; 8] = [
    "fire",
    "cold",
    "poison",
    "lightning",
    "psychic",
    "necrotic",
    "radiant",
    "force",
];

/// Build one deterministic RPG fixture set with one row per named character from
/// Amiga-era Dungeon Master and Bloodwych rosters.
#[must_use]
pub fn characters() -> Vec<Character> {
    let rows = DUNGEON_MASTER_CHARACTERS
        .iter()
        .copied()
        .chain(BLOODWYCH_CHARACTERS.iter().copied())
        .enumerate()
        .map(|(index, name)| {
            // Derive deterministic categorical values so queries can group/filter by role and rank.
            let class_name = CLASSES[index % CLASSES.len()].to_string();
            let background = BACKGROUNDS[index % BACKGROUNDS.len()].to_string();
            let guild_rank = if index % 5 == 0 {
                None
            } else {
                Some(GUILD_RANKS[index % GUILD_RANKS.len()].to_string())
            };

            // Compute numeric and combat-focused fields with bounded, non-random formulas.
            let level = u16::try_from((index % 20) + 1).unwrap_or(u16::MAX);
            let strength = i16::try_from(8 + (index * 3 % 13)).unwrap_or(i16::MAX);
            let dexterity = i16::try_from(8 + (index * 5 % 13)).unwrap_or(i16::MAX);
            let constitution = i16::try_from(8 + (index * 7 % 13)).unwrap_or(i16::MAX);
            let intelligence = i16::try_from(8 + (index * 11 % 13)).unwrap_or(i16::MAX);
            let wisdom = i16::try_from(8 + (index * 2 % 13)).unwrap_or(i16::MAX);
            let charisma = i16::try_from(8 + (index * 9 % 13)).unwrap_or(i16::MAX);

            // Build multi-value fields and mixed optional fields for broader SQL coverage.
            let resistances = vec![
                RESISTANCES[index % RESISTANCES.len()].to_string(),
                RESISTANCES[(index + 3) % RESISTANCES.len()].to_string(),
            ];
            let inventory_weights = vec![
                u16::try_from((index % 60) + 1).unwrap_or(u16::MAX),
                u16::try_from(((index * 3) % 60) + 1).unwrap_or(u16::MAX),
                u16::try_from(((index * 5) % 60) + 1).unwrap_or(u16::MAX),
            ];
            let mentor_principal = if index % 4 == 0 {
                None
            } else {
                Some(Principal::anonymous())
            };
            let critical_step = u8::try_from(index % 10).unwrap_or(u8::MAX);
            let dodge_step = u8::try_from(index % 15).unwrap_or(u8::MAX);
            let critical_chance = Decimal::new(i64::from(critical_step) + 5, 2);
            let dodge_chance =
                Float64::try_new(0.1 + f64::from(dodge_step) * 0.015).unwrap_or_default();

            Character {
                name: name.to_string(),
                description: format!(
                    "{} specialized in {} tactics.",
                    name,
                    if index % 2 == 0 {
                        "frontline"
                    } else {
                        "control"
                    }
                ),
                class_name,
                background,
                level,
                experience: (u64::from(level) * 1_750)
                    + (u64::try_from(index).unwrap_or(u64::MAX) * 90),
                strength,
                dexterity,
                constitution,
                intelligence,
                wisdom,
                charisma,
                hit_points: i32::from(constitution) * 6
                    + i32::try_from(index % 25).unwrap_or(i32::MAX),
                armor_class: u8::try_from(10 + (index % 9)).unwrap_or(u8::MAX),
                spell_slots: u8::try_from((index % 7) + 1).unwrap_or(u8::MAX),
                initiative_bonus: i8::try_from(index % 6).unwrap_or(i8::MAX),
                gold_pieces: u32::try_from(250 + (index * 37)).unwrap_or(u32::MAX),
                critical_chance,
                dodge_chance,
                is_npc: index % 6 == 0,
                guild_rank,
                mentor_principal,
                resistances,
                inventory_weights,
                portrait: vec![
                    u8::try_from(index % 255).unwrap_or(u8::MAX),
                    u8::try_from((index * 7) % 255).unwrap_or(u8::MAX),
                    u8::try_from((index * 11) % 255).unwrap_or(u8::MAX),
                ]
                .into(),
                last_rest_at: (1_700_000_000_000 + u64::try_from(index).unwrap_or(u64::MAX) * 60)
                    .into(),
                respawn_cooldown: (30 + u64::try_from(index % 120).unwrap_or(u64::MAX)).into(),
                ..Default::default()
            }
        })
        .collect::<Vec<_>>();
    debug_assert_eq!(rows.len(), CHARACTER_ROW_COUNT);

    rows
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn base_characters_has_one_row_per_amiga_character() {
        let rows = characters();
        assert_eq!(rows.len(), CHARACTER_ROW_COUNT);
    }

    #[test]
    fn base_rpg_characters_covers_full_character_roster_once() {
        let rows = characters();
        let names: BTreeSet<String> = rows.iter().map(|row| row.name.clone()).collect();
        assert_eq!(names.len(), rows.len());

        for expected in DUNGEON_MASTER_CHARACTERS
            .iter()
            .chain(BLOODWYCH_CHARACTERS.iter())
        {
            assert!(names.contains(*expected));
        }
    }

    #[test]
    fn base_rpg_characters_has_non_empty_multi_value_fields() {
        let rows = characters();
        assert!(
            rows.iter()
                .all(|row| !row.resistances.is_empty() && !row.inventory_weights.is_empty())
        );
    }
}
