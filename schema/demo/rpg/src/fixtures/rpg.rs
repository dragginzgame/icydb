use icydb::design::prelude::{Decimal, Float64, Principal};

use crate::schema::Character;

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
const BALDURS_GATE_II_CHARACTERS: [&str; 18] = [
    "Imoen",
    "Minsc",
    "Jaheira",
    "Yoshimo",
    "Aerie",
    "Anomen Delryn",
    "Cernd",
    "Edwin Odesseiron",
    "Haer'Dalis",
    "Jan Jansen",
    "Keldorn Firecam",
    "Korgan Bloodaxe",
    "Mazzy Fentan",
    "Nalia de'Arnise",
    "Valygar Corthala",
    "Viconia DeVir",
    "Sarevok Anchev",
    "Jon Irenicus",
];
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
const HOMELANDS: [&str; 12] = [
    "Avalon Marches",
    "Banville Reach",
    "Moonwych Fens",
    "Yinn Protectorate",
    "Makan Salt Flats",
    "Baldor Enclave",
    "Yaitopya Coast",
    "Sepulcrast Hollow",
    "Emerald Tangle",
    "Redglass Steppe",
    "Silvermere",
    "Stormwake Isles",
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
/// Dungeon Master, Bloodwych, and Baldur's Gate II party rosters.
#[must_use]
pub fn characters() -> Vec<Character> {
    DUNGEON_MASTER_CHARACTERS
        .iter()
        .copied()
        .chain(BLOODWYCH_CHARACTERS.iter().copied())
        .chain(BALDURS_GATE_II_CHARACTERS.iter().copied())
        .enumerate()
        .map(|(index, name)| {
            // Build a stable character seed first so every downstream field can
            // vary independently without falling back to visible modulo cycles.
            let seed = character_seed(index, name);
            let class_name = class_for_name(name, seed).to_string();
            let background = background_for_name(name, seed).to_string();
            let homeland = homeland_for_name(name, seed).to_string();
            let level = level_for_seed(seed);

            // Derive the six core abilities from class/background profiles plus
            // a wider per-character roll so rows stop clustering in tiny bands.
            let (strength, dexterity, constitution, intelligence, wisdom, charisma) =
                build_abilities(class_name.as_str(), background.as_str(), level, seed);

            // Compute combat-facing columns from the character profile rather
            // than independent increments so these fields read coherently.
            let hit_points = hit_points_for(class_name.as_str(), level, constitution, seed);
            let armor_class = armor_class_for(class_name.as_str(), dexterity, intelligence, seed);
            let spell_slots = spell_slots_for(class_name.as_str(), level, seed);
            let initiative_bonus = initiative_bonus_for(class_name.as_str(), dexterity, seed);
            let critical_chance =
                critical_chance_for(class_name.as_str(), strength, dexterity, seed);
            let dodge_chance = dodge_chance_for(class_name.as_str(), dexterity, armor_class, seed);

            // Economy and social columns should not be thin level-based offsets.
            let is_npc = is_npc_for(name, seed);
            let renown = renown_for(
                class_name.as_str(),
                background.as_str(),
                level,
                is_npc,
                seed,
            );
            let guild_rank = guild_rank_for(background.as_str(), renown, is_npc, seed);
            let mentor_principal = mentor_principal_for(name, background.as_str(), level, seed);

            // Inventory/rest metadata now varies on wider, class-aware ranges so
            // timestamp, duration, and collection queries have more texture.
            let resistances = build_resistances(class_name.as_str(), background.as_str(), seed);
            let inventory_weights = build_inventory_weights(class_name.as_str(), level, seed);
            let portrait = portrait_for(seed);
            let last_rest_at = last_rest_at_for(level, background.as_str(), seed);
            let respawn_cooldown = respawn_cooldown_for(level, is_npc, seed);
            let gold_pieces = gold_pieces_for(background.as_str(), level, renown, seed);

            Character {
                name: name.to_string(),
                description: description_for(
                    name,
                    class_name.as_str(),
                    background.as_str(),
                    homeland.as_str(),
                    level,
                    renown,
                ),
                class_name,
                background,
                homeland,
                level,
                experience: experience_for(level, renown, seed),
                renown,
                strength,
                dexterity,
                constitution,
                intelligence,
                wisdom,
                charisma,
                hit_points,
                armor_class,
                spell_slots,
                initiative_bonus,
                gold_pieces,
                critical_chance,
                dodge_chance,
                is_npc,
                guild_rank,
                mentor_principal,
                resistances,
                inventory_weights,
                portrait: portrait.into(),
                last_rest_at: last_rest_at.into(),
                respawn_cooldown: respawn_cooldown.into(),
                ..Default::default()
            }
        })
        .collect()
}

// Hash character identity into one stable seed that can drive the whole row.
fn character_seed(index: usize, name: &str) -> u64 {
    let mut seed = 0xcbf2_9ce4_8422_2325u64 ^ (u64::try_from(index).unwrap_or(u64::MAX) << 17);
    for byte in name.as_bytes() {
        seed ^= u64::from(*byte);
        seed = seed.wrapping_mul(0x1000_0000_01b3);
    }

    seed
}

// Step the seed with an extra salt so each derived field gets an independent
// distribution instead of reusing the same low bits repeatedly.
fn seed_step(seed: u64, salt: u64) -> u64 {
    let rotate = u32::try_from((salt % 31) + 1).unwrap_or(1);
    seed.rotate_left(rotate)
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407 ^ salt.wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

// Produce one inclusive deterministic integer range from the stepped seed.
fn seed_range_u64(seed: u64, salt: u64, min: u64, max: u64) -> u64 {
    if min >= max {
        return min;
    }

    let span = max - min + 1;
    min + (seed_step(seed, salt) % span)
}

// Produce one inclusive signed range without bringing in an RNG dependency.
fn seed_range_i16(seed: u64, salt: u64, min: i16, max: i16) -> i16 {
    if min >= max {
        return min;
    }

    let min_i64 = i64::from(min);
    let span = u64::try_from(i64::from(max) - min_i64 + 1).unwrap_or(1);
    let offset = i64::try_from(seed_step(seed, salt) % span).unwrap_or(0);

    i16::try_from(min_i64 + offset).unwrap_or(max)
}

// Pick one stable item from a static list using the salted seed.
fn seed_pick<'a>(seed: u64, salt: u64, values: &'a [&'a str]) -> &'a str {
    let max_index = values.len().saturating_sub(1);
    let index = usize::try_from(seed_range_u64(
        seed,
        salt,
        0,
        u64::try_from(max_index).unwrap_or(0),
    ))
    .unwrap_or(0);

    values[index]
}

// Use strong name cues first, then fall back to seeded variety so obviously
// named characters stop getting implausible classes.
fn class_for_name(name: &str, seed: u64) -> &'static str {
    let lower = name.to_ascii_lowercase();
    if lower.contains("wizard") || lower.contains("runecaster") || lower.contains("mantric") {
        return "Wizard";
    }
    if lower.contains("healer") || lower.contains("prophet") {
        return "Cleric";
    }
    if lower.contains("shadow") || lower.contains("swifthand") {
        return "Rogue";
    }
    if lower.contains("nature") {
        return "Druid";
    }
    if lower.contains("barbarian")
        || lower.contains("sternaxe")
        || lower.contains("stonemaiden")
        || lower.contains("gothmog")
    {
        return "Fighter";
    }
    if lower.contains("lion") || lower.contains("valiant") || lower.contains("fearless") {
        return "Paladin";
    }

    seed_pick(seed, 1, &CLASSES)
}

// Backgrounds follow a similar rule: honor obvious social cues before using a
// seeded fallback so the roster reads more like a world than a modulo table.
fn background_for_name(name: &str, seed: u64) -> &'static str {
    let lower = name.to_ascii_lowercase();
    if lower.contains("sir") || lower.contains("duke") {
        return "Noble";
    }
    if lower.contains("shadow") || lower.contains("darkheart") || lower.contains("swifthand") {
        return "Criminal";
    }
    if lower.contains("prophet") || lower.contains("healer") {
        return "Acolyte";
    }
    if lower.contains("wizard") || lower.contains("runecaster") || lower.contains("mantric") {
        return "Sage";
    }
    if lower.contains("barbarian") || lower.contains("fearless") || lower.contains("sternaxe") {
        return "Soldier";
    }

    seed_pick(seed, 2, &BACKGROUNDS)
}

// Pull homeland from explicit name references when they exist so the new column
// is grounded in the existing roster names rather than purely synthetic labels.
fn homeland_for_name(name: &str, seed: u64) -> &'static str {
    let lower = name.to_ascii_lowercase();
    if lower.contains("yaitopya") {
        return "Yaitopya Coast";
    }
    if lower.contains("baldor") {
        return "Baldor Enclave";
    }
    if lower.contains("makan") {
        return "Makan Salt Flats";
    }
    if lower.contains("avalon") {
        return "Avalon Marches";
    }
    if lower.contains("moonwych") {
        return "Moonwych Fens";
    }
    if lower.contains("yinn") {
        return "Yinn Protectorate";
    }
    if lower.contains("banville") {
        return "Banville Reach";
    }

    seed_pick(seed, 3, &HOMELANDS)
}

// Spread levels across a broader adventure curve so query ranges are not tied
// to one visible 1..20 cycle.
fn level_for_seed(seed: u64) -> u16 {
    let rarity = seed_range_u64(seed, 4, 0, 99);
    let level = match rarity {
        0..=11 => seed_range_u64(seed, 5, 2, 5),
        12..=34 => seed_range_u64(seed, 5, 6, 10),
        35..=62 => seed_range_u64(seed, 5, 11, 15),
        63..=83 => seed_range_u64(seed, 5, 16, 20),
        84..=95 => seed_range_u64(seed, 5, 21, 25),
        _ => seed_range_u64(seed, 5, 26, 30),
    };

    u16::try_from(level).unwrap_or(u16::MAX)
}

// Build all six abilities from class/background profiles plus per-character
// variance so related columns still correlate without collapsing into lockstep.
fn build_abilities(
    class_name: &str,
    background: &str,
    level: u16,
    seed: u64,
) -> (i16, i16, i16, i16, i16, i16) {
    let mut stats = [
        seed_range_i16(seed, 10, 7, 13),
        seed_range_i16(seed, 11, 7, 13),
        seed_range_i16(seed, 12, 7, 13),
        seed_range_i16(seed, 13, 7, 13),
        seed_range_i16(seed, 14, 7, 13),
        seed_range_i16(seed, 15, 7, 13),
    ];

    let class_mods = match class_name {
        "Fighter" => [4, 1, 3, 0, 0, 0],
        "Wizard" => [-1, 1, 0, 5, 2, 0],
        "Rogue" => [1, 5, 0, 1, 0, 2],
        "Cleric" => [1, 0, 2, 0, 5, 1],
        "Ranger" => [2, 3, 2, 1, 2, 0],
        "Paladin" => [4, 0, 3, 0, 1, 3],
        "Druid" => [0, 1, 2, 2, 4, 1],
        "Bard" => [0, 2, 1, 2, 1, 5],
        _ => [0; 6],
    };
    let background_mods = match background {
        "Acolyte" => [0, 0, 1, 0, 2, 1],
        "Criminal" => [0, 2, 0, 1, 0, 2],
        "Sage" => [0, 0, 0, 2, 1, 0],
        "Soldier" => [2, 0, 2, 0, 0, 0],
        "Outlander" => [1, 1, 2, 0, 1, 0],
        "Noble" => [0, 0, 0, 1, 0, 2],
        "Guild Artisan" => [0, 1, 0, 2, 0, 1],
        "Hermit" => [0, 0, 1, 1, 2, 0],
        _ => [0; 6],
    };
    let veteran_bonus = if level >= 24 { 1 } else { 0 };

    for (index, stat) in stats.iter_mut().enumerate() {
        let adjusted = *stat + class_mods[index] + background_mods[index] + veteran_bonus;
        *stat = adjusted.clamp(6, 20);
    }

    (stats[0], stats[1], stats[2], stats[3], stats[4], stats[5])
}

// Hit points track class durability, constitution, and level instead of being a
// thin linear transform of one stat.
fn hit_points_for(class_name: &str, level: u16, constitution: i16, seed: u64) -> i32 {
    let hit_die = match class_name {
        "Fighter" | "Paladin" => 10,
        "Ranger" => 9,
        "Cleric" | "Druid" | "Bard" | "Rogue" => 8,
        "Wizard" => 6,
        _ => 8,
    };
    let constitution_mod = ((constitution - 10) / 2).clamp(-1, 5);
    let bonus = i32::try_from(seed_range_u64(seed, 20, 4, 26)).unwrap_or(i32::MAX);

    (i32::from(level) * hit_die) + (i32::from(level) * i32::from(constitution_mod)) + bonus
}

// Armor class uses class armor expectations plus dexterity/intelligence where
// that makes sense, yielding a broader and more believable defense spread.
fn armor_class_for(class_name: &str, dexterity: i16, intelligence: i16, seed: u64) -> u8 {
    let dex_mod = ((dexterity - 10) / 2).clamp(0, 5);
    let int_mod = ((intelligence - 10) / 2).clamp(0, 3);
    let seeded_bonus = i16::try_from(seed_range_u64(seed, 21, 0, 2)).unwrap_or(0);
    let armor_class = match class_name {
        "Fighter" => 15 + dex_mod.clamp(0, 2) + seeded_bonus,
        "Paladin" => 16 + dex_mod.clamp(0, 1) + seeded_bonus,
        "Ranger" => 13 + dex_mod + seeded_bonus,
        "Rogue" => 12 + dex_mod + seeded_bonus,
        "Cleric" => 14 + dex_mod.clamp(0, 2) + seeded_bonus,
        "Druid" => 13 + dex_mod.clamp(0, 2) + seeded_bonus,
        "Bard" => 12 + dex_mod + seeded_bonus,
        "Wizard" => 10 + dex_mod + int_mod + seeded_bonus,
        _ => 12 + dex_mod + seeded_bonus,
    }
    .clamp(10, 22);

    u8::try_from(armor_class).unwrap_or(u8::MAX)
}

// Spell slots stay class-shaped instead of assigning a small positive number to
// every character regardless of whether they cast spells.
fn spell_slots_for(class_name: &str, level: u16, seed: u64) -> u8 {
    let seeded_bonus = u8::try_from(seed_range_u64(seed, 22, 0, 1)).unwrap_or(0);
    match class_name {
        "Wizard" => ((level / 3) + 2).min(9) as u8,
        "Cleric" | "Druid" => (((level / 4) + 2) as u8)
            .saturating_add(seeded_bonus)
            .min(7),
        "Bard" => (((level / 4) + 1) as u8)
            .saturating_add(seeded_bonus)
            .min(6),
        "Paladin" | "Ranger" => {
            if level < 4 {
                0
            } else {
                (((level / 5) + 1) as u8)
                    .saturating_add(seeded_bonus)
                    .min(4)
            }
        }
        "Rogue" if level >= 23 && seeded_bonus == 1 => 1,
        "Fighter" if level >= 26 && seeded_bonus == 1 => 1,
        _ => 0,
    }
}

// Initiative comes from dexterity first, with light class seasoning so faster
// archetypes reliably surface in top-k ordering queries.
fn initiative_bonus_for(class_name: &str, dexterity: i16, seed: u64) -> i8 {
    let dex_mod = ((dexterity - 10) / 2).clamp(-1, 5);
    let class_bonus = match class_name {
        "Rogue" | "Ranger" => 2,
        "Bard" => 1,
        _ => 0,
    };
    let seeded_bonus = seed_range_i16(seed, 23, -1, 1);
    let initiative_bonus = (dex_mod + class_bonus + seeded_bonus).clamp(-1, 8);

    i8::try_from(initiative_bonus).unwrap_or(i8::MAX)
}

// Critical chance is stored as a decimal percentage and now varies by combat
// style rather than repeating one tiny stepped pattern every ten rows.
fn critical_chance_for(class_name: &str, strength: i16, dexterity: i16, seed: u64) -> Decimal {
    let strength_mod = ((strength - 10) / 2).clamp(0, 5);
    let dex_mod = ((dexterity - 10) / 2).clamp(0, 5);
    let base = match class_name {
        "Rogue" => 12 + dex_mod,
        "Ranger" => 10 + dex_mod,
        "Fighter" => 9 + strength_mod,
        "Paladin" => 8 + strength_mod,
        "Bard" => 7 + dex_mod,
        "Cleric" | "Druid" => 6 + strength_mod.clamp(0, 2),
        "Wizard" => 4 + dex_mod.clamp(0, 2),
        _ => 6,
    };
    let swing = i16::try_from(seed_range_u64(seed, 24, 0, 4)).unwrap_or(0);
    let hundredths = (base + swing).clamp(4, 28);

    Decimal::new(i64::from(hundredths), 2)
}

// Dodge chance remains a float but now reflects dexterity, armor trade-offs,
// and class mobility instead of one visible arithmetic progression.
fn dodge_chance_for(class_name: &str, dexterity: i16, armor_class: u8, seed: u64) -> Float64 {
    let dex_mod = ((dexterity - 10) / 2).clamp(0, 5);
    let class_bonus = match class_name {
        "Rogue" => 0.11,
        "Ranger" => 0.08,
        "Bard" => 0.05,
        "Wizard" => 0.03,
        "Druid" => 0.04,
        "Cleric" => 0.02,
        "Fighter" => 0.01,
        "Paladin" => 0.0,
        _ => 0.02,
    };
    let armor_penalty = f64::from(armor_class.saturating_sub(12)) * 0.0065;
    let seeded_bonus =
        f64::from(u32::try_from(seed_range_u64(seed, 25, 0, 8)).unwrap_or(0)) * 0.006;
    let dodge_chance = (0.04 + (f64::from(dex_mod) * 0.028) + class_bonus + seeded_bonus
        - armor_penalty)
        .clamp(0.04, 0.39);

    Float64::try_new(dodge_chance).unwrap_or_default()
}

// NPC assignment is deterministic but no longer every sixth row.
fn is_npc_for(name: &str, seed: u64) -> bool {
    let lower = name.to_ascii_lowercase();
    if lower.contains("gothmog") || lower.contains("wuuf") || lower.contains("hissssa") {
        return true;
    }

    seed_range_u64(seed, 26, 0, 99) < 22
}

// Renown adds a second social/progression axis that is only loosely correlated
// with level, making range and grouping queries less repetitive.
fn renown_for(class_name: &str, background: &str, level: u16, is_npc: bool, seed: u64) -> i16 {
    let class_mod = match class_name {
        "Paladin" => 8,
        "Cleric" => 5,
        "Bard" => 4,
        "Fighter" => 3,
        "Ranger" => 2,
        "Druid" => 1,
        "Wizard" => 1,
        "Rogue" => -2,
        _ => 0,
    };
    let background_mod = match background {
        "Noble" => 12,
        "Acolyte" => 7,
        "Guild Artisan" => 5,
        "Soldier" => 4,
        "Sage" => 3,
        "Outlander" => 1,
        "Hermit" => -2,
        "Criminal" => -10,
        _ => 0,
    };
    let seeded_swing = seed_range_i16(seed, 27, -12, 16);
    let npc_penalty = if is_npc { 7 } else { 0 };

    ((i16::try_from(level).unwrap_or(i16::MAX) * 4) - 18
        + class_mod
        + background_mod
        + seeded_swing
        - npc_penalty)
        .clamp(-20, 125)
}

// Guild membership now depends on social standing and archetype instead of one
// fixed every-fifth-row null pattern.
fn guild_rank_for(background: &str, renown: i16, is_npc: bool, seed: u64) -> Option<String> {
    if matches!(background, "Hermit" | "Criminal")
        && renown < 28
        && seed_range_u64(seed, 28, 0, 99) < 55
    {
        return None;
    }
    if is_npc && renown < 18 && seed_range_u64(seed, 29, 0, 99) < 40 {
        return None;
    }
    if renown < 10 {
        return None;
    }

    let rank = match renown {
        10..=29 => GUILD_RANKS[0],
        30..=54 => GUILD_RANKS[1],
        55..=84 => GUILD_RANKS[2],
        _ => GUILD_RANKS[3],
    };

    Some(rank.to_string())
}

// Mentor principals remain optional, but some non-null rows now vary instead of
// collapsing onto the anonymous principal.
fn mentor_principal_for(name: &str, background: &str, level: u16, seed: u64) -> Option<Principal> {
    if background == "Hermit" || level >= 26 || seed_range_u64(seed, 30, 0, 99) < 24 {
        return None;
    }

    let mut bytes = [0u8; 29];
    let mut rolling = seed ^ character_seed(0, name);
    for byte in &mut bytes {
        rolling = seed_step(rolling, 31);
        *byte = u8::try_from(rolling & 0xff).unwrap_or_default();
    }

    Some(Principal::from_slice(&bytes))
}

// Resistances get one archetype anchor plus extra seeded variety, with duplicate
// elimination so array-valued queries see more realistic combinations.
fn build_resistances(class_name: &str, background: &str, seed: u64) -> Vec<String> {
    let mut resistances = Vec::new();
    push_unique(
        &mut resistances,
        match class_name {
            "Wizard" => "psychic",
            "Cleric" => "radiant",
            "Druid" => "poison",
            "Paladin" => "force",
            "Rogue" => "necrotic",
            "Ranger" => "cold",
            "Bard" => "lightning",
            "Fighter" => "fire",
            _ => RESISTANCES[0],
        },
    );
    push_unique(
        &mut resistances,
        match background {
            "Acolyte" => "radiant",
            "Criminal" => "necrotic",
            "Sage" => "psychic",
            "Soldier" => "fire",
            "Outlander" => "cold",
            "Noble" => "force",
            "Guild Artisan" => "lightning",
            "Hermit" => "poison",
            _ => RESISTANCES[1],
        },
    );
    if seed_range_u64(seed, 32, 0, 99) < 45 {
        push_unique(&mut resistances, seed_pick(seed, 33, &RESISTANCES));
    }

    resistances
}

// Inventory arrays now vary in length and mass, which makes array aggregation
// and inspection queries less repetitive.
fn build_inventory_weights(class_name: &str, level: u16, seed: u64) -> Vec<u16> {
    let item_count = usize::try_from(seed_range_u64(seed, 34, 3, 6)).unwrap_or(3);
    let class_bias = match class_name {
        "Fighter" | "Paladin" => 18u64,
        "Ranger" | "Cleric" | "Druid" => 12,
        "Bard" | "Rogue" => 8,
        "Wizard" => 5,
        _ => 10,
    };
    let level_bias = u64::from(level / 2);

    (0..item_count)
        .map(|offset| {
            let salt = 35 + u64::try_from(offset).unwrap_or(0);
            let weight = seed_range_u64(seed, salt, 2, 38) + class_bias + level_bias;
            u16::try_from(weight.min(u64::from(u16::MAX))).unwrap_or(u16::MAX)
        })
        .collect()
}

// Portrait bytes only need to be deterministic blobs, but using more bytes
// makes them look less like a toy RGB triplet in debugging output.
fn portrait_for(seed: u64) -> Vec<u8> {
    (0..8)
        .map(|offset| {
            let salt = 50 + u64::try_from(offset).unwrap_or(0);
            u8::try_from(seed_step(seed, salt) & 0xff).unwrap_or_default()
        })
        .collect()
}

// Rest times now vary by days and hours, not one-minute increments.
fn last_rest_at_for(level: u16, background: &str, seed: u64) -> u64 {
    let base_ms = 1_714_000_000_000u64;
    let day_span = match background {
        "Soldier" | "Outlander" => 28,
        "Hermit" => 60,
        _ => 40,
    };
    let days_ago = seed_range_u64(seed, 60, 0, day_span);
    let hours_ago = seed_range_u64(seed, 61, 0, 23);
    let level_adjustment = u64::from(level / 3) * 3_600_000;

    base_ms
        .saturating_sub(days_ago * 86_400_000)
        .saturating_sub(hours_ago * 3_600_000)
        .saturating_sub(level_adjustment)
}

// Respawn cooldown spans minutes to hours so this column is useful for more
// than a tiny stepped integer demo.
fn respawn_cooldown_for(level: u16, is_npc: bool, seed: u64) -> u64 {
    let base_seconds = if is_npc { 180 } else { 420 };
    let level_seconds = u64::from(level) * 110;
    let swing_seconds = seed_range_u64(seed, 62, 0, 7_200);

    base_seconds + level_seconds + swing_seconds
}

// Experience grows non-linearly with level and renown so rows stop reading as a
// simple linear offset from one other field.
fn experience_for(level: u16, renown: i16, seed: u64) -> u64 {
    let level_u64 = u64::from(level);
    let renown_bonus = u64::try_from(renown.max(0)).unwrap_or_default() * 180;
    let seeded_bonus = seed_range_u64(seed, 63, 0, 18_000);

    (level_u64.pow(3) * 340) + (level_u64.pow(2) * 110) + renown_bonus + seeded_bonus
}

// Gold varies with background, level, and social standing instead of one fixed
// arithmetic increment.
fn gold_pieces_for(background: &str, level: u16, renown: i16, seed: u64) -> u32 {
    let background_base = match background {
        "Noble" => 1_800u64,
        "Guild Artisan" => 1_050,
        "Soldier" => 700,
        "Sage" => 520,
        "Acolyte" => 460,
        "Outlander" => 320,
        "Hermit" => 180,
        "Criminal" => 260,
        _ => 300,
    };
    let gold = background_base
        + (u64::from(level) * seed_range_u64(seed, 64, 55, 240))
        + (u64::try_from(renown.max(0)).unwrap_or_default() * 14)
        + seed_range_u64(seed, 65, 0, 2_800);

    u32::try_from(gold.min(u64::from(u32::MAX))).unwrap_or(u32::MAX)
}

// Descriptions mention the new homeland/renown fields and avoid the old
// frontline/control binary wording that made the rows feel synthetic.
fn description_for(
    name: &str,
    class_name: &str,
    background: &str,
    homeland: &str,
    level: u16,
    renown: i16,
) -> String {
    let specialty = match class_name {
        "Fighter" => "shield-breaking duels",
        "Wizard" => "counter-spell discipline",
        "Rogue" => "quiet knife work",
        "Cleric" => "battlefield blessings",
        "Ranger" => "trail ambushes",
        "Paladin" => "oath-bound vanguard work",
        "Druid" => "weather and rootcraft",
        "Bard" => "courtly manipulation",
        _ => "field work",
    };
    let reputation = if renown >= 75 {
        "widely celebrated"
    } else if renown >= 35 {
        "locally respected"
    } else if renown >= 0 {
        "still proving themself"
    } else {
        "better known in whispers than songs"
    };

    format!(
        "{name} is a level {level} {class_name} from {homeland}, shaped by a {background} past and known for {specialty}; they are {reputation}."
    )
}

// Keep the resistance list unique without allocating a set for this tiny fixture.
fn push_unique(values: &mut Vec<String>, candidate: &str) {
    if values.iter().any(|existing| existing == candidate) {
        return;
    }

    values.push(candidate.to_string());
}
