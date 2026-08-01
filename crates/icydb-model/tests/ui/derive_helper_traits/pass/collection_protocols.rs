use icydb_model::prelude::*;

#[list(item(prim = "Nat16"))]
pub struct Numbers {}

#[set(item(prim = "Text", unbounded))]
pub struct Labels {}

#[map(
    key(prim = "Nat8"),
    value(item(prim = "Text", unbounded))
)]
pub struct Names {}

#[newtype(item(prim = "Text", unbounded))]
pub struct TextValue {}

#[list(
    item(prim = "Nat8"),
    traits(remove(FromIterator, IntoIterator))
)]
pub struct ManualCollection {}

struct Narrow(u8);

impl From<Narrow> for u16 {
    fn from(value: Narrow) -> Self {
        u16::from(value.0)
    }
}

struct Word(&'static str);

impl From<Word> for String {
    fn from(value: Word) -> Self {
        value.0.to_string()
    }
}

impl FromIterator<u8> for ManualCollection {
    fn from_iter<T: IntoIterator<Item = u8>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl IntoIterator for ManualCollection {
    type Item = u8;
    type IntoIter = std::vec::IntoIter<u8>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

fn main() {
    let mut numbers: Numbers = [1, 2].into_iter().collect();
    let _shared_total: u16 = (&numbers).into_iter().copied().sum();
    for value in &mut numbers {
        *value += 1;
    }
    let _owned_numbers: Vec<u16> = numbers.into_iter().collect();

    let labels: Labels = [String::from("a"), String::from("b")]
        .into_iter()
        .collect();
    let _shared_labels: Vec<&String> = (&labels).into_iter().collect();
    let _owned_labels: Vec<String> = labels.into_iter().collect();

    let mut names: Names = [(1, String::from("one"))].into_iter().collect();
    for (_, value) in &mut names {
        value.push('!');
    }
    let _shared_names: Vec<(&u8, &String)> = (&names).into_iter().collect();
    let _owned_names: Vec<(u8, String)> = names.into_iter().collect();

    let _broad_collection_from: Numbers = vec![Narrow(3)].into();
    let _broad_newtype_from: TextValue = Word("value").into();

    let manual: ManualCollection = [1_u8, 2].into_iter().collect();
    let _manual_values: Vec<u8> = manual.into_iter().collect();
}
