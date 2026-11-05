use super::{ListValidated, MapValidated, SetValidated};

#[test]
fn generated_collections_follow_standard_iteration_protocols() {
    let mut list: ListValidated = [1_u8, 2].into_iter().collect();
    assert_eq!((&list).into_iter().copied().collect::<Vec<_>>(), [1, 2]);
    for value in &mut list {
        *value += 1;
    }
    assert_eq!(list.into_iter().collect::<Vec<_>>(), [2, 3]);

    let set: SetValidated = [2_u8, 1, 2].into_iter().collect();
    assert_eq!((&set).into_iter().copied().collect::<Vec<_>>(), [1, 2]);
    assert_eq!(set.into_iter().collect::<Vec<_>>(), [1, 2]);

    let mut map: MapValidated = [(2_u8, 20_u8), (1, 10)].into_iter().collect();
    for (_, value) in &mut map {
        *value += 1;
    }
    assert_eq!(
        (&map)
            .into_iter()
            .map(|(key, value)| (*key, *value))
            .collect::<Vec<_>>(),
        [(1, 11), (2, 21)],
    );
    assert_eq!(map.into_iter().collect::<Vec<_>>(), [(1, 11), (2, 21)]);
}
