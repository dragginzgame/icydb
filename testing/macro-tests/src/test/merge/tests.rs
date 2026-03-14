use super::*;
use icydb::{
    error::{ErrorKind, ErrorOrigin, PatchError, UpdateErrorKind},
    patch::{ListPatch, MapPatch, SetPatch},
    traits::{AsView, Collection, Create, MapCollection, Update, UpdateView, View},
};
use std::collections::{HashMap, HashSet};

type Patch<T> = Update<T>;

fn profile(bio: &str, visits: u32, favorites: &[u32]) -> MergeProfile {
    MergeProfile {
        bio: bio.into(),
        visits,
        favorite_numbers: favorites.to_vec(),
    }
}

#[test]
#[expect(clippy::field_reassign_with_default)]
fn entity_merge_updates_nested_structures() {
    let mut entity: MergeEntity = Create::<MergeEntity> {
        name: "original".into(),
        score: 7,
        nickname: None,
        scores: vec![1, 2, 3],
        tags: vec!["red".to_string(), "blue".to_string()],
        settings: vec![("volume".to_string(), 10u32), ("remove".to_string(), 5u32)],
        profile: profile("quiet", 1, &[10, 11]).as_view(),
        wrapper: profile("nested", 3, &[42]).as_view(),
        tuple_field: ("alpha".to_string(), 1),
        opt_profile: None,
    }
    .into();

    let mut update: Patch<MergeEntity> = Default::default();
    update.name = Some("updated".into());
    update.nickname = Some(Some("nick".into()));
    update.scores = Some(vec![
        ListPatch::Update {
            index: 0,
            patch: 10,
        },
        ListPatch::Update {
            index: 1,
            patch: 20,
        },
    ]);
    update.tags = Some(vec![SetPatch::Clear, SetPatch::Insert("green".to_string())]);
    update.profile = Some(Update::<MergeProfile> {
        visits: Some(10),
        ..Default::default()
    });
    update.wrapper = Some(Update::<MergeWrapper> {
        bio: Some("outer".into()),
        ..Default::default()
    });
    update.tuple_field = Some((Some("omega".into()), Some(99)));
    update.opt_profile = Some(Some(Update::<MergeProfile> {
        bio: Some("loaded".into()),
        visits: Some(2),
        favorite_numbers: None,
    }));

    entity
        .merge(update)
        .expect("entity merge update should succeed");

    let view: View<MergeEntity> = entity.as_view();
    assert_eq!(view.name, "updated");
    assert_eq!(view.nickname.as_deref(), Some("nick"));
    assert_eq!(view.scores, vec![10, 20, 3]);

    let tags: HashSet<_> = view.tags.iter().cloned().collect();
    let expected_tags: HashSet<_> = vec!["green".to_string()].into_iter().collect();
    assert_eq!(tags, expected_tags);

    let settings: HashMap<String, u32> = view.settings.iter().cloned().collect();
    assert_eq!(settings.get("volume"), Some(&10));
    assert_eq!(settings.get("remove"), Some(&5));

    assert_eq!(view.profile.visits, 10);
    assert_eq!(view.wrapper.bio, "outer");
    assert_eq!(view.tuple_field.0, "omega");
    assert_eq!(view.tuple_field.1, 99);
    let opt_profile = view.opt_profile.as_ref().expect("opt_profile should exist");
    assert_eq!(opt_profile.bio, "loaded");
    assert_eq!(opt_profile.visits, 2);
    assert!(opt_profile.favorite_numbers.is_empty());
}

#[test]
#[expect(clippy::default_trait_access)]
#[expect(clippy::field_reassign_with_default)]
fn entity_merge_clears_optional_fields() {
    let mut entity: MergeEntity = Create::<MergeEntity> {
        name: "original".into(),
        score: 7,
        nickname: Some("nick".into()),
        scores: vec![1, 2, 3],
        tags: vec!["red".to_string(), "blue".to_string()],
        settings: vec![("volume".to_string(), 10u32)],
        profile: profile("quiet", 1, &[10, 11]).as_view(),
        wrapper: profile("nested", 3, &[42]).as_view(),
        tuple_field: ("alpha".to_string(), 1),
        opt_profile: Some(profile("opt", 9, &[1]).as_view()),
    }
    .into();

    // Leaving an option unset in the update should not change it.
    let update: Patch<MergeEntity> = Default::default();
    entity
        .merge(update)
        .expect("entity merge update should preserve existing optional values");
    let view: View<MergeEntity> = entity.as_view();
    assert_eq!(view.nickname.as_deref(), Some("nick"));
    assert!(view.opt_profile.is_some());

    // Setting `Some(None)` should clear the optional field.
    let mut update: Patch<MergeEntity> = Default::default();
    update.nickname = Some(None);
    update.opt_profile = Some(None);
    entity
        .merge(update)
        .expect("entity merge should clear optional fields when explicitly requested");

    let view: View<MergeEntity> = entity.as_view();
    assert!(view.nickname.is_none());
    assert!(view.opt_profile.is_none());
}

#[test]
#[expect(clippy::field_reassign_with_default)]
fn record_merge_preserves_unset_fields() {
    let mut profile = profile("start", 1, &[1, 2, 3]);
    let mut update: Patch<MergeProfile> = Default::default();
    update.bio = Some("updated".into());
    profile
        .merge(update)
        .expect("record merge should apply partial update");

    let view: View<MergeProfile> = profile.as_view();
    assert_eq!(view.bio, "updated");
    assert_eq!(view.visits, 1);
    assert_eq!(view.favorite_numbers, vec![1, 2, 3]);
}

#[test]
fn map_and_set_merge_behaviors() {
    let mut tags = MergeTags::from(vec!["old".to_string(), "stale".to_string()]);
    tags.merge(vec![
        SetPatch::Clear,
        SetPatch::Insert("fresh".to_string()),
        SetPatch::Insert("new".to_string()),
    ])
    .expect("set patch merge should succeed");
    let tag_set: HashSet<_> = tags.iter().cloned().collect();
    let expected: HashSet<_> = vec!["fresh".to_string(), "new".to_string()]
        .into_iter()
        .collect();
    assert_eq!(tag_set, expected);

    let mut settings =
        MergeSettings::from(vec![("keep".to_string(), 1u32), ("drop".to_string(), 2u32)]);
    let patch: Patch<MergeSettings> = vec![
        MapPatch::Remove {
            key: "drop".to_string(),
        },
        MapPatch::Insert {
            key: "fresh".to_string(),
            value: 9u32,
        },
        MapPatch::Replace {
            key: "keep".to_string(),
            value: 7u32,
        },
    ];
    settings
        .merge(patch)
        .expect("map patch merge should succeed");

    let settings: HashMap<String, u32> = settings.iter().map(|(k, v)| (k.clone(), *v)).collect();
    assert_eq!(settings.get("keep"), Some(&7u32));
    assert_eq!(settings.get("fresh"), Some(&9u32));
    assert!(!settings.contains_key("drop"));
}

#[test]
#[expect(clippy::field_reassign_with_default)]
fn entity_merge_overwrites_collections() {
    let mut entity: MergeEntity = Create::<MergeEntity> {
        name: "reset".into(),
        score: 2,
        nickname: None,
        scores: vec![1, 2, 3],
        tags: vec!["old".to_string(), "stale".to_string()],
        settings: vec![("keep".to_string(), 1u32)],
        profile: profile("overwrite", 0, &[]).as_view(),
        wrapper: profile("wrapper", 0, &[]).as_view(),
        tuple_field: ("tuple".to_string(), 0),
        opt_profile: None,
    }
    .into();

    let mut update: Patch<MergeEntity> = Default::default();
    update.scores = Some(vec![ListPatch::Overwrite {
        values: vec![9u32, 8, 7],
    }]);
    update.tags = Some(vec![SetPatch::Overwrite {
        values: vec!["fresh".to_string(), "new".to_string()],
    }]);

    entity
        .merge(update)
        .expect("entity merge overwrite patch should succeed");

    let view: View<MergeEntity> = entity.as_view();
    assert_eq!(view.scores, vec![9, 8, 7]);

    let tags: HashSet<_> = view.tags.iter().cloned().collect();
    let expected_tags: HashSet<_> = vec!["fresh".to_string(), "new".to_string()]
        .into_iter()
        .collect();
    assert_eq!(tags, expected_tags);

    let settings: HashMap<String, u32> = view.settings.iter().cloned().collect();
    assert_eq!(settings.get("keep"), Some(&1));
}

#[test]
#[expect(clippy::field_reassign_with_default)]
fn entity_merge_wraps_patch_errors_with_field_path() {
    let mut entity: MergeEntity = Create::<MergeEntity> {
        settings: vec![("keep".to_string(), 1u32)],
        ..Default::default()
    }
    .into();

    let mut update: Patch<MergeEntity> = Default::default();
    update.settings = Some(vec![
        MapPatch::Insert {
            key: "keep".to_string(),
            value: 2u32,
        },
        MapPatch::Replace {
            key: "keep".to_string(),
            value: 3u32,
        },
    ]);

    let err = entity
        .merge(update)
        .expect_err("duplicate map-key operations should fail and preserve field path");

    assert_eq!(err.origin(), ErrorOrigin::Interface);
    assert!(err.message().contains("settings[0]"));
    assert!(matches!(
        err.kind(),
        ErrorKind::Update(UpdateErrorKind::Patch(PatchError::InvalidShape))
    ));
}
