use super::*;

#[test]
fn serialize_is_required_for_generated_application_values() {
    let baseline = application_type_trait_set();
    assert!(baseline.contains(TraitKind::Serialize));
    let builder = TraitBuilder {
        add: TraitListMeta(vec![TraitKind::Serialize]),
        remove: TraitListMeta::default(),
    };
    assert!(
        builder
            .validate_for_type(ApplicationTypeKind::Record, baseline)
            .is_err()
    );
}

#[test]
fn required_type_traits_are_emitted_without_author_directives() {
    let traits = TraitBuilder::default().build_for_type(application_type_trait_set());

    for required in REQUIRED_TYPE_TRAITS {
        assert!(traits.contains(*required));
    }
}

#[test]
fn required_type_traits_reject_explicit_addition_and_removal() {
    for required in REQUIRED_TYPE_TRAITS.iter().copied() {
        let adds_required = TraitBuilder {
            add: TraitListMeta(vec![required]),
            remove: TraitListMeta::default(),
        };
        assert!(
            adds_required
                .validate_for_type(ApplicationTypeKind::Entity, application_type_trait_set(),)
                .is_err()
        );

        let removes_required = TraitBuilder {
            add: TraitListMeta::default(),
            remove: TraitListMeta(vec![required]),
        };
        assert!(
            removes_required
                .validate_for_type(ApplicationTypeKind::Entity, application_type_trait_set(),)
                .is_err()
        );
    }
}

#[test]
fn optional_and_override_type_traits_remain_configurable() {
    for configurable in [TraitKind::NormalizeCustom, TraitKind::ValidateCustom] {
        let builder = TraitBuilder {
            add: TraitListMeta::default(),
            remove: TraitListMeta(vec![configurable]),
        };
        assert!(
            builder
                .validate_for_type(ApplicationTypeKind::Entity, application_type_trait_set(),)
                .is_ok()
        );
        assert!(
            !builder
                .build_for_type(application_type_trait_set())
                .0
                .contains(&configurable)
        );
    }

    let default = TraitBuilder {
        add: TraitListMeta(vec![TraitKind::Default]),
        remove: TraitListMeta::default(),
    };
    assert!(
        default
            .validate_for_type(ApplicationTypeKind::Entity, application_type_trait_set(),)
            .is_ok()
    );
    assert!(
        default
            .build_for_type(application_type_trait_set())
            .0
            .contains(&TraitKind::Default)
    );
}

#[test]
fn shape_baseline_is_applied_before_author_directives() {
    let mut baseline = application_type_trait_set();
    baseline.extend([
        TraitKind::Default,
        TraitKind::Deref,
        TraitKind::DerefMut,
        TraitKind::From,
        TraitKind::FromIterator,
        TraitKind::IntoIterator,
    ]);

    let remove_deref = TraitBuilder {
        add: TraitListMeta::default(),
        remove: TraitListMeta(vec![TraitKind::Deref]),
    };
    assert!(
        remove_deref
            .validate_for_type(ApplicationTypeKind::List, baseline.clone())
            .is_ok()
    );
    assert!(
        !remove_deref
            .build_for_type(baseline.clone())
            .0
            .contains(&TraitKind::Deref)
    );

    let add_default = TraitBuilder {
        add: TraitListMeta(vec![TraitKind::Default]),
        remove: TraitListMeta::default(),
    };
    assert!(
        add_default
            .validate_for_type(ApplicationTypeKind::List, baseline)
            .is_err()
    );
}

#[test]
fn generated_custom_policy_is_node_specific() {
    let baseline = application_type_trait_set();
    let application_nodes = [
        ApplicationTypeKind::Entity,
        ApplicationTypeKind::Enum,
        ApplicationTypeKind::List,
        ApplicationTypeKind::Map,
        ApplicationTypeKind::Newtype,
        ApplicationTypeKind::Record,
        ApplicationTypeKind::Set,
        ApplicationTypeKind::Tuple,
    ];

    for node_kind in application_nodes {
        for trait_kind in [TraitKind::NormalizeCustom, TraitKind::ValidateCustom] {
            assert_eq!(
                node_kind.configurable_trait_policy(trait_kind, &baseline),
                Some(ConfigurableTraitPolicy::GeneratedOverrideable),
            );
        }

        let wrapper_policy = if matches!(
            node_kind,
            ApplicationTypeKind::List
                | ApplicationTypeKind::Map
                | ApplicationTypeKind::Newtype
                | ApplicationTypeKind::Set
        ) {
            ConfigurableTraitPolicy::GeneratedOverrideable
        } else {
            ConfigurableTraitPolicy::Unsupported
        };
        assert_eq!(
            node_kind.configurable_trait_policy(TraitKind::From, &baseline),
            Some(wrapper_policy),
        );

        let newtype_policy = if matches!(node_kind, ApplicationTypeKind::Newtype) {
            ConfigurableTraitPolicy::GeneratedOverrideable
        } else {
            ConfigurableTraitPolicy::Unsupported
        };
        assert_eq!(
            node_kind.configurable_trait_policy(TraitKind::Inner, &baseline),
            Some(newtype_policy),
        );

        let numeric_policy = if matches!(node_kind, ApplicationTypeKind::Newtype) {
            ConfigurableTraitPolicy::OptIn
        } else {
            ConfigurableTraitPolicy::Unsupported
        };
        assert_eq!(
            node_kind.configurable_trait_policy(TraitKind::NumericValue, &baseline),
            Some(numeric_policy),
        );
    }

    let mut numeric_baseline = baseline;
    numeric_baseline.add(TraitKind::NumericValue);
    assert_eq!(
        ApplicationTypeKind::Newtype
            .configurable_trait_policy(TraitKind::NumericValue, &numeric_baseline),
        Some(ConfigurableTraitPolicy::GeneratedOverrideable),
    );
}

#[test]
fn configurable_policy_covers_every_non_required_trait() {
    let baseline = application_type_trait_set();
    let application_nodes = [
        ApplicationTypeKind::Entity,
        ApplicationTypeKind::Enum,
        ApplicationTypeKind::List,
        ApplicationTypeKind::Map,
        ApplicationTypeKind::Newtype,
        ApplicationTypeKind::Record,
        ApplicationTypeKind::Set,
        ApplicationTypeKind::Tuple,
    ];

    let complete_inventory: BTreeSet<_> = REQUIRED_TYPE_TRAITS
        .iter()
        .chain(CONFIGURABLE_TYPE_TRAITS.iter())
        .copied()
        .collect();
    assert_eq!(REQUIRED_TYPE_TRAITS.len(), 10);
    assert_eq!(CONFIGURABLE_TYPE_TRAITS.len(), 28);
    assert_eq!(complete_inventory.len(), 38);

    for node_kind in application_nodes {
        for trait_kind in CONFIGURABLE_TYPE_TRAITS {
            assert!(
                node_kind
                    .configurable_trait_policy(*trait_kind, &baseline)
                    .is_some(),
                "missing {node_kind:?}/{trait_kind:?} policy",
            );
        }
        for trait_kind in REQUIRED_TYPE_TRAITS {
            assert!(
                node_kind
                    .configurable_trait_policy(*trait_kind, &baseline)
                    .is_none(),
                "required {node_kind:?}/{trait_kind:?} entered configurable policy",
            );
        }
    }
}

#[test]
fn fixed_generated_nodes_do_not_enter_application_trait_resolution() {
    assert_eq!(
        generated_node_trait_set().into_vec(),
        vec![TraitKind::Clone, TraitKind::Debug, TraitKind::Path],
    );
}

#[test]
fn derive_and_helper_policies_follow_node_and_baseline() {
    let base = application_type_trait_set();

    for collection in [
        ApplicationTypeKind::List,
        ApplicationTypeKind::Map,
        ApplicationTypeKind::Set,
    ] {
        assert_eq!(
            collection.configurable_trait_policy(TraitKind::Default, &base),
            Some(ConfigurableTraitPolicy::GeneratedOverrideable),
        );
    }
    for opt_in in [
        ApplicationTypeKind::Entity,
        ApplicationTypeKind::Enum,
        ApplicationTypeKind::Newtype,
        ApplicationTypeKind::Record,
        ApplicationTypeKind::Tuple,
    ] {
        assert_eq!(
            opt_in.configurable_trait_policy(TraitKind::Default, &base),
            Some(ConfigurableTraitPolicy::OptIn),
        );
    }

    for standard in [
        TraitKind::Copy,
        TraitKind::Hash,
        TraitKind::Ord,
        TraitKind::PartialOrd,
    ] {
        assert_eq!(
            ApplicationTypeKind::Record.configurable_trait_policy(standard, &base),
            Some(ConfigurableTraitPolicy::OptIn),
        );
        let mut generated = base.clone();
        generated.add(standard);
        assert_eq!(
            ApplicationTypeKind::Enum.configurable_trait_policy(standard, &generated),
            Some(ConfigurableTraitPolicy::GeneratedOverrideable),
        );
    }

    for dereference in [TraitKind::Deref, TraitKind::DerefMut] {
        assert_eq!(
            ApplicationTypeKind::List.configurable_trait_policy(dereference, &base),
            Some(ConfigurableTraitPolicy::GeneratedOverrideable),
        );
        assert_eq!(
            ApplicationTypeKind::Newtype.configurable_trait_policy(dereference, &base),
            Some(ConfigurableTraitPolicy::OptIn),
        );
        assert_eq!(
            ApplicationTypeKind::Record.configurable_trait_policy(dereference, &base),
            Some(ConfigurableTraitPolicy::Unsupported),
        );
    }

    assert_eq!(
        ApplicationTypeKind::Newtype.configurable_trait_policy(TraitKind::Display, &base),
        Some(ConfigurableTraitPolicy::OptIn),
    );
    assert_eq!(
        ApplicationTypeKind::Entity.configurable_trait_policy(TraitKind::Display, &base),
        Some(ConfigurableTraitPolicy::Unsupported),
    );

    for arithmetic in [
        TraitKind::Add,
        TraitKind::AddAssign,
        TraitKind::Div,
        TraitKind::DivAssign,
        TraitKind::Mul,
        TraitKind::MulAssign,
        TraitKind::Neg,
        TraitKind::Product,
        TraitKind::Rem,
        TraitKind::RemAssign,
        TraitKind::Sub,
        TraitKind::SubAssign,
        TraitKind::Sum,
    ] {
        assert_eq!(
            ApplicationTypeKind::Newtype.configurable_trait_policy(arithmetic, &base),
            Some(ConfigurableTraitPolicy::OptIn),
        );
        let mut generated = base.clone();
        generated.add(arithmetic);
        assert_eq!(
            ApplicationTypeKind::Newtype.configurable_trait_policy(arithmetic, &generated),
            Some(ConfigurableTraitPolicy::GeneratedOverrideable),
        );
        assert_eq!(
            ApplicationTypeKind::Tuple.configurable_trait_policy(arithmetic, &base),
            Some(ConfigurableTraitPolicy::Unsupported),
        );
    }
}

#[test]
fn collection_protocols_are_generated_only_for_collection_nodes() {
    let base = application_type_trait_set();
    let protocols = [TraitKind::FromIterator, TraitKind::IntoIterator];

    for collection in [
        ApplicationTypeKind::List,
        ApplicationTypeKind::Map,
        ApplicationTypeKind::Set,
    ] {
        let mut baseline = base.clone();
        baseline.extend(protocols);
        for protocol in protocols {
            assert_eq!(
                collection.configurable_trait_policy(protocol, &baseline),
                Some(ConfigurableTraitPolicy::GeneratedOverrideable),
            );
        }
    }

    for other in [
        ApplicationTypeKind::Entity,
        ApplicationTypeKind::Enum,
        ApplicationTypeKind::Newtype,
        ApplicationTypeKind::Record,
        ApplicationTypeKind::Tuple,
    ] {
        for protocol in protocols {
            assert_eq!(
                other.configurable_trait_policy(protocol, &base),
                Some(ConfigurableTraitPolicy::Unsupported),
            );
        }
    }
}

#[test]
fn generated_custom_directives_follow_the_closed_policy() {
    let mut newtype_baseline = application_type_trait_set();
    newtype_baseline.extend([TraitKind::From, TraitKind::Inner]);

    let remove_generated = TraitBuilder {
        add: TraitListMeta::default(),
        remove: TraitListMeta(vec![
            TraitKind::From,
            TraitKind::Inner,
            TraitKind::NormalizeCustom,
            TraitKind::ValidateCustom,
        ]),
    };
    assert!(
        remove_generated
            .validate_for_type(ApplicationTypeKind::Newtype, newtype_baseline.clone())
            .is_ok()
    );

    let add_opt_in_numeric = TraitBuilder {
        add: TraitListMeta(vec![TraitKind::NumericValue]),
        remove: TraitListMeta::default(),
    };
    assert!(
        add_opt_in_numeric
            .validate_for_type(ApplicationTypeKind::Newtype, newtype_baseline.clone())
            .is_ok()
    );

    let remove_opt_in_numeric = TraitBuilder {
        add: TraitListMeta::default(),
        remove: TraitListMeta(vec![TraitKind::NumericValue]),
    };
    let error = remove_opt_in_numeric
        .validate_for_type(ApplicationTypeKind::Newtype, newtype_baseline)
        .expect_err("an absent opt-in cannot be removed");
    assert!(error.to_string().contains("is not generated for newtype"));

    let add_unsupported_inner = TraitBuilder {
        add: TraitListMeta(vec![TraitKind::Inner]),
        remove: TraitListMeta::default(),
    };
    let error = add_unsupported_inner
        .validate_for_type(ApplicationTypeKind::Record, application_type_trait_set())
        .expect_err("record Inner should be unsupported");
    assert!(error.to_string().contains("is not supported by record"));

    let conflicting_numeric = TraitBuilder {
        add: TraitListMeta(vec![TraitKind::NumericValue]),
        remove: TraitListMeta(vec![TraitKind::NumericValue]),
    };
    let mut newtype_baseline = application_type_trait_set();
    newtype_baseline.extend([TraitKind::From, TraitKind::Inner]);
    let error = conflicting_numeric
        .validate_for_type(ApplicationTypeKind::Newtype, newtype_baseline)
        .expect_err("one trait cannot be added and removed together");
    assert!(error.to_string().contains("cannot appear in both"));
}
