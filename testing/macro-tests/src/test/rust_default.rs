#[cfg(test)]
mod tests {
    use crate::prelude::*;
    use crate::test::{List, Map, Set};
    use icydb::types::Principal;

    #[record(
        fields(
            field(
                ident = "label",
                value(item(prim = "Text", unbounded)),
                default = "String::new"
            ),
            field(ident = "note", value(opt, item(prim = "Text", unbounded)))
        ),
        traits(add(Default))
    )]
    pub struct ExplicitDefaultRecord {}

    #[record(fields(field(ident = "owner", value(item(prim = "Principal")))))]
    pub struct RequiredRecord {}

    #[entity(
        store = "TestStore",
        version = 1,
        pk(fields = ["id"]),
        fields(
            field(
                ident = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
            field(
                ident = "label",
                value(item(prim = "Text", unbounded)),
                default = "guest"
            ),
            field(ident = "note", value(opt, item(prim = "Text", unbounded)))
        ),
        traits(add(Default))
    )]
    pub struct ExplicitDefaultEntity {}

    #[newtype(
        primitive = "Bool",
        item(prim = "Bool"),
        default = true,
        traits(add(Default))
    )]
    pub struct ExplicitDefaultFlag {}

    #[enum_(
        variant(ident = "Pending", default),
        variant(ident = "Active"),
        traits(add(Default))
    )]
    pub struct ExplicitDefaultStatus {}

    #[enum_(
        variant(ident = "Count", value(item(prim = "Nat32")), default),
        variant(ident = "Unknown"),
        traits(add(Default))
    )]
    pub struct ExplicitPayloadDefault {}

    #[enum_(variant(ident = "Pending"), variant(ident = "Active"))]
    pub struct NoDefaultStatus {}

    #[tuple(
        value(item(prim = "Nat32")),
        value(item(prim = "Text", unbounded)),
        traits(add(Default))
    )]
    pub struct ExplicitDefaultTuple;

    #[tuple(value(item(prim = "Principal")))]
    pub struct NoDefaultTuple;

    #[list(item(prim = "Principal"), traits(add(Default)))]
    pub struct PrincipalList;

    #[set(item(prim = "Principal"))]
    pub struct PrincipalSet;

    #[map(key(prim = "Principal"), value(item(prim = "Principal")))]
    pub struct PrincipalMap;

    #[test]
    fn explicit_domain_defaults_construct_the_authored_values() {
        assert_eq!(
            ExplicitDefaultRecord::default(),
            ExplicitDefaultRecord {
                label: String::new(),
                note: None,
            }
        );

        let entity = ExplicitDefaultEntity::default();
        assert_eq!(entity.label, "guest");
        assert_eq!(entity.note, None);

        assert_eq!(ExplicitDefaultFlag::default(), ExplicitDefaultFlag(true));
        assert_eq!(
            ExplicitDefaultStatus::default(),
            ExplicitDefaultStatus::Pending
        );
        assert_eq!(
            ExplicitPayloadDefault::default(),
            ExplicitPayloadDefault::Count(0)
        );
        assert_eq!(
            ExplicitDefaultTuple::default(),
            ExplicitDefaultTuple(0, String::new())
        );
    }

    #[test]
    fn non_default_domain_types_remain_explicitly_constructible() {
        let principal = Principal::anonymous();
        assert_eq!(
            RequiredRecord { owner: principal },
            RequiredRecord { owner: principal }
        );
        assert_eq!(NoDefaultStatus::Pending, NoDefaultStatus::Pending);
        assert_eq!(
            NoDefaultTuple(Principal::anonymous()),
            NoDefaultTuple(Principal::anonymous())
        );
    }

    #[test]
    fn collections_and_create_inputs_keep_intrinsic_defaults() {
        assert!(List::default().is_empty());
        assert!(Set::default().is_empty());
        assert!(Map::default().is_empty());
        assert!(PrincipalList::default().is_empty());
        assert!(PrincipalSet::default().is_empty());
        assert!(PrincipalMap::default().is_empty());

        let create = ExplicitDefaultEntity_Create::default();
        assert_eq!(create.label, None);
        assert_eq!(create.note, None);
    }
}
