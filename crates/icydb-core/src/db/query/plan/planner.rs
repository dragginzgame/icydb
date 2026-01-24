//! Semantic planning from predicates to access strategies; must not assert invariants.
//!
//! Determinism: the planner canonicalizes output so the same model and
//! predicate shape always produce identical access plans.

use super::{AccessPath, AccessPlan, PlanError, canonical, validate_plan_invariants};
use crate::{
    db::{
        index::fingerprint,
        query::predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo, normalize,
            validate::{FieldType, ScalarType, literal_matches_type},
        },
    },
    key::Key,
    model::index::IndexModel,
    traits::EntityKind,
    value::Value,
};

#[cfg(test)]
pub(crate) use tests::PlannerEntity;

impl AccessPlan {
    fn normalize(self) -> Self {
        match self {
            Self::Path(_) => self,
            Self::Union(children) => normalize_union(children),
            Self::Intersection(children) => normalize_intersection(children),
        }
    }
}

/// Planner entrypoint that operates on a prebuilt schema surface.
pub(crate) fn plan_access<E: EntityKind>(
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<AccessPlan, PlanError> {
    let Some(predicate) = predicate else {
        return Ok(AccessPlan::full_scan());
    };

    // Planner determinism guarantee:
    // Given a validated EntityModel and normalized predicate, planning is pure and deterministic.
    //
    // Planner determinism rules:
    // - Predicate normalization sorts AND/OR children by (field, operator, value, coercion).
    // - Index candidates are considered in lexicographic IndexModel.name order.
    // - Access paths are ranked: primary key lookups, exact index matches, prefix matches, full scans.
    // - Order specs preserve user order after validation (planner does not reorder).
    // - Field resolution uses SchemaInfo's name map (sorted by field name).
    crate::db::query::predicate::validate(schema, predicate)?;

    let normalized = normalize(predicate);
    let plan = plan_predicate::<E>(schema, &normalized).normalize();
    validate_plan_invariants::<E>(&plan, schema, Some(&normalized));
    Ok(plan)
}

fn plan_predicate<E: EntityKind>(schema: &SchemaInfo, predicate: &Predicate) -> AccessPlan {
    match predicate {
        Predicate::True
        | Predicate::False
        | Predicate::Not(_)
        | Predicate::IsNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::MapContainsKey { .. }
        | Predicate::MapContainsValue { .. }
        | Predicate::MapContainsEntry { .. } => AccessPlan::full_scan(),
        Predicate::And(children) => {
            let mut plans = children
                .iter()
                .map(|child| plan_predicate::<E>(schema, child))
                .collect::<Vec<_>>();

            if let Some(prefix) = index_prefix_from_and::<E>(schema, children) {
                plans.push(AccessPlan::Path(prefix));
            }

            AccessPlan::Intersection(plans)
        }
        Predicate::Or(children) => AccessPlan::Union(
            children
                .iter()
                .map(|child| plan_predicate::<E>(schema, child))
                .collect::<Vec<_>>(),
        ),
        Predicate::Compare(cmp) => plan_compare::<E>(schema, cmp),
    }
}

fn plan_compare<E: EntityKind>(schema: &SchemaInfo, cmp: &ComparePredicate) -> AccessPlan {
    if cmp.coercion.id != CoercionId::Strict {
        return AccessPlan::full_scan();
    }

    if is_primary_key::<E>(schema, &cmp.field)
        && let Some(path) = plan_pk_compare::<E>(schema, cmp)
    {
        return AccessPlan::Path(path);
    }

    match cmp.op {
        CompareOp::Eq => {
            if let Some(paths) = index_prefix_for_eq::<E>(schema, &cmp.field, &cmp.value) {
                return AccessPlan::Union(paths);
            }
        }
        CompareOp::In => {
            if let Value::List(items) = &cmp.value {
                let mut plans = Vec::new();
                for item in items {
                    if let Some(paths) = index_prefix_for_eq::<E>(schema, &cmp.field, item) {
                        plans.extend(paths);
                    }
                }
                if !plans.is_empty() {
                    return AccessPlan::Union(plans);
                }
            }
        }
        _ => {}
    }

    AccessPlan::full_scan()
}

fn plan_pk_compare<E: EntityKind>(
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Option<AccessPath> {
    match cmp.op {
        CompareOp::Eq => {
            let key = cmp.value.as_key()?;
            key_matches_pk::<E>(schema, &key).then_some(AccessPath::ByKey(key))
        }
        CompareOp::In => {
            let Value::List(items) = &cmp.value else {
                return None;
            };
            let mut keys = Vec::with_capacity(items.len());
            for item in items {
                let key = item.as_key()?;
                if !key_matches_pk::<E>(schema, &key) {
                    return None;
                }
                keys.push(key);
            }
            Some(AccessPath::ByKeys(keys))
        }
        _ => None,
    }
}

fn sorted_indexes<E: EntityKind>() -> Vec<&'static IndexModel> {
    let mut indexes = E::INDEXES.to_vec();
    indexes.sort_by(|left, right| left.name.cmp(right.name));
    indexes
}

fn index_prefix_for_eq<E: EntityKind>(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
) -> Option<Vec<AccessPlan>> {
    let field_type = schema.field(field)?;

    if !literal_matches_type(value, field_type) {
        return None;
    }

    fingerprint::to_index_fingerprint(value)?;

    let mut out = Vec::new();
    for index in sorted_indexes::<E>() {
        if index.fields.first() != Some(&field) {
            continue;
        }
        out.push(AccessPlan::Path(AccessPath::IndexPrefix {
            index: *index,
            values: vec![value.clone()],
        }));
    }

    if out.is_empty() { None } else { Some(out) }
}

fn index_prefix_from_and<E: EntityKind>(
    schema: &SchemaInfo,
    children: &[Predicate],
) -> Option<AccessPath> {
    let mut field_values = Vec::new();

    for child in children {
        let Predicate::Compare(cmp) = child else {
            continue;
        };
        if cmp.op != CompareOp::Eq {
            continue;
        }
        if cmp.coercion.id != CoercionId::Strict {
            continue;
        }
        field_values.push((cmp.field.as_str(), &cmp.value));
    }

    let mut best: Option<(usize, bool, &IndexModel, Vec<Value>)> = None;
    for index in sorted_indexes::<E>() {
        let mut prefix = Vec::new();
        for field in index.fields {
            let Some((_, value)) = field_values.iter().find(|(name, _)| *name == *field) else {
                break;
            };
            let field_type = schema.field(field)?;
            if !literal_matches_type(value, field_type) {
                prefix.clear();
                break;
            }
            if fingerprint::to_index_fingerprint(value).is_none() {
                prefix.clear();
                break;
            }
            prefix.push((*value).clone());
        }

        if prefix.is_empty() {
            continue;
        }

        let exact = prefix.len() == index.fields.len();
        match &best {
            None => best = Some((prefix.len(), exact, index, prefix)),
            Some((best_len, best_exact, best_index, _)) => {
                if better_index(
                    (prefix.len(), exact, index),
                    (*best_len, *best_exact, best_index),
                ) {
                    best = Some((prefix.len(), exact, index, prefix));
                }
            }
        }
    }

    best.map(|(_, _, index, values)| AccessPath::IndexPrefix {
        index: *index,
        values,
    })
}

fn better_index(
    candidate: (usize, bool, &IndexModel),
    current: (usize, bool, &IndexModel),
) -> bool {
    let (cand_len, cand_exact, cand_index) = candidate;
    let (best_len, best_exact, best_index) = current;

    cand_len > best_len
        || (cand_len == best_len && cand_exact && !best_exact)
        || (cand_len == best_len && cand_exact == best_exact && cand_index.name < best_index.name)
}

fn normalize_union(children: Vec<AccessPlan>) -> AccessPlan {
    let mut out = Vec::new();

    for child in children {
        let child = child.normalize();
        if is_full_scan(&child) {
            return AccessPlan::full_scan();
        }

        match child {
            AccessPlan::Union(grand) => out.extend(grand),
            _ => out.push(child),
        }
    }

    if out.is_empty() {
        return AccessPlan::full_scan();
    }
    if out.len() == 1 {
        return out.pop().expect("single union child");
    }

    canonical::canonicalize_access_plans(&mut out);
    out.dedup();
    AccessPlan::Union(out)
}

fn normalize_intersection(children: Vec<AccessPlan>) -> AccessPlan {
    let mut out = Vec::new();

    for child in children {
        let child = child.normalize();
        if is_full_scan(&child) {
            continue;
        }

        match child {
            AccessPlan::Intersection(grand) => out.extend(grand),
            _ => out.push(child),
        }
    }

    if out.is_empty() {
        return AccessPlan::full_scan();
    }
    if out.len() == 1 {
        return out.pop().expect("single intersection child");
    }

    canonical::canonicalize_access_plans(&mut out);
    out.dedup();
    AccessPlan::Intersection(out)
}

const fn is_full_scan(plan: &AccessPlan) -> bool {
    matches!(plan, AccessPlan::Path(AccessPath::FullScan))
}

fn is_primary_key<E: EntityKind>(schema: &SchemaInfo, field: &str) -> bool {
    field == E::PRIMARY_KEY && schema.field(field).is_some()
}

fn key_matches_pk<E: EntityKind>(schema: &SchemaInfo, key: &Key) -> bool {
    let field = E::PRIMARY_KEY;
    let Some(field_type) = schema.field(field) else {
        return false;
    };

    let Some(expected) = key_type_for_field(field_type) else {
        return false;
    };

    key_variant(key) == expected
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeyVariant {
    Account,
    Int,
    Principal,
    Subaccount,
    Timestamp,
    Uint,
    Ulid,
    Unit,
}

const fn key_variant(key: &Key) -> KeyVariant {
    match key {
        Key::Account(_) => KeyVariant::Account,
        Key::Int(_) => KeyVariant::Int,
        Key::Principal(_) => KeyVariant::Principal,
        Key::Subaccount(_) => KeyVariant::Subaccount,
        Key::Timestamp(_) => KeyVariant::Timestamp,
        Key::Uint(_) => KeyVariant::Uint,
        Key::Ulid(_) => KeyVariant::Ulid,
        Key::Unit => KeyVariant::Unit,
    }
}

const fn key_type_for_field(field_type: &FieldType) -> Option<KeyVariant> {
    match field_type {
        FieldType::Scalar(ScalarType::Account) => Some(KeyVariant::Account),
        FieldType::Scalar(ScalarType::Int) => Some(KeyVariant::Int),
        FieldType::Scalar(ScalarType::Principal) => Some(KeyVariant::Principal),
        FieldType::Scalar(ScalarType::Subaccount) => Some(KeyVariant::Subaccount),
        FieldType::Scalar(ScalarType::Timestamp) => Some(KeyVariant::Timestamp),
        FieldType::Scalar(ScalarType::Uint) => Some(KeyVariant::Uint),
        FieldType::Scalar(ScalarType::Ulid) => Some(KeyVariant::Ulid),
        FieldType::Scalar(ScalarType::Unit) => Some(KeyVariant::Unit),
        _ => None,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::query::predicate::SchemaInfo,
        db::query::predicate::coercion::CoercionSpec,
        model::{
            entity::EntityModel,
            field::{EntityFieldKind, EntityFieldModel},
        },
        prelude::IndexModel,
        traits::{
            CanisterKind, FieldValues, Path, SanitizeAuto, SanitizeCustom, StoreKind, ValidateAuto,
            ValidateCustom, View, Visitable,
        },
        types::Ulid,
        value::Value,
    };
    use serde::{Deserialize, Serialize};

    const CANISTER_PATH: &str = "planner_test::PlannerCanister";
    const DATA_STORE_PATH: &str = "planner_test::PlannerData";
    const INDEX_STORE_PATH: &str = "planner_test::PlannerIndex";
    const ENTITY_PATH: &str = "planner_test::PlannerEntity";

    const INDEX_FIELDS: [&str; 2] = ["idx_a", "idx_b"];
    const INDEX_MODEL: IndexModel = IndexModel::new(
        "planner_test::idx_a_idx_b",
        INDEX_STORE_PATH,
        &INDEX_FIELDS,
        false,
    );
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];
    const PLANNER_FIELDS: [EntityFieldModel; 4] = [
        EntityFieldModel {
            name: "id",
            kind: EntityFieldKind::Ulid,
        },
        EntityFieldModel {
            name: "idx_a",
            kind: EntityFieldKind::Text,
        },
        EntityFieldModel {
            name: "idx_b",
            kind: EntityFieldKind::Text,
        },
        EntityFieldModel {
            name: "other",
            kind: EntityFieldKind::Text,
        },
    ];
    const PLANNER_MODEL: EntityModel = EntityModel {
        path: ENTITY_PATH,
        entity_name: "PlannerEntity",
        primary_key: &PLANNER_FIELDS[0],
        fields: &PLANNER_FIELDS,
        indexes: &INDEXES,
    };

    #[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
    pub struct PlannerEntity {
        id: Ulid,
        idx_a: String,
        idx_b: String,
        other: String,
    }

    impl Path for PlannerEntity {
        const PATH: &'static str = ENTITY_PATH;
    }

    impl View for PlannerEntity {
        type ViewType = Self;

        fn to_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl SanitizeAuto for PlannerEntity {}
    impl SanitizeCustom for PlannerEntity {}
    impl ValidateAuto for PlannerEntity {}
    impl ValidateCustom for PlannerEntity {}
    impl Visitable for PlannerEntity {}

    impl FieldValues for PlannerEntity {
        fn get_value(&self, field: &str) -> Option<Value> {
            match field {
                "id" => Some(Value::Ulid(self.id)),
                "idx_a" => Some(Value::Text(self.idx_a.clone())),
                "idx_b" => Some(Value::Text(self.idx_b.clone())),
                "other" => Some(Value::Text(self.other.clone())),
                _ => None,
            }
        }
    }

    pub struct PlannerCanister;

    impl Path for PlannerCanister {
        const PATH: &'static str = CANISTER_PATH;
    }

    impl CanisterKind for PlannerCanister {}

    pub struct PlannerStore;

    impl Path for PlannerStore {
        const PATH: &'static str = DATA_STORE_PATH;
    }

    impl StoreKind for PlannerStore {
        type Canister = PlannerCanister;
    }

    impl EntityKind for PlannerEntity {
        type PrimaryKey = Ulid;
        type Store = PlannerStore;
        type Canister = PlannerCanister;

        const ENTITY_NAME: &'static str = "PlannerEntity";
        const PRIMARY_KEY: &'static str = "id";
        const FIELDS: &'static [&'static str] = &["id", "idx_a", "idx_b", "other"];
        const INDEXES: &'static [&'static IndexModel] = &INDEXES;
        const MODEL: &'static EntityModel = &PLANNER_MODEL;

        fn key(&self) -> Key {
            self.id.into()
        }

        fn primary_key(&self) -> Self::PrimaryKey {
            self.id
        }

        fn set_primary_key(&mut self, key: Self::PrimaryKey) {
            self.id = key;
        }
    }

    const MULTI_ENTITY_PATH: &str = "planner_test::MultiIndexEntity";

    const MULTI_INDEX_FIELDS_A: [&str; 1] = ["idx_a"];
    const MULTI_INDEX_FIELDS_AB: [&str; 2] = ["idx_a", "idx_b"];

    const MULTI_INDEX_A: IndexModel = IndexModel::new(
        "planner_test::idx_a",
        INDEX_STORE_PATH,
        &MULTI_INDEX_FIELDS_A,
        false,
    );
    const MULTI_INDEX_A_ALT: IndexModel = IndexModel::new(
        "planner_test::idx_a_alt",
        INDEX_STORE_PATH,
        &MULTI_INDEX_FIELDS_A,
        false,
    );
    const MULTI_INDEX_AB: IndexModel = IndexModel::new(
        "planner_test::idx_a_b",
        INDEX_STORE_PATH,
        &MULTI_INDEX_FIELDS_AB,
        false,
    );
    const MULTI_INDEXES: [&IndexModel; 3] = [&MULTI_INDEX_AB, &MULTI_INDEX_A_ALT, &MULTI_INDEX_A];

    const MULTI_FIELDS: [EntityFieldModel; 3] = [
        EntityFieldModel {
            name: "id",
            kind: EntityFieldKind::Ulid,
        },
        EntityFieldModel {
            name: "idx_a",
            kind: EntityFieldKind::Text,
        },
        EntityFieldModel {
            name: "idx_b",
            kind: EntityFieldKind::Text,
        },
    ];
    const MULTI_MODEL: EntityModel = EntityModel {
        path: MULTI_ENTITY_PATH,
        entity_name: "MultiIndexEntity",
        primary_key: &MULTI_FIELDS[0],
        fields: &MULTI_FIELDS,
        indexes: &MULTI_INDEXES,
    };

    #[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
    struct MultiIndexEntity {
        id: Ulid,
        idx_a: String,
        idx_b: String,
    }

    impl Path for MultiIndexEntity {
        const PATH: &'static str = MULTI_ENTITY_PATH;
    }

    impl View for MultiIndexEntity {
        type ViewType = Self;

        fn to_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl SanitizeAuto for MultiIndexEntity {}
    impl SanitizeCustom for MultiIndexEntity {}
    impl ValidateAuto for MultiIndexEntity {}
    impl ValidateCustom for MultiIndexEntity {}
    impl Visitable for MultiIndexEntity {}

    impl FieldValues for MultiIndexEntity {
        fn get_value(&self, field: &str) -> Option<Value> {
            match field {
                "id" => Some(Value::Ulid(self.id)),
                "idx_a" => Some(Value::Text(self.idx_a.clone())),
                "idx_b" => Some(Value::Text(self.idx_b.clone())),
                _ => None,
            }
        }
    }

    impl EntityKind for MultiIndexEntity {
        type PrimaryKey = Ulid;
        type Store = PlannerStore;
        type Canister = PlannerCanister;

        const ENTITY_NAME: &'static str = "MultiIndexEntity";
        const PRIMARY_KEY: &'static str = "id";
        const FIELDS: &'static [&'static str] = &["id", "idx_a", "idx_b"];
        const INDEXES: &'static [&'static IndexModel] = &MULTI_INDEXES;
        const MODEL: &'static EntityModel = &MULTI_MODEL;

        fn key(&self) -> Key {
            self.id.into()
        }

        fn primary_key(&self) -> Self::PrimaryKey {
            self.id
        }

        fn set_primary_key(&mut self, key: Self::PrimaryKey) {
            self.id = key;
        }
    }

    fn model_schema() -> SchemaInfo {
        SchemaInfo::from_entity_model(PlannerEntity::MODEL).expect("valid model")
    }

    fn strict() -> CoercionSpec {
        CoercionSpec::new(CoercionId::Strict)
    }

    fn non_strict() -> CoercionSpec {
        CoercionSpec::new(CoercionId::TextCasefold)
    }

    fn eq(field: &str, value: Value, coercion: CoercionSpec) -> Predicate {
        Predicate::Compare(ComparePredicate {
            field: field.to_string(),
            op: CompareOp::Eq,
            value,
            coercion,
        })
    }

    fn in_list(field: &str, values: Vec<Value>, coercion: CoercionSpec) -> Predicate {
        Predicate::Compare(ComparePredicate {
            field: field.to_string(),
            op: CompareOp::In,
            value: Value::List(values),
            coercion,
        })
    }

    fn v_text(s: &str) -> Value {
        Value::Text(s.to_string())
    }

    #[test]
    fn pk_eq_strict_plans_by_key() {
        let schema = model_schema();
        let id = Ulid::default();
        let predicate = eq("id", Value::Ulid(id), strict());
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::ByKey(Key::Ulid(id))));
    }

    #[test]
    fn pk_in_strict_plans_by_keys() {
        let schema = model_schema();
        let a = Ulid::default();
        let b = Ulid::from_bytes([1u8; 16]);
        let predicate = in_list("id", vec![Value::Ulid(a), Value::Ulid(b)], strict());
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Path(AccessPath::ByKeys(vec![Key::Ulid(a), Key::Ulid(b)]))
        );
    }

    #[test]
    fn pk_eq_non_strict_falls_back_to_full_scan() {
        let schema = model_schema();
        let id = Ulid::default();
        let predicate = eq("id", Value::Ulid(id), non_strict());
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn index_eq_strict_plans_prefix() {
        let schema = model_schema();
        let predicate = eq("idx_a", v_text("alpha"), strict());
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Path(AccessPath::IndexPrefix {
                index: INDEX_MODEL,
                values: vec![v_text("alpha")],
            })
        );
    }

    #[test]
    fn index_in_strict_plans_union_of_prefixes() {
        let schema = model_schema();
        let predicate = in_list("idx_a", vec![v_text("a"), v_text("b")], strict());
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Union(vec![
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![v_text("a")],
                }),
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![v_text("b")],
                }),
            ])
        );
    }

    #[test]
    fn index_non_first_field_falls_back_to_full_scan() {
        let schema = model_schema();
        let predicate = eq("idx_b", v_text("beta"), strict());
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn index_non_strict_falls_back_to_full_scan() {
        let schema = model_schema();
        let predicate = eq("idx_a", v_text("alpha"), non_strict());
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn and_two_indexable_predicates_intersect() {
        let schema = model_schema();
        let id = Ulid::default();
        let predicate = Predicate::And(vec![
            eq("id", Value::Ulid(id), strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Intersection(vec![
                AccessPlan::Path(AccessPath::ByKey(Key::Ulid(id))),
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![v_text("alpha")],
                }),
            ])
        );
    }

    #[test]
    fn and_indexable_with_non_indexable_normalizes_to_indexable() {
        let schema = model_schema();
        let id = Ulid::default();
        let predicate = Predicate::And(vec![
            eq("id", Value::Ulid(id), strict()),
            eq("other", v_text("x"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::ByKey(Key::Ulid(id))));
    }

    #[test]
    fn mixed_pk_non_strict_and_index_strict_does_not_plan_by_key() {
        let schema = model_schema();
        let id = Ulid::default();
        let predicate = Predicate::And(vec![
            eq("id", Value::Ulid(id), non_strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Path(AccessPath::IndexPrefix {
                index: INDEX_MODEL,
                values: vec![v_text("alpha")],
            })
        );
    }

    #[test]
    fn and_non_indexable_predicates_fall_back_to_full_scan() {
        let schema = model_schema();
        let predicate = Predicate::And(vec![
            eq("idx_b", v_text("beta"), strict()),
            eq("other", v_text("x"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn composite_prefix_requires_strict_coercions() {
        let schema = model_schema();
        let predicate = Predicate::And(vec![
            eq("idx_a", v_text("a"), strict()),
            eq("idx_b", v_text("b"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Intersection(vec![
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![v_text("a"), v_text("b")],
                }),
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![v_text("a")],
                }),
            ])
        );

        let non_strict_predicate = Predicate::And(vec![
            eq("idx_a", v_text("a"), strict()),
            eq("idx_b", v_text("b"), non_strict()),
        ]);
        let non_strict_plan =
            plan_access::<PlannerEntity>(&schema, Some(&non_strict_predicate)).unwrap();

        assert_eq!(
            non_strict_plan,
            AccessPlan::Intersection(vec![AccessPlan::Path(AccessPath::IndexPrefix {
                index: INDEX_MODEL,
                values: vec![v_text("a")],
            }),])
        );
    }

    #[test]
    fn index_prefix_from_and_prefers_longest_prefix_then_name() {
        let schema = SchemaInfo::from_entity_model(MultiIndexEntity::MODEL).expect("valid model");

        let children = vec![
            eq("idx_a", v_text("alpha"), strict()),
            eq("idx_b", v_text("beta"), strict()),
        ];
        let first =
            index_prefix_from_and::<MultiIndexEntity>(&schema, &children).expect("index prefix");
        let second =
            index_prefix_from_and::<MultiIndexEntity>(&schema, &children).expect("index prefix");
        assert_eq!(first, second);

        let AccessPath::IndexPrefix { index, values } = first else {
            panic!("expected index prefix path");
        };
        assert_eq!(index.name, "planner_test::idx_a_b");
        assert_eq!(values, vec![v_text("alpha"), v_text("beta")]);

        let children = vec![eq("idx_a", v_text("alpha"), strict())];
        let AccessPath::IndexPrefix { index, .. } =
            index_prefix_from_and::<MultiIndexEntity>(&schema, &children).expect("index prefix")
        else {
            panic!("expected index prefix path");
        };
        assert_eq!(index.name, "planner_test::idx_a");
    }

    #[test]
    fn or_two_indexable_predicates_union() {
        let schema = model_schema();
        let id = Ulid::default();
        let predicate = Predicate::Or(vec![
            eq("id", Value::Ulid(id), strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Union(vec![
                AccessPlan::Path(AccessPath::ByKey(Key::Ulid(id))),
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![v_text("alpha")],
                }),
            ])
        );
    }

    #[test]
    fn or_indexable_with_non_indexable_normalizes_to_full_scan() {
        let schema = model_schema();
        let predicate = Predicate::Or(vec![
            eq("idx_a", v_text("alpha"), strict()),
            eq("other", v_text("x"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn empty_and_or_normalize_to_full_scan() {
        let schema = model_schema();
        let empty_and = Predicate::And(Vec::new());
        let empty_or = Predicate::Or(Vec::new());

        let and_plan = plan_access::<PlannerEntity>(&schema, Some(&empty_and)).unwrap();
        let or_plan = plan_access::<PlannerEntity>(&schema, Some(&empty_or)).unwrap();

        assert_eq!(and_plan, AccessPlan::Path(AccessPath::FullScan));
        assert_eq!(or_plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn nested_or_and_flatten_deterministically() {
        let schema = model_schema();
        let id = Ulid::default();
        let nested = Predicate::Or(vec![
            Predicate::Or(vec![eq("idx_a", v_text("alpha"), strict())]),
            Predicate::Or(vec![eq("id", Value::Ulid(id), strict())]),
        ]);
        let direct = Predicate::Or(vec![
            eq("id", Value::Ulid(id), strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);

        let nested_plan = plan_access::<PlannerEntity>(&schema, Some(&nested)).unwrap();
        let direct_plan = plan_access::<PlannerEntity>(&schema, Some(&direct)).unwrap();

        assert_eq!(nested_plan, direct_plan);
    }

    #[test]
    fn predicate_order_does_not_change_access_plan() {
        let schema = model_schema();
        let a = Predicate::And(vec![
            eq("id", Value::Ulid(Ulid::default()), strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);
        let b = Predicate::And(vec![
            eq("idx_a", v_text("alpha"), strict()),
            eq("id", Value::Ulid(Ulid::default()), strict()),
        ]);

        let plan_a = plan_access::<PlannerEntity>(&schema, Some(&a)).unwrap();
        let plan_b = plan_access::<PlannerEntity>(&schema, Some(&b)).unwrap();

        assert_eq!(plan_a, plan_b);
    }

    #[test]
    fn deterministic_output_across_runs() {
        let schema = model_schema();
        let predicate = eq("idx_a", v_text("alpha"), strict());

        let plan_a = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();
        let plan_b = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan_a, plan_b);
    }
}
