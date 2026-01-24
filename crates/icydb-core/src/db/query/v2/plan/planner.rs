use crate::{
    db::{
        index::fingerprint,
        query::v2::predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo,
            validate::{FieldType, ScalarType, literal_matches_type},
        },
    },
    key::Key,
    traits::EntityKind,
    value::Value,
};

use super::{AccessPath, PlanError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccessPlan {
    Path(AccessPath),
    Union(Vec<Self>),
    Intersection(Vec<Self>),
}

#[cfg(test)]
pub(crate) use tests::PlannerEntity;

impl AccessPlan {
    #[must_use]
    pub const fn full_scan() -> Self {
        Self::Path(AccessPath::FullScan)
    }

    fn normalize(self) -> Self {
        match self {
            Self::Path(_) => self,
            Self::Union(children) => normalize_union(children),
            Self::Intersection(children) => normalize_intersection(children),
        }
    }
}

pub fn plan_access<E: EntityKind>(predicate: Option<&Predicate>) -> Result<AccessPlan, PlanError> {
    let Some(predicate) = predicate else {
        return Ok(AccessPlan::full_scan());
    };

    let schema = SchemaInfo::from_entity::<E>()?;
    crate::db::query::v2::predicate::validate(&schema, predicate)?;

    Ok(plan_predicate::<E>(&schema, predicate).normalize())
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
    for index in E::INDEXES {
        if index.fields.first() != Some(&field) {
            continue;
        }
        out.push(AccessPlan::Path(AccessPath::IndexPrefix {
            index: **index,
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

    for index in E::INDEXES {
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

        if !prefix.is_empty() {
            return Some(AccessPath::IndexPrefix {
                index: **index,
                values: prefix,
            });
        }
    }

    None
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

mod canonical {
    use super::{AccessPath, AccessPlan};
    use crate::{
        key::Key,
        value::{Value, ValueEnum},
    };
    use std::cmp::Ordering;

    /// Canonical ordering only exists to make planner output deterministic.
    pub(super) fn canonicalize_access_plans(plans: &mut [AccessPlan]) {
        plans.sort_by(canonical_cmp_access_plan);
    }

    fn canonical_cmp_access_plan(left: &AccessPlan, right: &AccessPlan) -> Ordering {
        match (left, right) {
            (AccessPlan::Path(left), AccessPlan::Path(right)) => {
                canonical_cmp_access_path(left, right)
            }
            (AccessPlan::Intersection(left), AccessPlan::Intersection(right))
            | (AccessPlan::Union(left), AccessPlan::Union(right)) => {
                canonical_cmp_plan_list(left, right)
            }
            _ => canonical_access_plan_rank(left).cmp(&canonical_access_plan_rank(right)),
        }
    }

    const fn canonical_access_plan_rank(plan: &AccessPlan) -> u8 {
        match plan {
            AccessPlan::Path(_) => 0,
            AccessPlan::Intersection(_) => 1,
            AccessPlan::Union(_) => 2,
        }
    }

    fn canonical_cmp_plan_list(left: &[AccessPlan], right: &[AccessPlan]) -> Ordering {
        let limit = left.len().min(right.len());
        for (left, right) in left.iter().take(limit).zip(right.iter().take(limit)) {
            let cmp = canonical_cmp_access_plan(left, right);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        left.len().cmp(&right.len())
    }

    fn canonical_cmp_access_path(left: &AccessPath, right: &AccessPath) -> Ordering {
        let rank = canonical_access_path_rank(left).cmp(&canonical_access_path_rank(right));
        if rank != Ordering::Equal {
            return rank;
        }

        match (left, right) {
            (AccessPath::ByKey(left), AccessPath::ByKey(right)) => left.cmp(right),
            (AccessPath::ByKeys(left), AccessPath::ByKeys(right)) => {
                canonical_cmp_key_list(left, right)
            }
            (
                AccessPath::KeyRange {
                    start: left_start,
                    end: left_end,
                },
                AccessPath::KeyRange {
                    start: right_start,
                    end: right_end,
                },
            ) => left_start.cmp(right_start).then(left_end.cmp(right_end)),
            (
                AccessPath::IndexPrefix {
                    index: left_index,
                    values: left_values,
                },
                AccessPath::IndexPrefix {
                    index: right_index,
                    values: right_values,
                },
            ) => {
                let cmp = left_index.store.cmp(right_index.store);
                if cmp != Ordering::Equal {
                    return cmp;
                }
                let cmp = canonical_cmp_str_slice(left_index.fields, right_index.fields);
                if cmp != Ordering::Equal {
                    return cmp;
                }
                let cmp = left_values.len().cmp(&right_values.len());
                if cmp != Ordering::Equal {
                    return cmp;
                }
                canonical_cmp_value_list(left_values, right_values)
            }
            _ => Ordering::Equal,
        }
    }

    const fn canonical_access_path_rank(path: &AccessPath) -> u8 {
        match path {
            AccessPath::FullScan => 0,
            AccessPath::IndexPrefix { .. } => 1,
            AccessPath::ByKey(_) => 2,
            AccessPath::ByKeys(_) => 3,
            AccessPath::KeyRange { .. } => 4,
        }
    }

    fn canonical_cmp_key_list(left: &[Key], right: &[Key]) -> Ordering {
        let limit = left.len().min(right.len());
        for (left, right) in left.iter().take(limit).zip(right.iter().take(limit)) {
            let cmp = left.cmp(right);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        left.len().cmp(&right.len())
    }

    fn canonical_cmp_str_slice(left: &[&str], right: &[&str]) -> Ordering {
        let limit = left.len().min(right.len());
        for (left, right) in left.iter().take(limit).zip(right.iter().take(limit)) {
            let cmp = left.cmp(right);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        left.len().cmp(&right.len())
    }

    fn canonical_cmp_value_list(left: &[Value], right: &[Value]) -> Ordering {
        let limit = left.len().min(right.len());
        for (left, right) in left.iter().take(limit).zip(right.iter().take(limit)) {
            let cmp = canonical_cmp_value(left, right);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        left.len().cmp(&right.len())
    }

    fn canonical_cmp_value(left: &Value, right: &Value) -> Ordering {
        let rank = canonical_value_rank(left).cmp(&canonical_value_rank(right));
        if rank != Ordering::Equal {
            return rank;
        }

        match (left, right) {
            (Value::Account(left), Value::Account(right)) => left.cmp(right),
            (Value::Blob(left), Value::Blob(right)) => left.cmp(right),
            (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
            (Value::Date(left), Value::Date(right)) => left.cmp(right),
            (Value::Decimal(left), Value::Decimal(right)) => left.cmp(right),
            (Value::Duration(left), Value::Duration(right)) => left.cmp(right),
            (Value::Enum(left), Value::Enum(right)) => canonical_cmp_value_enum(left, right),
            (Value::E8s(left), Value::E8s(right)) => left.cmp(right),
            (Value::E18s(left), Value::E18s(right)) => left.cmp(right),
            (Value::Float32(left), Value::Float32(right)) => left.cmp(right),
            (Value::Float64(left), Value::Float64(right)) => left.cmp(right),
            (Value::Int(left), Value::Int(right)) => left.cmp(right),
            (Value::Int128(left), Value::Int128(right)) => left.cmp(right),
            (Value::IntBig(left), Value::IntBig(right)) => left.cmp(right),
            (Value::List(left), Value::List(right)) => canonical_cmp_value_list(left, right),
            (Value::Principal(left), Value::Principal(right)) => left.cmp(right),
            (Value::Subaccount(left), Value::Subaccount(right)) => left.cmp(right),
            (Value::Text(left), Value::Text(right)) => left.cmp(right),
            (Value::Timestamp(left), Value::Timestamp(right)) => left.cmp(right),
            (Value::Uint(left), Value::Uint(right)) => left.cmp(right),
            (Value::Uint128(left), Value::Uint128(right)) => left.cmp(right),
            (Value::UintBig(left), Value::UintBig(right)) => left.cmp(right),
            (Value::Ulid(left), Value::Ulid(right)) => left.cmp(right),
            _ => Ordering::Equal,
        }
    }

    const fn canonical_value_rank(value: &Value) -> u8 {
        match value {
            Value::Account(_) => 0,
            Value::Blob(_) => 1,
            Value::Bool(_) => 2,
            Value::Date(_) => 3,
            Value::Decimal(_) => 4,
            Value::Duration(_) => 5,
            Value::Enum(_) => 6,
            Value::E8s(_) => 7,
            Value::E18s(_) => 8,
            Value::Float32(_) => 9,
            Value::Float64(_) => 10,
            Value::Int(_) => 11,
            Value::Int128(_) => 12,
            Value::IntBig(_) => 13,
            Value::List(_) => 14,
            Value::None => 15,
            Value::Principal(_) => 16,
            Value::Subaccount(_) => 17,
            Value::Text(_) => 18,
            Value::Timestamp(_) => 19,
            Value::Uint(_) => 20,
            Value::Uint128(_) => 21,
            Value::UintBig(_) => 22,
            Value::Ulid(_) => 23,
            Value::Unit => 24,
            Value::Unsupported => 25,
        }
    }

    fn canonical_cmp_value_enum(left: &ValueEnum, right: &ValueEnum) -> Ordering {
        let cmp = left.variant.cmp(&right.variant);
        if cmp != Ordering::Equal {
            return cmp;
        }
        let cmp = left.path.cmp(&right.path);
        if cmp != Ordering::Equal {
            return cmp;
        }
        match (&left.payload, &right.payload) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(left), Some(right)) => canonical_cmp_value(left, right),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::query::v2::predicate::coercion::CoercionSpec,
        prelude::IndexModel,
        traits::{
            CanisterKind, FieldValues, Path, SanitizeAuto, SanitizeCustom, StoreKind, ValidateAuto,
            ValidateCustom, View, Visitable,
        },
        types::Ulid,
        value::Value,
    };
    use icydb_schema::{
        build::schema_write,
        node::{
            Canister, Def, Entity, Field, FieldList, Index, Item, ItemTarget, SchemaNode, Store,
            Type, Value as SchemaValue,
        },
        types::{Cardinality, Primitive, StoreType},
    };
    use serde::{Deserialize, Serialize};
    use std::sync::Once;

    const TEST_MODULE: &str = "planner_test";
    const CANISTER_PATH: &str = "planner_test::PlannerCanister";
    const DATA_STORE_PATH: &str = "planner_test::PlannerData";
    const INDEX_STORE_PATH: &str = "planner_test::PlannerIndex";
    const ENTITY_PATH: &str = "planner_test::PlannerEntity";

    const INDEX_FIELDS: [&str; 2] = ["idx_a", "idx_b"];
    const INDEX_MODEL: IndexModel = IndexModel::new(INDEX_STORE_PATH, &INDEX_FIELDS, false);
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

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

    static INIT_SCHEMA: Once = Once::new();

    #[allow(clippy::too_many_lines)]
    fn init_schema() {
        INIT_SCHEMA.call_once(|| {
            static INDEXES: [Index; 1] = [Index {
                store: INDEX_STORE_PATH,
                fields: &INDEX_FIELDS,
                unique: false,
            }];

            static FIELDS: [Field; 4] = [
                Field {
                    ident: "id",
                    value: SchemaValue {
                        cardinality: Cardinality::One,
                        item: Item {
                            target: ItemTarget::Primitive(Primitive::Ulid),
                            relation: None,
                            validators: &[],
                            sanitizers: &[],
                            indirect: false,
                        },
                    },
                    default: None,
                },
                Field {
                    ident: "idx_a",
                    value: SchemaValue {
                        cardinality: Cardinality::One,
                        item: Item {
                            target: ItemTarget::Primitive(Primitive::Text),
                            relation: None,
                            validators: &[],
                            sanitizers: &[],
                            indirect: false,
                        },
                    },
                    default: None,
                },
                Field {
                    ident: "idx_b",
                    value: SchemaValue {
                        cardinality: Cardinality::One,
                        item: Item {
                            target: ItemTarget::Primitive(Primitive::Text),
                            relation: None,
                            validators: &[],
                            sanitizers: &[],
                            indirect: false,
                        },
                    },
                    default: None,
                },
                Field {
                    ident: "other",
                    value: SchemaValue {
                        cardinality: Cardinality::One,
                        item: Item {
                            target: ItemTarget::Primitive(Primitive::Text),
                            relation: None,
                            validators: &[],
                            sanitizers: &[],
                            indirect: false,
                        },
                    },
                    default: None,
                },
            ];

            let mut schema = schema_write();

            let canister = Canister {
                def: Def {
                    module_path: TEST_MODULE,
                    ident: "PlannerCanister",
                    comments: None,
                },
                memory_min: 0,
                memory_max: 1,
            };

            let data_store = Store {
                def: Def {
                    module_path: TEST_MODULE,
                    ident: "PlannerData",
                    comments: None,
                },
                ident: "PLANNER_DATA",
                ty: StoreType::Data,
                canister: CANISTER_PATH,
                memory_id: 0,
            };

            let index_store = Store {
                def: Def {
                    module_path: TEST_MODULE,
                    ident: "PlannerIndex",
                    comments: None,
                },
                ident: "PLANNER_INDEX",
                ty: StoreType::Index,
                canister: CANISTER_PATH,
                memory_id: 1,
            };

            let entity = Entity {
                def: Def {
                    module_path: TEST_MODULE,
                    ident: "PlannerEntity",
                    comments: None,
                },
                store: DATA_STORE_PATH,
                primary_key: "id",
                name: None,
                indexes: &INDEXES,
                fields: FieldList { fields: &FIELDS },
                ty: Type {
                    sanitizers: &[],
                    validators: &[],
                },
            };

            schema.insert_node(SchemaNode::Canister(canister));
            schema.insert_node(SchemaNode::Store(data_store));
            schema.insert_node(SchemaNode::Store(index_store));
            schema.insert_node(SchemaNode::Entity(entity));
        });
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
        init_schema();
        let id = Ulid::default();
        let predicate = eq("id", Value::Ulid(id), strict());
        let plan = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::ByKey(Key::Ulid(id))));
    }

    #[test]
    fn pk_in_strict_plans_by_keys() {
        init_schema();
        let a = Ulid::default();
        let b = Ulid::from_bytes([1u8; 16]);
        let predicate = in_list("id", vec![Value::Ulid(a), Value::Ulid(b)], strict());
        let plan = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Path(AccessPath::ByKeys(vec![Key::Ulid(a), Key::Ulid(b)]))
        );
    }

    #[test]
    fn pk_eq_non_strict_falls_back_to_full_scan() {
        init_schema();
        let id = Ulid::default();
        let predicate = eq("id", Value::Ulid(id), non_strict());
        let plan = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn index_eq_strict_plans_prefix() {
        init_schema();
        let predicate = eq("idx_a", v_text("alpha"), strict());
        let plan = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();

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
        init_schema();
        let predicate = in_list("idx_a", vec![v_text("a"), v_text("b")], strict());
        let plan = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();

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
        init_schema();
        let predicate = eq("idx_b", v_text("beta"), strict());
        let plan = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn index_non_strict_falls_back_to_full_scan() {
        init_schema();
        let predicate = eq("idx_a", v_text("alpha"), non_strict());
        let plan = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn and_two_indexable_predicates_intersect() {
        init_schema();
        let id = Ulid::default();
        let predicate = Predicate::And(vec![
            eq("id", Value::Ulid(id), strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Intersection(vec![
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![v_text("alpha")],
                }),
                AccessPlan::Path(AccessPath::ByKey(Key::Ulid(id))),
            ])
        );
    }

    #[test]
    fn and_indexable_with_non_indexable_normalizes_to_indexable() {
        init_schema();
        let id = Ulid::default();
        let predicate = Predicate::And(vec![
            eq("id", Value::Ulid(id), strict()),
            eq("other", v_text("x"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::ByKey(Key::Ulid(id))));
    }

    #[test]
    fn composite_prefix_requires_strict_coercions() {
        init_schema();
        let predicate = Predicate::And(vec![
            eq("idx_a", v_text("a"), strict()),
            eq("idx_b", v_text("b"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Intersection(vec![
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![v_text("a")],
                }),
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![v_text("a"), v_text("b")],
                }),
            ])
        );

        let non_strict_predicate = Predicate::And(vec![
            eq("idx_a", v_text("a"), strict()),
            eq("idx_b", v_text("b"), non_strict()),
        ]);
        let non_strict_plan = plan_access::<PlannerEntity>(Some(&non_strict_predicate)).unwrap();

        assert_eq!(
            non_strict_plan,
            AccessPlan::Intersection(vec![AccessPlan::Path(AccessPath::IndexPrefix {
                index: INDEX_MODEL,
                values: vec![v_text("a")],
            }),])
        );
    }

    #[test]
    fn or_two_indexable_predicates_union() {
        init_schema();
        let id = Ulid::default();
        let predicate = Predicate::Or(vec![
            eq("id", Value::Ulid(id), strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Union(vec![
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![v_text("alpha")],
                }),
                AccessPlan::Path(AccessPath::ByKey(Key::Ulid(id))),
            ])
        );
    }

    #[test]
    fn or_indexable_with_non_indexable_normalizes_to_full_scan() {
        init_schema();
        let predicate = Predicate::Or(vec![
            eq("idx_a", v_text("alpha"), strict()),
            eq("other", v_text("x"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn nested_or_and_flatten_deterministically() {
        init_schema();
        let id = Ulid::default();
        let nested = Predicate::Or(vec![
            Predicate::Or(vec![eq("idx_a", v_text("alpha"), strict())]),
            Predicate::Or(vec![eq("id", Value::Ulid(id), strict())]),
        ]);
        let direct = Predicate::Or(vec![
            eq("id", Value::Ulid(id), strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);

        let nested_plan = plan_access::<PlannerEntity>(Some(&nested)).unwrap();
        let direct_plan = plan_access::<PlannerEntity>(Some(&direct)).unwrap();

        assert_eq!(nested_plan, direct_plan);
    }

    #[test]
    fn predicate_order_does_not_change_access_plan() {
        init_schema();
        let a = Predicate::And(vec![
            eq("id", Value::Ulid(Ulid::default()), strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);
        let b = Predicate::And(vec![
            eq("idx_a", v_text("alpha"), strict()),
            eq("id", Value::Ulid(Ulid::default()), strict()),
        ]);

        let plan_a = plan_access::<PlannerEntity>(Some(&a)).unwrap();
        let plan_b = plan_access::<PlannerEntity>(Some(&b)).unwrap();

        assert_eq!(plan_a, plan_b);
    }

    #[test]
    fn deterministic_output_across_runs() {
        init_schema();
        let predicate = eq("idx_a", v_text("alpha"), strict());

        let plan_a = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();
        let plan_b = plan_access::<PlannerEntity>(Some(&predicate)).unwrap();

        assert_eq!(plan_a, plan_b);
    }
}
