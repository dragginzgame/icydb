use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet},
};

use super::{
    TypedAdapterContext, TypedEnumDescriptor, TypedEnumSelection, TypedInputValue,
    TypedOutputValue, TypedScalarValue, TypedValueError,
};

#[crate::enum_(
    name = "TestChoiceSource",
    variant(name = "Empty"),
    variant(name = "Count", value(item(prim = "Int64")))
)]
pub struct TestChoice {}

#[crate::record(
    name = "TestProfileSource",
    fields(
        field(name = "label", value(item(prim = "Text", unbounded))),
        field(name = "choice", value(item(is = "TestChoice"))),
        field(name = "note", value(opt, item(prim = "Int64")))
    )
)]
pub struct TestProfile {}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TestValue {
    Enum {
        ordinal: usize,
        payload: Option<Box<Self>>,
    },
    Int64(i64),
    List(Vec<Self>),
    Map(Vec<(Self, Self)>),
    Null,
    Text(String),
}

#[derive(Default)]
struct TestContext {
    output_enum_calls: Cell<usize>,
}

impl TypedAdapterContext for TestContext {
    type PublicValue = TestValue;

    fn input_scalar(&self, value: TypedScalarValue) -> Self::PublicValue {
        match value {
            TypedScalarValue::Int64(value) => TestValue::Int64(value),
            TypedScalarValue::Text(value) => TestValue::Text(value),
            _ => unreachable!("test context admits only i64 scalars"),
        }
    }

    fn input_list(&self, values: Vec<Self::PublicValue>) -> Self::PublicValue {
        TestValue::List(values)
    }

    fn input_map(&self, entries: Vec<(Self::PublicValue, Self::PublicValue)>) -> Self::PublicValue {
        TestValue::Map(entries)
    }

    fn input_null(&self) -> Self::PublicValue {
        TestValue::Null
    }

    fn input_enum(
        &self,
        type_source_key: &'static str,
        variant_source_key: &'static str,
        payload: Option<Self::PublicValue>,
    ) -> Result<Self::PublicValue, TypedValueError> {
        if type_source_key != "TestChoiceSource" {
            return Err(TypedValueError::SourceUnavailable);
        }
        let ordinal = match variant_source_key {
            "Empty" => 0,
            "Count" => 1,
            _ => return Err(TypedValueError::SourceUnavailable),
        };
        Ok(TestValue::Enum {
            ordinal,
            payload: payload.map(Box::new),
        })
    }

    fn input_record(
        &self,
        type_source_key: &'static str,
        fields: Vec<(&'static str, Self::PublicValue)>,
    ) -> Result<Self::PublicValue, TypedValueError> {
        if type_source_key != "TestProfileSource" {
            return Err(TypedValueError::SourceUnavailable);
        }
        Ok(TestValue::Map(
            fields
                .into_iter()
                .map(|(name, value)| (TestValue::Text(name.to_string()), value))
                .collect(),
        ))
    }

    fn output_scalar(&self, value: &Self::PublicValue) -> Option<TypedScalarValue> {
        match value {
            TestValue::Int64(value) => Some(TypedScalarValue::Int64(*value)),
            TestValue::Text(value) => Some(TypedScalarValue::Text(value.clone())),
            _ => None,
        }
    }

    fn output_list<'a>(&self, value: &'a Self::PublicValue) -> Option<&'a [Self::PublicValue]> {
        match value {
            TestValue::List(values) => Some(values),
            _ => None,
        }
    }

    fn output_map<'a>(
        &self,
        value: &'a Self::PublicValue,
    ) -> Option<&'a [(Self::PublicValue, Self::PublicValue)]> {
        match value {
            TestValue::Map(entries) => Some(entries),
            _ => None,
        }
    }

    fn output_is_null(&self, value: &Self::PublicValue) -> bool {
        matches!(value, TestValue::Null)
    }

    fn output_enum<'a>(
        &self,
        descriptor: &'static TypedEnumDescriptor,
        value: &'a Self::PublicValue,
    ) -> Result<TypedEnumSelection<'a, Self::PublicValue>, TypedValueError> {
        self.output_enum_calls
            .set(self.output_enum_calls.get().saturating_add(1));
        if descriptor.type_source_key != "TestChoiceSource"
            || descriptor.variants != ["Empty", "Count"]
        {
            return Err(TypedValueError::SourceUnavailable);
        }
        let TestValue::Enum { ordinal, payload } = value else {
            return Err(TypedValueError::ShapeMismatch);
        };
        Ok(TypedEnumSelection {
            ordinal: *ordinal,
            payload: payload.as_deref(),
        })
    }

    fn output_record<'a>(
        &self,
        _type_source_key: &'static str,
        _member_source_keys: &[&'static str],
        _value: &'a Self::PublicValue,
    ) -> Result<Vec<&'a Self::PublicValue>, TypedValueError> {
        Err(TypedValueError::ShapeMismatch)
    }
}

#[test]
fn collection_adapters_preserve_values_and_canonical_order() {
    let context = TestContext::default();
    let list = vec![3_i64, 1];
    let encoded_list = list
        .clone()
        .encode_typed_input(&context)
        .expect("list should encode");
    assert_eq!(
        encoded_list,
        TestValue::List(vec![TestValue::Int64(3), TestValue::Int64(1)])
    );
    assert_eq!(
        Vec::<i64>::decode_typed_output(&context, &encoded_list).expect("list should decode"),
        list,
    );

    let map = BTreeMap::from([(2_i64, 20_i64), (1, 10)]);
    let encoded_map = map
        .clone()
        .encode_typed_input(&context)
        .expect("map should encode");
    assert_eq!(
        encoded_map,
        TestValue::Map(vec![
            (TestValue::Int64(1), TestValue::Int64(10)),
            (TestValue::Int64(2), TestValue::Int64(20)),
        ]),
    );
    assert_eq!(
        BTreeMap::<i64, i64>::decode_typed_output(&context, &encoded_map)
            .expect("map should decode"),
        map,
    );

    let set = BTreeSet::from([2_i64, 1]);
    let encoded_set = set
        .clone()
        .encode_typed_input(&context)
        .expect("set should encode");
    assert_eq!(
        encoded_set,
        TestValue::List(vec![TestValue::Int64(1), TestValue::Int64(2)])
    );
    assert_eq!(
        BTreeSet::<i64>::decode_typed_output(&context, &encoded_set).expect("set should decode"),
        set,
    );
}

#[test]
fn generated_nested_input_preserves_record_list_enum_payload_and_null_shape() {
    let context = TestContext::default();
    let encoded = vec![
        TestProfile {
            label: "Ada".to_string(),
            choice: TestChoice::Empty,
            note: None,
        },
        TestProfile {
            label: "Grace".to_string(),
            choice: TestChoice::Count(7),
            note: Some(9),
        },
    ]
    .encode_typed_input(&context)
    .expect("generated nested input should encode once through the context");

    assert_eq!(
        encoded,
        TestValue::List(vec![
            TestValue::Map(vec![
                (
                    TestValue::Text("label".to_string()),
                    TestValue::Text("Ada".to_string()),
                ),
                (
                    TestValue::Text("choice".to_string()),
                    TestValue::Enum {
                        ordinal: 0,
                        payload: None,
                    },
                ),
                (TestValue::Text("note".to_string()), TestValue::Null),
            ]),
            TestValue::Map(vec![
                (
                    TestValue::Text("label".to_string()),
                    TestValue::Text("Grace".to_string()),
                ),
                (
                    TestValue::Text("choice".to_string()),
                    TestValue::Enum {
                        ordinal: 1,
                        payload: Some(Box::new(TestValue::Int64(7))),
                    },
                ),
                (TestValue::Text("note".to_string()), TestValue::Int64(9),),
            ]),
        ]),
    );
}

#[test]
fn generated_enum_decode_selects_once_and_preserves_payload_shape() {
    let context = TestContext::default();
    let unit = TestValue::Enum {
        ordinal: 0,
        payload: None,
    };

    assert_eq!(
        TestChoice::decode_typed_output(&context, &unit),
        Ok(TestChoice::Empty),
    );
    assert_eq!(context.output_enum_calls.get(), 1);

    let context = TestContext::default();
    let value = TestValue::Enum {
        ordinal: 1,
        payload: Some(Box::new(TestValue::Int64(7))),
    };

    assert_eq!(
        TestChoice::decode_typed_output(&context, &value),
        Ok(TestChoice::Count(7)),
    );
    assert_eq!(context.output_enum_calls.get(), 1);

    for malformed in [
        TestValue::Enum {
            ordinal: 0,
            payload: Some(Box::new(TestValue::Int64(7))),
        },
        TestValue::Enum {
            ordinal: 1,
            payload: None,
        },
        TestValue::Enum {
            ordinal: 2,
            payload: None,
        },
    ] {
        let context = TestContext::default();
        assert_eq!(
            TestChoice::decode_typed_output(&context, &malformed),
            Err(TypedValueError::ShapeMismatch),
        );
        assert_eq!(context.output_enum_calls.get(), 1);
    }
}
