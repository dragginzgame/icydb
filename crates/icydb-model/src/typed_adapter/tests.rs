use std::collections::{BTreeMap, BTreeSet};

use super::{
    TypedAdapterContext, TypedEnumOutput, TypedInputValue, TypedOutputValue, TypedScalarValue,
    TypedValueError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
enum TestValue {
    Int64(i64),
    List(Vec<Self>),
    Map(Vec<(Self, Self)>),
    Null,
}

struct TestContext;

impl TypedAdapterContext for TestContext {
    type InputValue = TestValue;
    type OutputValue = TestValue;

    fn input_scalar(&self, value: TypedScalarValue) -> Self::InputValue {
        match value {
            TypedScalarValue::Int64(value) => TestValue::Int64(value),
            _ => unreachable!("test context admits only i64 scalars"),
        }
    }

    fn input_list(&self, values: Vec<Self::InputValue>) -> Self::InputValue {
        TestValue::List(values)
    }

    fn input_map(&self, entries: Vec<(Self::InputValue, Self::InputValue)>) -> Self::InputValue {
        TestValue::Map(entries)
    }

    fn input_null(&self) -> Self::InputValue {
        TestValue::Null
    }

    fn input_enum(
        &self,
        _type_source_key: &'static str,
        _variant_source_key: &'static str,
        _payload: Option<Self::InputValue>,
    ) -> Result<Self::InputValue, TypedValueError> {
        Err(TypedValueError::ShapeMismatch)
    }

    fn input_record(
        &self,
        _type_source_key: &'static str,
        _fields: Vec<(&'static str, Self::InputValue)>,
    ) -> Result<Self::InputValue, TypedValueError> {
        Err(TypedValueError::ShapeMismatch)
    }

    fn output_scalar(&self, value: &Self::OutputValue) -> Option<TypedScalarValue> {
        match value {
            TestValue::Int64(value) => Some(TypedScalarValue::Int64(*value)),
            _ => None,
        }
    }

    fn output_list<'a>(&self, value: &'a Self::OutputValue) -> Option<&'a [Self::OutputValue]> {
        match value {
            TestValue::List(values) => Some(values),
            _ => None,
        }
    }

    fn output_map<'a>(
        &self,
        value: &'a Self::OutputValue,
    ) -> Option<&'a [(Self::OutputValue, Self::OutputValue)]> {
        match value {
            TestValue::Map(entries) => Some(entries),
            _ => None,
        }
    }

    fn output_is_null(&self, value: &Self::OutputValue) -> bool {
        matches!(value, TestValue::Null)
    }

    fn output_enum_variant<'a>(
        &self,
        _type_source_key: &'static str,
        _variant_source_key: &'static str,
        _value: &'a Self::OutputValue,
    ) -> Result<Option<TypedEnumOutput<'a, Self::OutputValue>>, TypedValueError> {
        Err(TypedValueError::ShapeMismatch)
    }

    fn output_record<'a>(
        &self,
        _type_source_key: &'static str,
        _member_source_keys: &[&'static str],
        _value: &'a Self::OutputValue,
    ) -> Result<Vec<&'a Self::OutputValue>, TypedValueError> {
        Err(TypedValueError::ShapeMismatch)
    }
}

#[test]
fn collection_adapters_preserve_values_and_canonical_order() {
    let context = TestContext;
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
