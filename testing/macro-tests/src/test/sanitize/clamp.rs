pub use icydb_testing_test_fixtures::macro_test::sanitize::clamp::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use icydb::{
        __macro::{
            encode_persisted_many_slot_payload_by_meta,
            encode_persisted_option_slot_payload_by_meta, encode_persisted_slot_payload_by_meta,
        },
        db::{InternalError, PersistedRow, SlotWriter},
        sanitize,
        traits::{Collection, EntitySchema, Inner},
        types::Decimal,
    };

    ///
    /// CaptureSlotWriter
    ///
    /// CaptureSlotWriter stores generated persisted slot payloads in-memory so
    /// macro tests can inspect the exact encoded field image before any store
    /// boundary rewraps it into a raw row.
    ///

    struct CaptureSlotWriter {
        slots: Vec<Option<Vec<u8>>>,
    }

    impl CaptureSlotWriter {
        fn new(slot_count: usize) -> Self {
            Self {
                slots: vec![None; slot_count],
            }
        }

        fn into_slots(self) -> Vec<Option<Vec<u8>>> {
            self.slots
        }
    }

    impl SlotWriter for CaptureSlotWriter {
        fn write_slot(&mut self, slot: usize, payload: Option<&[u8]>) -> Result<(), InternalError> {
            let cell = self
                .slots
                .get_mut(slot)
                .unwrap_or_else(|| panic!("test writer slot {slot} outside capture bounds"));
            *cell = payload.map(Vec::from);

            Ok(())
        }
    }

    #[entity(
        store = "TestStore",
        pk(field = "id"),
        fields(
            field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
            field(ident = "cint32", value(item(is = "ClampInt32"))),
            field(ident = "cint32_opt", value(opt, item(is = "ClampInt32"))),
            field(ident = "cdec", value(item(is = "ClampDecimal"))),
            field(ident = "cdec_opt", value(opt, item(is = "ClampDecimal"))),
            field(ident = "cdec_many", value(many, item(is = "ClampDecimal"))),
        )
    )]
    pub struct ClampEntityHarness {}

    #[test]
    fn test_clamp_int32() {
        let mut v = ClampInt32::from(5);
        sanitize(&mut v).unwrap();
        assert_eq!(*v.inner(), 10, "should clamp up to min");

        let mut v = ClampInt32::from(25);
        sanitize(&mut v).unwrap();
        assert_eq!(*v.inner(), 20, "should clamp down to max");

        let mut v = ClampInt32::from(15);
        sanitize(&mut v).unwrap();
        assert_eq!(*v.inner(), 15, "in-range value should be unchanged");
    }

    #[test]
    fn test_clamp_decimal() {
        let mut v = ClampDecimal::from(Decimal::from(0.1));
        sanitize(&mut v).unwrap();
        assert_eq!(*v.inner(), Decimal::from(0.5), "should clamp up to min");

        let mut v = ClampDecimal::from(Decimal::from(10));
        sanitize(&mut v).unwrap();
        assert_eq!(*v.inner(), Decimal::from(5.5), "should clamp down to max");

        let mut v = ClampDecimal::from(Decimal::from(2));
        sanitize(&mut v).unwrap();
        assert_eq!(
            *v.inner(),
            Decimal::from(2.0),
            "in-range value should be unchanged"
        );
    }

    #[test]
    fn test_clamp_option_fields() {
        let mut opt: Option<ClampInt32> = Some(ClampInt32::from(5));
        sanitize(&mut opt).unwrap();
        assert_eq!(
            opt.unwrap(),
            ClampInt32::from(10),
            "option should clamp inner"
        );

        let mut none: Option<ClampInt32> = None;
        sanitize(&mut none).unwrap();
        assert!(none.is_none(), "None should remain untouched");
    }

    #[test]
    fn test_clamp_list_decimal() {
        let mut list = ClampListDecimal::from(vec![
            Decimal::from(0.1),
            Decimal::from(2.0),
            Decimal::from(10.0),
        ]);
        sanitize(&mut list).unwrap();

        let expected = vec![Decimal::from(0.5), Decimal::from(2.0), Decimal::from(5.5)];
        let actual: Vec<_> = list.iter().map(|value| *value.inner()).collect();
        assert_eq!(
            actual, expected,
            "list values should be clamped element-wise"
        );
    }

    #[test]
    fn test_sanitize_entity() {
        let mut e = ClampEntityHarness {
            cint32: ClampInt32::from(5),
            cint32_opt: Some(ClampInt32::from(25)),
            cdec: ClampDecimal::from(10),
            cdec_opt: Some(ClampDecimal::from(0.1)),
            cdec_many: vec![],
            ..Default::default()
        };

        sanitize(&mut e).unwrap();

        assert_eq!(e.cint32, ClampInt32::from(10), "clamped up");
        assert_eq!(e.cint32_opt.unwrap(), ClampInt32::from(20), "clamped down");
        assert_eq!(e.cdec, ClampDecimal::from(5.5), "clamped down");
        assert_eq!(e.cdec_opt.unwrap(), ClampDecimal::from(0.5), "clamped up");
    }

    #[test]
    fn item_is_decimal_entity_slots_use_field_meta_storage_contract() {
        let entity = ClampEntityHarness {
            cdec: ClampDecimal::from(2.5),
            cdec_opt: Some(ClampDecimal::from(3.5)),
            cdec_many: vec![ClampDecimal::from(1.5), ClampDecimal::from(4.5)],
            ..Default::default()
        };
        let mut writer = CaptureSlotWriter::new(ClampEntityHarness::MODEL.fields().len());
        entity
            .write_slots(&mut writer)
            .expect("generated persisted row should write slots");
        let slots = writer.into_slots();

        assert_eq!(
            required_slot_payload(slots.as_slice(), 3),
            encode_persisted_slot_payload_by_meta(&entity.cdec, "cdec")
                .expect("required decimal metadata payload should encode")
                .as_slice(),
            "required item(is) decimal field should use field metadata storage",
        );
        assert_eq!(
            required_slot_payload(slots.as_slice(), 4),
            encode_persisted_option_slot_payload_by_meta(&entity.cdec_opt, "cdec_opt")
                .expect("optional decimal metadata payload should encode")
                .as_slice(),
            "optional item(is) decimal field should use field metadata storage",
        );
        assert_eq!(
            required_slot_payload(slots.as_slice(), 5),
            encode_persisted_many_slot_payload_by_meta(entity.cdec_many.as_slice(), "cdec_many")
                .expect("repeated decimal metadata payload should encode")
                .as_slice(),
            "repeated item(is) decimal field should use item metadata storage",
        );
    }

    fn required_slot_payload(slots: &[Option<Vec<u8>>], slot: usize) -> &[u8] {
        slots
            .get(slot)
            .and_then(Option::as_deref)
            .unwrap_or_else(|| panic!("expected captured payload for slot {slot}"))
    }
}
