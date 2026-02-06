use crate::prelude::*;

///
/// WriteResponseEntity
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct WriteResponseEntity {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::db::{WriteBatchResponse, WriteResponse};

    #[test]
    fn write_response_exposes_key_and_view() {
        // Test-only identity creation (allowed in this module)
        let key = Id::<WriteResponseEntity>::from_storage_key(Ulid::generate());

        let entity = WriteResponseEntity {
            id: key,
            ..Default::default()
        };

        let response = WriteResponse::new(entity.clone());

        // 1. Identity is exposed exactly as issued
        assert_eq!(response.key(), key);

        // 2. View exposes value fields (no identity semantics)
        let view = response.view();

        // We can only assert equality against the original entity's view,
        // not against the identity itself.
        assert_eq!(view.id, entity.to_view().id);
    }

    #[test]
    fn write_batch_response_iter_and_helpers() {
        let first = WriteResponseEntity {
            id: ::icydb::traits::View::from_view(Ulid::generate()),
            ..Default::default()
        };
        let second = WriteResponseEntity {
            id: ::icydb::traits::View::from_view(Ulid::generate()),
            ..Default::default()
        };

        let batch = WriteBatchResponse::new(vec![first, second]);
        let keys = batch.keys();

        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], batch.entries()[0].key());
        assert_eq!(keys[1], batch.entries()[1].key());

        let from_ref: Vec<_> = (&batch).into_iter().map(WriteResponse::key).collect();
        assert_eq!(from_ref, keys);

        let from_owned: usize = batch.into_iter().map(WriteResponse::key).count();
        assert_eq!(from_owned, 2);
    }
}
