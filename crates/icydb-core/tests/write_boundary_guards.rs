use std::{fs, path::PathBuf};

fn read_source(relative_path: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(relative_path);

    fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
}

#[test]
fn data_store_insert_stays_canonical_row_only() {
    let source = read_source("src/db/data/store.rs");

    assert!(
        source.contains("pub(in crate::db) fn insert(&mut self, key: RawDataKey, row: CanonicalRow) -> Option<RawRow>"),
        "DataStore::insert must remain CanonicalRow-only at the production write boundary",
    );
    assert!(
        !source.contains("pub fn insert(&mut self, key: RawDataKey, row: RawRow)"),
        "DataStore::insert must not accept RawRow in production code",
    );
}

#[test]
fn prepared_row_write_payloads_stay_canonical() {
    let prepared_op = read_source("src/db/commit/prepared_op.rs");
    let save = read_source("src/db/executor/mutation/save.rs");

    assert!(
        prepared_op.contains("pub(crate) data_value: Option<CanonicalRow>,"),
        "prepared row commit ops must carry CanonicalRow after-images",
    );
    assert!(
        !prepared_op.contains("pub(crate) data_value: Option<RawRow>,"),
        "prepared row commit ops must not regress to RawRow after-images",
    );
    assert!(
        save.contains("let row_bytes = CanonicalRow::from_entity(entity)?"),
        "typed save after-image construction must stay CanonicalRow-backed",
    );
    assert!(
        save.contains("fn build_structural_after_image_row(\n        mode: MutationMode,\n        mutation: &MutationInput,\n        old_row: Option<&RawRow>,\n    ) -> Result<CanonicalRow, InternalError>"),
        "structural save after-image builder must return CanonicalRow",
    );
}
