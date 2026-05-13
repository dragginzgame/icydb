use super::*;

const SMALL_THUMBNAIL_BYTES: usize = 1_024;
const MEDIUM_THUMBNAIL_BYTES: usize = 8_192;
const LARGE_CHUNK_BYTES: usize = 65_536;
const XL_CHUNK_BYTES: usize = 131_072;

// Build deterministic blob bytes without relying on external image fixtures.
// The varied byte pattern catches accidental truncation, zero-fill, and row
// swapping while keeping expected values cheap to regenerate in assertions.
fn deterministic_blob(seed: u8, len: usize) -> Vec<u8> {
    (0u8..=250)
        .cycle()
        .take(len)
        .map(|offset| seed.wrapping_add(offset))
        .collect()
}

// Render a SQL hex blob literal (`X'...'`) from deterministic test bytes. This
// keeps large payload tests self-contained while exercising the real SQL write
// literal path instead of a typed setup shortcut.
fn hex_blob_literal(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut literal = String::with_capacity(bytes.len().saturating_mul(2).saturating_add(3));
    literal.push_str("X'");
    for byte in bytes {
        literal.push(char::from(HEX[usize::from(byte >> 4)]));
        literal.push(char::from(HEX[usize::from(byte & 0x0F)]));
    }
    literal.push('\'');

    literal
}

// Construct one large-blob SQL fixture row with ordinary metadata beside the
// binary columns so UPDATE and DELETE can target rows by scalar predicates.
fn blob_row(
    id: Ulid,
    label: &str,
    bucket: u64,
    thumbnail_seed: u8,
    thumbnail_len: usize,
    chunk_seed: u8,
    chunk_len: usize,
) -> SessionSqlBlobEntity {
    SessionSqlBlobEntity {
        id,
        label: label.to_string(),
        bucket,
        thumbnail: Blob::from(deterministic_blob(thumbnail_seed, thumbnail_len)),
        chunk: Blob::from(deterministic_blob(chunk_seed, chunk_len)),
    }
}

// Seed a mixed cohort with two large payload rows and one unrelated row. The
// unrelated row keeps SQL selectors honest when copying/updating/deleting by
// predicate rather than full-table mutation.
fn seed_blob_rows(session: &DbSession<SessionSqlCanister>) -> Vec<SessionSqlBlobEntity> {
    let rows = vec![
        blob_row(
            Ulid::from_u128(9_101),
            "hero-thumb-a",
            7,
            11,
            SMALL_THUMBNAIL_BYTES,
            31,
            LARGE_CHUNK_BYTES,
        ),
        blob_row(
            Ulid::from_u128(9_102),
            "hero-thumb-b",
            7,
            17,
            MEDIUM_THUMBNAIL_BYTES,
            37,
            XL_CHUNK_BYTES,
        ),
        blob_row(
            Ulid::from_u128(9_103),
            "archive-thumb",
            9,
            23,
            SMALL_THUMBNAIL_BYTES,
            43,
            LARGE_CHUNK_BYTES,
        ),
    ];

    for row in rows.iter().cloned() {
        session
            .insert(row)
            .expect("large blob setup insert should succeed");
    }

    rows
}

// Return one compact `(label, bucket, thumbnail_len, chunk_len)` summary
// from SQL projection rows so tests avoid diffing large byte vectors unless a
// payload mismatch actually matters.
fn blob_row_summaries(rows: Vec<Vec<Value>>) -> Vec<(String, u64, usize, usize)> {
    rows.into_iter()
        .map(|row| match row.as_slice() {
            [
                Value::Text(label),
                Value::Nat(bucket),
                Value::Blob(thumbnail),
                Value::Blob(chunk),
            ] => (label.clone(), *bucket, thumbnail.len(), chunk.len()),
            other => panic!("blob projection should emit label/bucket/blob/blob, got {other:?}"),
        })
        .collect()
}

// Extract just the blob payload pairs from `RETURNING` or `SELECT` projections
// when the test needs to prove byte-for-byte preservation.
fn blob_payload_pairs(rows: &[Vec<Value>]) -> Vec<(Vec<u8>, Vec<u8>)> {
    rows.iter()
        .map(|row| match row.as_slice() {
            [Value::Blob(thumbnail), Value::Blob(chunk)] => (thumbnail.clone(), chunk.clone()),
            other => panic!("blob payload projection should emit thumbnail/chunk, got {other:?}"),
        })
        .collect()
}

// Sort blob payload pairs by their compact shape so unordered SQL mutation
// surfaces can still be checked byte-for-byte without relying on blob ORDER BY,
// which the planner intentionally rejects.
fn blob_payload_pairs_sorted_by_shape(rows: &[Vec<Value>]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut pairs = blob_payload_pairs(rows);
    pairs.sort_by_key(|(thumbnail, chunk)| (thumbnail.len(), chunk.len()));

    pairs
}

// Select the canonical large-blob row surface used by the mutation tests.
fn select_blob_rows(
    session: &DbSession<SessionSqlCanister>,
    where_clause: &str,
) -> Vec<Vec<Value>> {
    let sql = format!(
        "SELECT label, bucket, thumbnail, chunk \
         FROM SessionSqlBlobEntity {where_clause} \
         ORDER BY label ASC"
    );

    statement_projection_rows::<SessionSqlBlobEntity>(session, sql.as_str())
        .expect("large blob SQL SELECT should succeed")
}

#[test]
fn sql_insert_select_copies_multiple_large_blob_rows() {
    reset_session_sql_store();
    let session = sql_session();
    let seeded = seed_blob_rows(&session);

    // Phase 1: copy the two hot-bucket blob rows through SQL INSERT SELECT.
    // This exercises SQL INSERT over blob values without requiring a blob
    // literal syntax in the reduced parser.
    let inserted = statement_projection_rows::<SessionSqlBlobEntity>(
        &session,
        "INSERT INTO SessionSqlBlobEntity (label, bucket, thumbnail, chunk) \
         SELECT label, bucket, thumbnail, chunk \
         FROM SessionSqlBlobEntity \
         WHERE bucket = 7 \
         ORDER BY label ASC \
         RETURNING label, bucket, thumbnail, chunk",
    )
    .expect("large blob INSERT SELECT RETURNING should succeed");

    assert_eq!(
        blob_row_summaries(inserted.clone()),
        vec![
            (
                "hero-thumb-a".to_string(),
                7,
                SMALL_THUMBNAIL_BYTES,
                LARGE_CHUNK_BYTES,
            ),
            (
                "hero-thumb-b".to_string(),
                7,
                MEDIUM_THUMBNAIL_BYTES,
                XL_CHUNK_BYTES,
            ),
        ],
        "INSERT SELECT should return copied blob rows in source order",
    );

    // Phase 2: prove the inserted rows are byte-for-byte copies, not merely
    // rows with matching lengths.
    let expected_payloads = seeded
        .iter()
        .take(2)
        .map(|row| (row.thumbnail.to_vec(), row.chunk.to_vec()))
        .collect::<Vec<_>>();

    assert_eq!(
        blob_payload_pairs(
            &inserted
                .into_iter()
                .map(|mut row| row.split_off(2))
                .collect::<Vec<_>>(),
        ),
        expected_payloads,
        "SQL INSERT SELECT RETURNING should expose exact copied thumbnail/chunk bytes",
    );

    assert_eq!(
        blob_row_summaries(select_blob_rows(&session, "WHERE bucket = 7")),
        vec![
            (
                "hero-thumb-a".to_string(),
                7,
                SMALL_THUMBNAIL_BYTES,
                LARGE_CHUNK_BYTES,
            ),
            (
                "hero-thumb-a".to_string(),
                7,
                SMALL_THUMBNAIL_BYTES,
                LARGE_CHUNK_BYTES,
            ),
            (
                "hero-thumb-b".to_string(),
                7,
                MEDIUM_THUMBNAIL_BYTES,
                XL_CHUNK_BYTES,
            ),
            (
                "hero-thumb-b".to_string(),
                7,
                MEDIUM_THUMBNAIL_BYTES,
                XL_CHUNK_BYTES,
            ),
        ],
        "SQL SELECT should observe both original and inserted blob rows",
    );
}

#[test]
fn sql_insert_values_writes_multiple_large_hex_blob_literals() {
    reset_session_sql_store();
    let session = sql_session();
    let first_thumbnail = deterministic_blob(81, SMALL_THUMBNAIL_BYTES);
    let first_chunk = deterministic_blob(82, LARGE_CHUNK_BYTES);
    let second_thumbnail = deterministic_blob(83, MEDIUM_THUMBNAIL_BYTES);
    let second_chunk = deterministic_blob(84, XL_CHUNK_BYTES);
    let sql = format!(
        "INSERT INTO SessionSqlBlobEntity (label, bucket, thumbnail, chunk) \
         VALUES \
         ('literal-a', 12, {}, {}), \
         ('literal-b', 12, {}, {}) \
         RETURNING label, bucket, thumbnail, chunk",
        hex_blob_literal(first_thumbnail.as_slice()),
        hex_blob_literal(first_chunk.as_slice()),
        hex_blob_literal(second_thumbnail.as_slice()),
        hex_blob_literal(second_chunk.as_slice()),
    );

    let inserted = statement_projection_rows::<SessionSqlBlobEntity>(&session, sql.as_str())
        .expect("large blob INSERT VALUES RETURNING should succeed");

    assert_eq!(
        blob_row_summaries(inserted.clone()),
        vec![
            (
                "literal-a".to_string(),
                12,
                SMALL_THUMBNAIL_BYTES,
                LARGE_CHUNK_BYTES,
            ),
            (
                "literal-b".to_string(),
                12,
                MEDIUM_THUMBNAIL_BYTES,
                XL_CHUNK_BYTES,
            ),
        ],
        "INSERT VALUES should return both large blob rows",
    );
    assert_eq!(
        blob_payload_pairs(
            &inserted
                .into_iter()
                .map(|mut row| row.split_off(2))
                .collect::<Vec<_>>(),
        ),
        vec![
            (first_thumbnail.clone(), first_chunk.clone()),
            (second_thumbnail.clone(), second_chunk.clone()),
        ],
        "SQL blob literals should persist exact thumbnail/chunk bytes",
    );
    assert_eq!(
        blob_payload_pairs(
            &statement_projection_rows::<SessionSqlBlobEntity>(
                &session,
                "SELECT thumbnail, chunk \
                 FROM SessionSqlBlobEntity \
                 WHERE bucket = 12 \
                 ORDER BY label ASC",
            )
            .expect("post-insert blob SELECT should succeed"),
        ),
        vec![
            (first_thumbnail, first_chunk),
            (second_thumbnail, second_chunk)
        ],
        "SQL SELECT should reload exact blob literal payloads",
    );
}

#[test]
fn sql_update_metadata_preserves_large_blob_payloads() {
    reset_session_sql_store();
    let session = sql_session();
    let seeded = seed_blob_rows(&session);
    let before_payloads = seeded
        .iter()
        .take(2)
        .map(|row| (row.thumbnail.to_vec(), row.chunk.to_vec()))
        .collect::<Vec<_>>();

    // Phase 1: update scalar metadata on rows carrying large blobs. Reduced SQL
    // UPDATE does not parse blob literals yet, so this locks the important
    // row-wide patch behavior: unchanged blob fields must survive the update.
    let updated = statement_projection_rows::<SessionSqlBlobEntity>(
        &session,
        "UPDATE SessionSqlBlobEntity \
         SET label = 'hot-updated', bucket = 70 \
         WHERE bucket = 7 \
         ORDER BY label ASC \
         RETURNING label, bucket, thumbnail, chunk",
    )
    .expect("large blob metadata UPDATE RETURNING should succeed");

    assert_eq!(
        blob_row_summaries(updated),
        vec![
            (
                "hot-updated".to_string(),
                70,
                SMALL_THUMBNAIL_BYTES,
                LARGE_CHUNK_BYTES,
            ),
            (
                "hot-updated".to_string(),
                70,
                MEDIUM_THUMBNAIL_BYTES,
                XL_CHUNK_BYTES,
            ),
        ],
        "UPDATE RETURNING should expose updated metadata beside unchanged blobs",
    );

    assert_eq!(
        blob_payload_pairs_sorted_by_shape(
            &statement_projection_rows::<SessionSqlBlobEntity>(
                &session,
                "SELECT thumbnail, chunk \
                 FROM SessionSqlBlobEntity \
                 WHERE bucket = 70",
            )
            .expect("post-update blob SELECT should succeed"),
        ),
        before_payloads,
        "SQL UPDATE should preserve untouched large blob bytes",
    );
}

#[test]
fn sql_update_writes_large_hex_blob_literals_to_multiple_rows() {
    reset_session_sql_store();
    let session = sql_session();
    seed_blob_rows(&session);
    let updated_thumbnail = deterministic_blob(91, MEDIUM_THUMBNAIL_BYTES * 2);
    let updated_chunk = deterministic_blob(92, XL_CHUNK_BYTES + LARGE_CHUNK_BYTES);
    let sql = format!(
        "UPDATE SessionSqlBlobEntity \
         SET thumbnail = {}, chunk = {} \
         WHERE bucket = 7 \
         ORDER BY label ASC \
         RETURNING label, bucket, thumbnail, chunk",
        hex_blob_literal(updated_thumbnail.as_slice()),
        hex_blob_literal(updated_chunk.as_slice()),
    );

    let updated = statement_projection_rows::<SessionSqlBlobEntity>(&session, sql.as_str())
        .expect("large blob literal UPDATE RETURNING should succeed");

    assert_eq!(
        blob_row_summaries(updated.clone()),
        vec![
            (
                "hero-thumb-a".to_string(),
                7,
                MEDIUM_THUMBNAIL_BYTES * 2,
                XL_CHUNK_BYTES + LARGE_CHUNK_BYTES,
            ),
            (
                "hero-thumb-b".to_string(),
                7,
                MEDIUM_THUMBNAIL_BYTES * 2,
                XL_CHUNK_BYTES + LARGE_CHUNK_BYTES,
            ),
        ],
        "UPDATE RETURNING should expose large literal blob replacements",
    );
    assert_eq!(
        blob_payload_pairs(
            &updated
                .into_iter()
                .map(|mut row| row.split_off(2))
                .collect::<Vec<_>>(),
        ),
        vec![
            (updated_thumbnail.clone(), updated_chunk.clone()),
            (updated_thumbnail.clone(), updated_chunk.clone()),
        ],
        "SQL UPDATE blob literals should replace each matched row exactly",
    );
    assert_eq!(
        blob_payload_pairs(
            &statement_projection_rows::<SessionSqlBlobEntity>(
                &session,
                "SELECT thumbnail, chunk \
                 FROM SessionSqlBlobEntity \
                 WHERE bucket = 7 \
                 ORDER BY label ASC",
            )
            .expect("post-update blob SELECT should succeed"),
        ),
        vec![
            (updated_thumbnail.clone(), updated_chunk.clone()),
            (updated_thumbnail, updated_chunk),
        ],
        "SQL SELECT should reload exact updated blob literal payloads",
    );
}

#[test]
fn sql_octet_length_reports_blob_byte_lengths() {
    reset_session_sql_store();
    let session = sql_session();
    seed_blob_rows(&session);

    let rows = statement_projection_rows::<SessionSqlBlobEntity>(
        &session,
        "SELECT label, OCTET_LENGTH(thumbnail), OCTET_LENGTH(chunk) \
         FROM SessionSqlBlobEntity \
         ORDER BY label ASC",
    )
    .expect("OCTET_LENGTH should project blob byte lengths");

    assert_eq!(
        rows,
        vec![
            vec![
                Value::Text("archive-thumb".to_string()),
                Value::Nat(SMALL_THUMBNAIL_BYTES as u64),
                Value::Nat(LARGE_CHUNK_BYTES as u64),
            ],
            vec![
                Value::Text("hero-thumb-a".to_string()),
                Value::Nat(SMALL_THUMBNAIL_BYTES as u64),
                Value::Nat(LARGE_CHUNK_BYTES as u64),
            ],
            vec![
                Value::Text("hero-thumb-b".to_string()),
                Value::Nat(MEDIUM_THUMBNAIL_BYTES as u64),
                Value::Nat(XL_CHUNK_BYTES as u64),
            ],
        ],
        "OCTET_LENGTH(blob) should count bytes without returning the payload",
    );
}

#[test]
fn sql_blob_equality_predicates_compare_bytes() {
    reset_session_sql_store();
    let session = sql_session();
    let seeded = seed_blob_rows(&session);
    let matching_thumbnail = seeded[0].thumbnail.to_vec();
    let matching_literal = hex_blob_literal(matching_thumbnail.as_slice());

    let equal_rows = statement_projection_rows::<SessionSqlBlobEntity>(
        &session,
        format!(
            "SELECT label \
             FROM SessionSqlBlobEntity \
             WHERE thumbnail = {matching_literal} \
             ORDER BY label ASC"
        )
        .as_str(),
    )
    .expect("blob equality predicate should compare exact bytes");

    assert_eq!(
        equal_rows,
        vec![vec![Value::Text("hero-thumb-a".to_string())]],
        "blob equality should return only the row with matching bytes",
    );

    let not_equal_rows = statement_projection_rows::<SessionSqlBlobEntity>(
        &session,
        format!(
            "SELECT label \
             FROM SessionSqlBlobEntity \
             WHERE thumbnail <> {matching_literal} \
             ORDER BY label ASC"
        )
        .as_str(),
    )
    .expect("blob inequality predicate should compare exact bytes");

    assert_eq!(
        not_equal_rows,
        vec![
            vec![Value::Text("archive-thumb".to_string())],
            vec![Value::Text("hero-thumb-b".to_string())],
        ],
        "blob inequality should exclude only the row with matching bytes",
    );
}

#[test]
fn sql_order_by_blob_field_is_rejected() {
    reset_session_sql_store();
    let session = sql_session();
    seed_blob_rows(&session);

    let err = statement_projection_rows::<SessionSqlBlobEntity>(
        &session,
        "SELECT label \
         FROM SessionSqlBlobEntity \
         ORDER BY chunk ASC",
    )
    .expect_err("ORDER BY over a raw blob field should fail planner validation");

    assert!(
        err.to_string()
            .contains("order field 'chunk' is not orderable"),
        "blob ORDER BY should preserve the unorderable-field planner error",
    );
}

#[test]
fn typed_replace_then_sql_select_and_delete_large_blobs() {
    reset_session_sql_store();
    let session = sql_session();
    seed_blob_rows(&session);
    let replacement = blob_row(
        Ulid::from_u128(9_102),
        "hero-thumb-b-replaced",
        11,
        61,
        MEDIUM_THUMBNAIL_BYTES * 2,
        71,
        XL_CHUNK_BYTES + LARGE_CHUNK_BYTES,
    );

    // Phase 1: keep typed replace covered for blob-heavy save paths. SQL literal
    // INSERT/UPDATE coverage above verifies the parser-owned blob write lane;
    // this test locks replacement rows that are then observed through SQL.
    session
        .replace(replacement.clone())
        .expect("typed large blob replace should succeed");

    assert_eq!(
        blob_row_summaries(select_blob_rows(
            &session,
            "WHERE label = 'hero-thumb-b-replaced'"
        )),
        vec![(
            "hero-thumb-b-replaced".to_string(),
            11,
            MEDIUM_THUMBNAIL_BYTES * 2,
            XL_CHUNK_BYTES + LARGE_CHUNK_BYTES,
        )],
        "SQL SELECT should observe the typed replacement blob sizes",
    );
    assert_eq!(
        blob_payload_pairs(
            &statement_projection_rows::<SessionSqlBlobEntity>(
                &session,
                "SELECT thumbnail, chunk \
                 FROM SessionSqlBlobEntity \
                 WHERE label = 'hero-thumb-b-replaced'",
            )
            .expect("replacement blob SELECT should succeed"),
        ),
        vec![(replacement.thumbnail.to_vec(), replacement.chunk.to_vec())],
        "SQL SELECT should observe exact replacement bytes",
    );

    // Phase 2: delete a bounded window of blob rows and require RETURNING to
    // materialize the large payloads before the rows disappear.
    let deleted = statement_projection_rows::<SessionSqlBlobEntity>(
        &session,
        "DELETE FROM SessionSqlBlobEntity \
         WHERE bucket >= 7 \
           AND label LIKE 'hero%' \
         ORDER BY label ASC \
         LIMIT 2 \
         RETURNING label, bucket, thumbnail, chunk",
    )
    .expect("large blob DELETE RETURNING should succeed");

    assert_eq!(
        blob_row_summaries(deleted),
        vec![
            (
                "hero-thumb-a".to_string(),
                7,
                SMALL_THUMBNAIL_BYTES,
                LARGE_CHUNK_BYTES,
            ),
            (
                "hero-thumb-b-replaced".to_string(),
                11,
                MEDIUM_THUMBNAIL_BYTES * 2,
                XL_CHUNK_BYTES + LARGE_CHUNK_BYTES,
            ),
        ],
        "DELETE RETURNING should preserve ordered blob rows before removal",
    );

    assert_eq!(
        blob_row_summaries(select_blob_rows(&session, "")),
        vec![(
            "archive-thumb".to_string(),
            9,
            SMALL_THUMBNAIL_BYTES,
            LARGE_CHUNK_BYTES,
        )],
        "DELETE should leave only the non-windowed blob row",
    );
}
