use super::*;

const SMALL_THUMBNAIL_BYTES: usize = 1_024;
const MEDIUM_THUMBNAIL_BYTES: usize = 8_192;
const LARGE_CHUNK_BYTES: usize = 65_536;
const XL_CHUNK_BYTES: usize = 131_072;

// Build deterministic blob bytes without relying on external image fixtures.
// The varied byte pattern catches accidental truncation, zero-fill, and row
// swapping while keeping expected values cheap to regenerate in assertions.
fn deterministic_blob(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| seed.wrapping_add((index % 251) as u8))
        .collect()
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
                Value::Uint(bucket),
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
    let copied_payloads = blob_payload_pairs(
        &statement_projection_rows::<SessionSqlBlobEntity>(
            &session,
            "SELECT thumbnail, chunk \
             FROM SessionSqlBlobEntity \
             WHERE bucket = 7 \
             ORDER BY label ASC \
             OFFSET 2",
        )
        .expect("copied blob SELECT should succeed"),
    );
    let expected_payloads = seeded
        .iter()
        .take(2)
        .map(|row| (row.thumbnail.to_vec(), row.chunk.to_vec()))
        .collect::<Vec<_>>();

    assert_eq!(
        copied_payloads, expected_payloads,
        "SQL INSERT SELECT should persist exact thumbnail/chunk bytes",
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
        blob_row_summaries(updated.clone()),
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
        blob_payload_pairs(
            &statement_projection_rows::<SessionSqlBlobEntity>(
                &session,
                "SELECT thumbnail, chunk \
                 FROM SessionSqlBlobEntity \
                 WHERE bucket = 70 \
                 ORDER BY chunk ASC",
            )
            .expect("post-update blob SELECT should succeed"),
        ),
        before_payloads,
        "SQL UPDATE should preserve untouched large blob bytes",
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

    // Phase 1: use the typed replace lane for the actual blob mutation because
    // reduced SQL has no blob literal syntax. The entity remains the same SQL
    // fixture and is immediately verified through SQL SELECT.
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
