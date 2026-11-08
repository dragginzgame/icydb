use super::*;
use crate::testing::{test_memory, test_memory_owner};

#[test]
fn namespace_views_share_growth_without_stale_roots_or_cross_family_reads() {
    let owner = test_memory_owner(242);
    let memory = Rc::new(owner.open());
    memory.as_ref().grow(1).unwrap();
    memory.write(0, b"retained frame");
    let mut application =
        SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Application)
            .unwrap();
    let mut checkpoint =
        SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Checkpoint)
            .unwrap();
    let mut identity =
        SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Identity).unwrap();
    // Interleave enough writes to split the root through all retained views.
    for key in 0..40_u8 {
        application
            .insert([key; 32], SchemaControlBytes(vec![key; 40]))
            .expect("fixture storage operation should succeed");
        checkpoint
            .insert([key; 32], SchemaControlBytes(vec![key + 1; 40]))
            .expect("fixture storage operation should succeed");
        identity
            .insert([key; 32], SchemaControlBytes(vec![key + 2; 40]))
            .unwrap();
    }
    assert_eq!(
        (application.len()).expect("storage operation should succeed"),
        40
    );
    assert_eq!(
        (checkpoint.len()).expect("storage operation should succeed"),
        40
    );
    assert_eq!(application.keys().expect("fixture keys").count(), 40);
    assert_eq!(identity.len().unwrap(), 40);
    for key in 0..40_u8 {
        assert_eq!(
            application.get(&[key; 32]).unwrap().unwrap().0,
            vec![key; 40]
        );
        assert_eq!(
            checkpoint.get(&[key; 32]).unwrap().unwrap().0,
            vec![key + 1; 40]
        );
        assert_eq!(
            identity.get(&[key; 32]).unwrap().unwrap().0,
            vec![key + 2; 40]
        );
        application.remove(&[key; 32]).unwrap();
    }
    assert_eq!(
        (application.len()).expect("storage operation should succeed"),
        0
    );
    assert_eq!(
        (checkpoint.len()).expect("storage operation should succeed"),
        40
    );
    assert!(memory.size() <= CONTROL_START_PAGE + 3);
    let mut frame = [0; 14];
    memory.read(0, &mut frame);
    assert_eq!(&frame, b"retained frame");
    let reopened =
        SchemaControlMap::<[u8; 32]>::open_existing(memory, ControlRecordFamily::Checkpoint)
            .unwrap()
            .unwrap();
    assert_eq!(
        (reopened.len()).expect("storage operation should succeed"),
        40
    );
    assert_eq!(reopened.get(&[3; 32]).unwrap().unwrap().0, vec![4; 40]);
    drop(identity);
    drop(application);
    drop(checkpoint);
    drop(reopened);
    // Every region view retains the same exclusive lease; dropping the
    // last view must release it before the admitted allocation reopens.
    let reopened = SchemaControlMap::<[u8; 32]>::open_existing(
        Rc::new(owner.open()),
        ControlRecordFamily::Checkpoint,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        (reopened.len()).expect("storage operation should succeed"),
        40
    );
    assert_eq!(reopened.get(&[3; 32]).unwrap().unwrap().0, vec![4; 40]);
}

#[test]
fn absent_control_inspection_preserves_marker_region_without_allocating() {
    let backing = Rc::new(test_memory(241));
    assert_eq!(backing.as_ref().grow(1), Ok(0));
    backing.write(0, &[19]);
    let memory = backing.clone();
    for family in [
        ControlRecordFamily::Application,
        ControlRecordFamily::Checkpoint,
        ControlRecordFamily::Identity,
    ] {
        assert!(
            SchemaControlMap::<[u8; 32]>::open_existing(memory.clone(), family)
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(backing.size(), 1);
    let mut byte = [0];
    backing.read(0, &mut byte);
    assert_eq!(byte, [19]);
    assert_eq!(CONTROL_START_PAGE, 257);
}

#[test]
fn previous_control_tree_framing_is_rejected_without_writing() {
    let memory = Rc::new(test_memory(243));
    SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Application).unwrap();
    for retired_frame in [b"ICMAP001", b"ICMAP002"] {
        memory.write(CONTROL_START_PAGE * 65_536, retired_frame);
        let pages = memory.size();
        let mut before = vec![0; usize::try_from(pages * 65_536).unwrap()];
        memory.read(0, &mut before);

        assert!(
            SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Application)
                .is_err()
        );
        assert_eq!(memory.size(), pages);
        let mut after = vec![0; before.len()];
        memory.read(0, &mut after);
        assert_eq!(after, before);
    }
}

#[test]
fn malformed_control_headers_and_unknown_namespaces_fail_without_reinitializing() {
    // Storage admits the BTR2 identity and explicit page-size marker. Other
    // header bytes belong to its engine, which traps on malformed internals.
    for offset in (0..4).chain(8..12) {
        let memory = Rc::new(test_memory(243));
        SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Application)
            .unwrap();
        let offset = CONTROL_START_PAGE * 65_536 + offset;
        let mut original = [0];
        memory.read(offset, &mut original);
        let byte = original[0] ^ 0xFF;
        memory.write(offset, &[byte]);
        assert!(
            SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Application)
                .is_err()
        );
        let mut observed = [0];
        memory.read(offset, &mut observed);
        assert_eq!(observed, [byte]);
    }
    for tag in [0, 4] {
        let memory = Rc::new(test_memory(243));
        let mut view =
            SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Application)
                .unwrap();
        let mut key = [0; 33];
        key[0] = tag;
        key[32] = 1;
        view.map
            .insert(key, SchemaControlBytes(vec![0]))
            .expect("fixture insert");
        assert!(
            SchemaControlMap::<[u8; 32]>::open_existing(memory, ControlRecordFamily::Application)
                .is_err()
        );
    }
    let memory = Rc::new(test_memory(243));
    let mut view =
        SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Application)
            .unwrap();
    view.corrupt_header_for_tests()
        .expect("fixture storage operation should succeed");
    assert!(
        SchemaControlMap::<[u8; 32]>::open_existing(memory, ControlRecordFamily::Application)
            .is_err()
    );
}

#[test]
fn prior_control_header_is_refused_without_reinitializing_records() {
    let memory = Rc::new(test_memory(243));
    let mut view =
        SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Application)
            .unwrap();
    view.insert([7; 32], SchemaControlBytes(vec![19; 32]))
        .unwrap();
    let mut header = view.map.get(&HEADER_KEY).unwrap().unwrap();
    header.0[..8].copy_from_slice(b"ICYSCMAP");
    let checksum = crc32c(&header.0[..9]).to_le_bytes();
    header.0[9..].copy_from_slice(&checksum);
    let before = header.0.clone();
    view.map.insert(HEADER_KEY, header).unwrap();
    let pages = memory.size();
    assert!(
        SchemaControlMap::<[u8; 32]>::open_existing(memory.clone(), ControlRecordFamily::Identity)
            .is_err()
    );
    assert_eq!(view.map.get(&HEADER_KEY).unwrap().unwrap().0, before);
    assert_eq!(view.get(&[7; 32]).unwrap().unwrap().0, vec![19; 32]);
    assert_eq!(memory.size(), pages);
}
