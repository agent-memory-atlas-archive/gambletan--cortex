//! The local database is plaintext SQLite holding every memory, Private included.
//! It must be readable by its owner only — never by other local accounts.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use cortex_core::Cortex;

fn mode(p: &Path) -> u32 {
    std::fs::metadata(p).unwrap().permissions().mode() & 0o777
}

fn assert_owner_only(p: &Path) {
    let m = mode(p);
    assert_eq!(
        m & 0o077,
        0,
        "{} is {:o}; group/other must have no access",
        p.display(),
        m
    );
}

#[test]
fn new_database_and_wal_files_are_owner_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("memory.db");
    let cortex = Cortex::open(db.to_str().unwrap()).unwrap();
    cortex
        .ingest("my private diagnosis", "test", None, None, None)
        .unwrap();

    assert_owner_only(&db);
    for suffix in ["-wal", "-shm"] {
        let side = dir.path().join(format!("memory.db{suffix}"));
        if side.exists() {
            assert_owner_only(&side);
        }
    }
}

#[test]
fn existing_world_readable_database_is_tightened_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("memory.db");
    {
        let cortex = Cortex::open(db.to_str().unwrap()).unwrap();
        cortex
            .ingest("written by an older version", "test", None, None, None)
            .unwrap();
    }
    // Simulate a database created by an older release under a 022 umask.
    for name in ["memory.db", "memory.db-wal", "memory.db-shm"] {
        let p = dir.path().join(name);
        if p.exists() {
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
    }

    let _cortex = Cortex::open(db.to_str().unwrap()).unwrap();

    for name in ["memory.db", "memory.db-wal", "memory.db-shm"] {
        let p = dir.path().join(name);
        if p.exists() {
            assert_owner_only(&p);
        }
    }
}

#[test]
fn sqlite_uri_cannot_bypass_private_file_guard() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("uri.db");
    let uri = format!("file:{}?mode=rwc", db.display());
    assert!(Cortex::open(&uri).is_err());
    assert!(!db.exists());
    assert!(Cortex::open(":memory:").is_ok());
}

#[test]
fn symlink_and_hardlink_database_aliases_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("original.db");
    drop(Cortex::open(target.to_str().unwrap()).unwrap());
    let link = dir.path().join("symlink.db");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    assert!(Cortex::open(link.to_str().unwrap()).is_err());
    std::fs::remove_file(&link).unwrap();
    std::fs::hard_link(&target, &link).unwrap();
    assert!(Cortex::open(link.to_str().unwrap()).is_err());
}
