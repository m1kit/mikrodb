extern crate mikrodb;

use mikrodb::database::Database;
use std::mem;

#[test]
fn forget1() {
    {
        let mut db: Database<i32, i32> = Database::new("forget1.log", "forget1.db").unwrap();
        db.clear().unwrap();
        let mut tx = db.begin_transaction().unwrap();
        tx.create(1, 123).unwrap();
        tx.commit().unwrap();
    }
    {
        let mut db: Database<i32, i32> = Database::new("forget1.log", "forget1.db").unwrap();
        let mut tx = db.begin_transaction().unwrap();
        assert_eq!(tx.read(1).unwrap(), 123);
        tx.update(1, 456).unwrap();
        mem::forget(tx);
        mem::forget(db);
    }
    {
        let mut db: Database<i32, i32> = Database::new("forget1.log", "forget1.db").unwrap();
        let mut tx = db.begin_transaction().unwrap();
        assert_eq!(tx.read(1).unwrap(), 123);
        tx.abort().unwrap();
    }
}

#[test]
fn redo1() {
    {
        let mut db: Database<i32, i32> = Database::new("redo1.log", "redo1.db").unwrap();
        db.clear().unwrap();
        let mut tx = db.begin_transaction().unwrap();
        tx.create(1, 123).unwrap();
        tx.commit().unwrap();
    }
    {
        let mut db: Database<i32, i32> = Database::new("redo1.log", "redo1.db").unwrap();
        let mut tx = db.begin_transaction().unwrap();
        assert_eq!(tx.read(1).unwrap(), 123);
        tx.update(1, 456).unwrap();
        tx.commit().unwrap();
        mem::forget(db);
    }
    {
        let mut db: Database<i32, i32> = Database::new("redo1.log", "redo1.db").unwrap();
        let mut tx = db.begin_transaction().unwrap();
        assert_eq!(tx.read(1).unwrap(), 456);
        tx.commit().unwrap();
    }
}
