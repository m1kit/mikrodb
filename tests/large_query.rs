extern crate mikrodb;

use mikrodb::database::Database;

#[test]
fn many_transaction() {
    {
        let mut db: Database<i32, i32> =
            Database::new("many_transaction.log", "many_transaction.db").unwrap();
        db.clear().unwrap();
        for x in 0..1000 {
            let mut tx = db.begin_transaction().unwrap();
            tx.create(x, x).unwrap();
            tx.commit().unwrap();
        }
        for x in 0..1000 {
            let mut tx = db.begin_transaction().unwrap();
            tx.update(x, x + 1).unwrap();
            tx.commit().unwrap();
        }
    }
    {
        let mut db: Database<i32, i32> =
            Database::new("many_transaction.log", "many_transaction.db").unwrap();
        for x in 0..1000 {
            let mut tx = db.begin_transaction().unwrap();
            assert_eq!(tx.read(x).unwrap(), x + 1);
            tx.commit().unwrap();
        }
    }
}

#[test]
fn many_checkpoint() {
    {
        let mut db: Database<i32, String> =
            Database::new("many_checkpoint.log", "many_checkpoint.db").unwrap();
        db.clear().unwrap();
    }
    for x in 0..1000 {
        let mut db: Database<i32, String> =
            Database::new("many_checkpoint.log", "many_checkpoint.db").unwrap();
        let mut tx = db.begin_transaction().unwrap();
        tx.create(x, x.to_string()).unwrap();
        tx.commit().unwrap();
    }
    let mut db: Database<i32, String> =
        Database::new("many_checkpoint.log", "many_checkpoint.db").unwrap();
    let mut tx = db.begin_transaction().unwrap();
    for x in 0..1000 {
        tx.delete(x).unwrap();
    }
    tx.commit().unwrap();
}
