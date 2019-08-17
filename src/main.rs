extern crate mikrodb;

use mikrodb::database::Database;
use std::result::Result;

fn main() {
    let mut db: Database<i32, i32> = Database::new("main.log", "main.db").unwrap();
    let mut tx = db.begin_transaction().unwrap();

    println!("Start");
    for k in 0..100000 {
        match tx.read(k) {
            Result::Err(_) => {
                println!("Record ({}, NA)", k);
                tx.create(k, -1).unwrap();
            }
            Result::Ok(v) => {
                println!("Record ({}, {})", k, v);
            }
        }
    }
    tx.commit().unwrap();

    for v in 0..100000 {
        println!("v = {}", v);
        let mut tx = db.begin_transaction().unwrap();
        for k in 0..100000 {
            tx.update(k, v).unwrap();
        }
        tx.commit().unwrap();
    }
}
