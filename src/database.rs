use crate::error::DatabaseError;
use crate::log::{LogRecord, WALManager};
use serde::de::DeserializeOwned;
use serde::Serialize;

use std::cmp::Ord;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::result::Result;

/// データベースを表す
pub struct Database<K, V>
where
    K: Debug + Clone + Serialize + DeserializeOwned + Ord,
    V: Debug + Clone + Serialize + DeserializeOwned,
{
    wal: WALManager,
    datapath: String,
    data: BTreeMap<K, V>,
}

/// トランザクションを表す
pub struct Transaction<'tx, K, V>
where
    K: Debug + Clone + Serialize + DeserializeOwned + Ord,
    V: Debug + Clone + Serialize + DeserializeOwned,
{
    database: &'tx mut Database<K, V>,
    writeset: BTreeMap<K, V>,
}

impl<K, V> Database<K, V>
where
    K: Debug + Clone + DeserializeOwned + Serialize + Ord,
    V: Debug + Clone + DeserializeOwned + Serialize,
{
    /// データベースを初期化する
    ///
    /// これには、以下の手続きが含まれる
    /// - ファイルシステム上に永続化されたデータベースの読み込み
    /// - ファイルシステム上に永続化されたログファイルの読み込み
    /// - ログファイル上の未反映の操作のRedo(Crash-recovery)
    /// - Crash-recovery後のデータベースの永続化
    pub fn new(logpath: &str, datapath: &str) -> Result<Self, DatabaseError> {
        let wal = WALManager::new(logpath)?;
        let content = std::fs::read_to_string(datapath);
        let data: BTreeMap<K, V> = match content {
            Result::Ok(v) => serde_json::from_str(&v)?,
            Result::Err(_) => BTreeMap::new(),
        };
        let mut db = Database {
            wal: wal,
            datapath: datapath.to_string(),
            data: data,
        };

        db.crash_recover()?;
        db.exec_checkpointing()?;
        Result::Ok(db)
    }

    /// ファイルシステムおよびメモリ上からデータベースに関する内容を消去する
    pub fn clear(&mut self) -> Result<(), DatabaseError> {
        self.wal.clear()?;
        self.data.clear();
        std::fs::remove_file(&self.datapath)?;
        Result::Ok(())
    }

    fn exec_checkpointing(&mut self) -> Result<(), DatabaseError> {
        let mut datafile = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.datapath)?;
        let content = serde_json::to_string(&self.data)?;
        let content = content.as_bytes();

        datafile.write_all(content)?;
        datafile.sync_all()?;
        self.wal.clear()?;
        Result::Ok(())
    }

    /// クラッシュリカバリを行う
    fn crash_recover(&mut self) -> Result<(), DatabaseError> {
        let logs: Vec<LogRecord<K, V>> = self.wal.read_log()?;
        let mut commit: VecDeque<LogRecord<K, V>> = VecDeque::new();
        for log in logs {
            match log {
                LogRecord::Commit => {
                    while let Option::Some(v) = commit.pop_front() {
                        match v {
                            LogRecord::Create { key, value } => {
                                self.data.insert(key, value);
                            }
                            LogRecord::Update { key, value } => {
                                self.data.insert(key, value);
                            }
                            LogRecord::Delete { key } => {
                                self.data.remove(&key);
                            }
                            _ => {}
                        }
                    }
                }
                LogRecord::Abort => {
                    commit.clear();
                }
                _ => {
                    commit.push_back(log);
                }
            };
        }
        Result::Ok(())
    }

    /// トランザクションを発行する
    pub fn begin_transaction<'tx>(&'tx mut self) -> Result<Transaction<'tx, K, V>, DatabaseError> {
        return Result::Ok(Transaction {
            writeset: self.data.clone(),
            database: self,
        });
    }
}

impl<K, V> Drop for Database<K, V>
where
    K: Debug + Clone + Serialize + DeserializeOwned + Ord,
    V: Debug + Clone + Serialize + DeserializeOwned,
{
    /// データベースの永続化を行います
    fn drop(&mut self) {
        if let Result::Err(e) = self.exec_checkpointing() {
            println!("Error: {}", e.to_string());
        }
    }
}

impl<'tx, K, V> Transaction<'tx, K, V>
where
    K: Debug + Clone + Serialize + DeserializeOwned + Ord,
    V: Debug + Clone + Serialize + DeserializeOwned,
{
    /// keyに対応する値をvalueとして新規設定する
    pub fn create(&mut self, key: K, value: V) -> Result<(), DatabaseError> {
        if self.writeset.contains_key(&key) {
            return Result::Err(DatabaseError::KeyDuplicationError);
        }
        {
            let log = LogRecord::Create {
                key: key.clone(),
                value: value.clone(),
            };
            self.database.wal.write_log(&log, false)?;
        }
        self.writeset.insert(key, value);
        return Result::Ok(());
    }


    /// keyに対応する値を読み取る
    pub fn read(&mut self, key: K) -> Result<V, DatabaseError> {
        let value = self
            .writeset
            .get(&key)
            .ok_or(DatabaseError::KeyNotFoundError)?;
        {
            let log: LogRecord<K, V> = LogRecord::Read { key: key.clone() };
            self.database.wal.write_log(&log, false)?;
        }
        return Result::Ok(value.clone());
    }

    /// keyに対応する値をvalueとして更新する
    pub fn update(&mut self, key: K, value: V) -> Result<(), DatabaseError> {
        if !self.writeset.contains_key(&key) {
            return Result::Err(DatabaseError::KeyNotFoundError);
        }
        {
            let log = LogRecord::Update {
                key: key.clone(),
                value: value.clone(),
            };
            self.database.wal.write_log(&log, false)?;
        }
        self.writeset.insert(key, value);
        return Result::Ok(());
    }

    /// keyに対応する値を削除する
    pub fn delete(&mut self, key: K) -> Result<(), DatabaseError> {
        if !self.writeset.contains_key(&key) {
            return Result::Err(DatabaseError::KeyNotFoundError);
        }
        {
            let log: LogRecord<K, V> = LogRecord::Delete { key: key.clone() };
            self.database.wal.write_log(&log, false)?;
        }
        self.writeset.remove(&key);
        return Result::Ok(());
    }

    /// Commitする(トランザクションを反映する)
    pub fn commit(self) -> Result<(), DatabaseError> {
        let log: LogRecord<K, V> = LogRecord::Commit;
        self.database.wal.write_log(&log, true)?;
        self.database.data = self.writeset.clone();
        std::mem::forget(self); // Prevent abort caused by Drop
        return Result::Ok(());
    }

    /// Abortする(トランザクションを破棄する)
    pub fn abort(self) -> Result<(), DatabaseError> {
        // Drop時に自動でAbortされる
        return Result::Ok(());
    }
}

impl<'tx, K, V> Drop for Transaction<'tx, K, V>
where
    K: Debug + Clone + Serialize + DeserializeOwned + Ord,
    V: Debug + Clone + Serialize + DeserializeOwned,
{
    /// 明示的にCommitされないままDropした場合、Abort扱いとなる
    fn drop(&mut self) {
        let log: LogRecord<K, V> = LogRecord::Abort;
        if let Result::Err(e) = self.database.wal.write_log(&log, true) {
            println!("Error: {}", e.to_string());
        }
    }
}
