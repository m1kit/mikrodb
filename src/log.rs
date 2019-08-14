use crate::error::DatabaseError;

use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::result::Result;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json;
use sha2::{Digest, Sha256};

/// WALレコードを表す
///
/// # レコードタイプ
/// 現在、6種類のレコードタイプをサポートする
/// - Create: キーバリューペアの新規作成
/// - Read: キーを元にバリューをルックアップする(Redoには使用しないが)
/// - Update: キーに紐付くバリューの更新
/// - Delete: キーを元にキーバリューペアの削除を行う
/// - Commit: ファイルの開始、または直前のCommit/Abortからの変更を反映する
/// - Abort: ファイルの開始、または直前のCommit/Abortからの変更を破棄する
#[derive(PartialEq, Deserialize, Serialize, Debug)]
pub enum LogRecord<K, V>
where
    K: Debug,
    V: Debug,
{
    Create { key: K, value: V },
    Read { key: K },
    Update { key: K, value: V },
    Delete { key: K },
    Commit,
    Abort,
}

/// WALレコードの読み書きに関する一連の手続きを表す
pub struct WALManager {
    file: File,
}

impl WALManager {
    /// WALマネージャを初期化する
    pub fn new(logpath: &str) -> Result<Self, DatabaseError> {
        let logfile = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .open(logpath)?;
        Result::Ok(WALManager { file: logfile })
    }

    /// WALマネージャにより管理されるログをファイルシステム上・メモリ上から破棄する
    pub fn clear(&mut self) -> Result<(), DatabaseError> {
        /// ここは atomic に中身を消したいですね。。。 たぶん truncate(2) が呼ばれるのでしょうが、
        /// atomic 保証はなさそうです。
        self.file.set_len(0)?;
        self.file.sync_all()?;
        Result::Ok(())
    }

    /// ログレコードをファイルシステムに書き込む
    ///
    /// フラグsyncを設定することで、fsyncにより確実に永続化されることが保証される。
    pub fn write_log<K, V>(
        &mut self,
        record: &LogRecord<K, V>,
        sync: bool,
    ) -> Result<(), DatabaseError>
    where
        K: Serialize + Debug,
        V: Serialize + Debug,
    {
        let body = serde_json::to_string(record)?;
        let body = body.as_bytes();

        let mut hasher = Sha256::new();
        hasher.input(body);
        let hash = hasher.result();
        let len = body.len();

        self.file.write_all(&hash[..])?;
        self.file.write_u64::<LittleEndian>(len as u64)?;
        self.file.write_all(body)?;
        if sync {
            self.file.sync_all()?;
        }
        Result::Ok(())
    }

    /// 現在ファイルシステム上に書き込まれているレコードを可能な限り取得し、ファイルをクリアする。
    /// まあ WAL が小さいときはこれでも良いですが、トランザクションひとつずつ読んで適用するのが良いと思います。
    /// さすがに WAL ファイルを少しずつ消すことは難しいので、最後にまとめてやるしかないですが。(
    /// でもそうするとやはり同一ログの複数回適用が可能(or 避けられるよう)になっている必要はあります。
    pub fn read_log<K, V>(&mut self) -> Result<Vec<LogRecord<K, V>>, DatabaseError>
    where
        K: DeserializeOwned + Debug,
        V: DeserializeOwned + Debug,
    {
        let mut result = Vec::new();
        while let Result::Ok(val) = self.read_log_entry() {
            result.push(val);
        }
        /// 順番が違いますね。log を読む --> commit/abort 判断 --> 適用 --> log 削除。
        self.clear()?;
        return Result::Ok(result);
    }

    /// 現在ファイルシステム上に書き込まれているレコードを1つ読み取る。
    fn read_log_entry<K, V>(&mut self) -> Result<LogRecord<K, V>, DatabaseError>
    where
        K: DeserializeOwned + Debug,
        V: DeserializeOwned + Debug,
    {
        let mut actual_hash = [0u8; 32];
        self.file.read_exact(&mut actual_hash)?;
        let len = self.file.read_u64::<LittleEndian>()? as usize;
        let mut buf = vec![0u8; len];
        self.file.read_exact(&mut buf[0..len])?;

        let mut hasher = Sha256::new();
        hasher.input(&buf[..]);
        let expected_hash = hasher.result();

        if &actual_hash != &expected_hash[..] {
            return Result::Err(DatabaseError::InvalidLogError {
                message: format!(
                    "Hash mismatch: expected {:x?}, but {:x?}. Body was {:x?}",
                    expected_hash, actual_hash, buf
                )
                .to_string(),
            });
        }
        let body = String::from_utf8(buf)?;
        let entry: LogRecord<K, V> = serde_json::from_str(body.as_str())?;
        return Result::Ok(entry);
    }
}

#[cfg(test)]
mod tests {
    use crate::log::{LogRecord, WALManager};

    #[test]
    fn log_rw() {
        let record = LogRecord::Create {
            key: 123,
            value: 456,
        };
        {
            let mut wal = WALManager::new("log_rw.log").unwrap();
            wal.write_log(&record, true).unwrap();
        }
        {
            let mut wal = WALManager::new("log_rw.log").unwrap();
            let result = wal.read_log().unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], record);
        }
    }
}
