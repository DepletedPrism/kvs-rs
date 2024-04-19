use crate::{
    engines::kvs::store::{DataReader, DataWriter, EntryPos},
    KvsEngine, Result,
};
use crossbeam_skiplist::SkipMap;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc, Mutex},
};

mod store;

/// Used for store key-value pairs.
///
/// # Examples
///
/// ```rust
/// # use kvs::{KvStore, KvsEngine};
/// # use std::env::current_dir;
/// let mut map = KvStore::open(current_dir().unwrap().join(".kv_data")).unwrap();
/// map.set("114".to_owned(), "514".to_owned());
///
/// assert_eq!(map.get("114".to_owned()).unwrap(), Some("514".to_owned()));
/// ```
#[derive(Clone)]
pub struct KvStore {
    // in-memory key index, replace Mutex<BTreeMap<T>> with SkipMap<T>
    index: Arc<SkipMap<String, EntryPos>>,
    writer: Arc<Mutex<DataWriter>>,
    reader: DataReader,
}

impl KvStore {
    /// Open a directory where the database is stored
    /// and create a KvStore which store key-value pairs.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir_path = path.into();
        std::fs::create_dir_all(&dir_path)?;

        let id_list = store::sorted_file_id_list(&dir_path)?;
        let current_id = id_list.last().unwrap_or(&0) + 1;

        let index = Arc::new(SkipMap::new());
        let mut uncompacted_bytes = 0;
        for file_id in id_list {
            uncompacted_bytes +=
                store::generate_index(&dir_path, file_id, &index)?;
        }

        let dir_path = Arc::new(dir_path);
        let reader = DataReader {
            dir_path: dir_path.clone(),
            readers: RefCell::new(BTreeMap::new()),
            last_id: Arc::new(AtomicU64::new(0)),
        };
        let writer = DataWriter {
            dir_path: dir_path.clone(),
            index: index.clone(),
            reader: reader.clone(),
            writer: store::new_entry_writer(&dir_path, current_id)?,
            current_id,
            uncompacted_bytes,
        };

        Ok(KvStore {
            index,
            writer: Arc::new(Mutex::new(writer)),
            reader,
        })
    }
}

impl KvsEngine for KvStore {
    /// Set a [`String`] key to a [`String`] value.
    ///
    /// The previous value will be overwritten when the key already exists.
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    /// Remove a given [`String`] key.
    ///
    /// Consider an empty string as a nonexistent value.
    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }

    /// Get the [`String`] key's corresponding value.
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(p) = self.index.get(&key) {
            let (_, value) = self.reader.locate_value(p.value())?;
            if !value.is_empty() {
                Ok(Some(value))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
