use crate::{
    engines::kvs::store::{DataReader, DataWriter, Entry, EntryPos},
    Error as KvsError, KvsEngine, Result,
};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Seek, SeekFrom},
    path::{Path, PathBuf},
};

mod store;

const COMPACTION_THRESHOLD_BYTES: u64 = 1 << 20;

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
pub struct KvStore {
    // directory path
    dir_path: PathBuf,
    // in-memory key index
    index: HashMap<String, EntryPos>,
    current_file: DataFile,
    uncompacted_bytes: u64,
}

impl KvStore {
    /// Open a directory where the database is stored
    /// and create a KvStore which store key-value pairs.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir_path = path.into();
        std::fs::create_dir_all(&dir_path)?;

        let mut uncompacted_bytes = 0;
        let mut current_file = DataFile::new(&dir_path)?;

        let index: HashMap<String, EntryPos> =
            if let Ok(index_file) = File::open(dir_path.join("hint")) {
                let buf_reader = BufReader::new(index_file);
                serde_json::from_reader(buf_reader)
                    .expect("unable to parse file `hint` into a `HashMap`")
            } else {
                // build HashMap from `data` when there is no `hint`
                current_file.reader.generate_index(&mut uncompacted_bytes)?
            };

        Ok(KvStore {
            dir_path,
            index,
            current_file,
            uncompacted_bytes,
        })
    }

    // compact data to reduce meaningless disk cost
    fn compact(&mut self) -> Result<()> {
        std::fs::rename(
            self.dir_path.join("data"),
            self.dir_path.join("data.backup"),
        )?;
        let mut new_file = DataFile::new(&self.dir_path)?;
        let mut removed_key = Vec::new();

        for p in self.index.values_mut() {
            let e = self.current_file.locate_entry(p.pos)?;
            if p.timestamp == e.timestamp {
                if e.value.is_empty() {
                    removed_key.push(e.key);
                } else {
                    *p = new_file.append(&e)?;
                }
            }
        }
        for key in removed_key {
            self.index.remove(&key);
        }

        self.current_file = new_file;
        self.uncompacted_bytes = 0;
        std::fs::remove_file(self.dir_path.join("data.backup"))?;
        Ok(())
    }
}

impl KvsEngine for KvStore {
    /// Set a [`String`] key to a [`String`] value.
    ///
    /// The previous value will be overwritten when the key already exists.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let e = Entry::new(key, value);
        let p = self.current_file.append(&e)?;

        if let Some(old_p) = self.index.insert(e.key, p) {
            self.uncompacted_bytes += old_p.sz;
        }

        if self.uncompacted_bytes > COMPACTION_THRESHOLD_BYTES {
            self.compact()?;
        }
        Ok(())
    }

    /// Remove a given [`String`] key.
    ///
    /// Consider an empty string as a nonexistent value.
    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let e = Entry::new(key, "".into());
            let p = self.current_file.append(&e)?;

            self.uncompacted_bytes += p.sz;
            if let Some(old_p) = self.index.insert(e.key, p) {
                self.uncompacted_bytes += old_p.sz;
            }

            if self.uncompacted_bytes > COMPACTION_THRESHOLD_BYTES {
                self.compact()?;
            }
            Ok(())
        } else {
            Err(KvsError::NonexistentKey)
        }
    }

    /// Get the [`String`] key's corresponding value.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(p) = self.index.get(&key) {
            let (_, value) = self.current_file.locate_value(p.pos)?;
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

impl Drop for KvStore {
    fn drop(&mut self) {
        self.compact().expect("fail to compact `KvStore`");

        // create a 'hint file' to restore index in-memory the next time
        let buf_writer = BufWriter::new(
            File::create(self.dir_path.join("hint"))
                .expect("fail to create file `hint`"),
        );
        serde_json::to_writer(buf_writer, &self.index)
            .expect("fail to write serialized data into file `hint`");
    }
}

struct DataFile {
    writer: DataWriter,
    reader: DataReader,
}

impl DataFile {
    fn new(path: &Path) -> Result<DataFile> {
        let path = path.join("data");

        // considering that a read-only empty file cannot be created by `OpenOptions`,
        // define the `writer` first to avoid resulting something like
        // "Os { code: 22, kind: InvalidInput, message: "Invalid argument" }"
        let mut writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .expect("unable to create `writer`"),
        );
        writer
            .seek(SeekFrom::End(0))
            .expect("unable to seek from `SeekFrom::End(0)`");

        let reader = BufReader::new(
            OpenOptions::new()
                .read(true)
                .open(&path)
                .expect("unable to create `reader`"),
        );

        Ok(DataFile {
            writer: DataWriter(writer),
            reader: DataReader(reader),
        })
    }

    fn append(&mut self, e: &Entry) -> Result<EntryPos> {
        self.writer.append(e)
    }

    fn locate_value(&mut self, pos: u64) -> Result<(i64, String)> {
        self.reader.locate_value(pos)
    }

    fn locate_entry(&mut self, pos: u64) -> Result<Entry> {
        self.reader.locate_entry(pos)
    }
}
