use crate::{io::BufWriterPos, Error as KvsError, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write, Seek, SeekFrom},
    path::{Path, PathBuf},
};

const COMPACTION_THRESHOLD_BYTES: u64 = 1 << 20;

#[derive(Deserialize, Serialize)]
struct Entry {
    key: String,
    // removing a value is considered as setting it to an empty string
    value: String,
    timestamp: i64,
}

impl Entry {
    fn new(key: String, value: String) -> Entry {
        Entry {
            key,
            value,
            timestamp: Utc::now().timestamp(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct EntryPos {
    pos: u64,
    sz: u64,
    timestamp: i64,
}

/// Used for store key-value pairs.
///
/// # Examples
///
/// ```rust
/// # use kvs::KvStore;
/// # use std::env::current_dir;
/// let mut map = KvStore::open(current_dir().unwrap()).unwrap();
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
        let dir_path = path.into().join(".kv_data");
        std::fs::create_dir_all(&dir_path)?;

        let index: HashMap<String, EntryPos> =
            if let Ok(index_file) = File::open(dir_path.join("hint")) {
                let buf_reader = BufReader::new(index_file);
                serde_json::from_reader(buf_reader)
                    .expect("unable to parse file `hint` into a `HashMap`")
            } else {
                HashMap::new()
                // TODO: build HashMap from `data`
            };
        let current_file = DataFile::new(&dir_path)?;
        Ok(KvStore {
            dir_path,
            index,
            current_file,
            uncompacted_bytes: 0,
        })
    }

    /// Set a [`String`] key to a [`String`] value.
    ///
    /// The previous value will be overwritten when the key already exists.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
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
    pub fn remove(&mut self, key: String) -> Result<()> {
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
            Err(KvsError::NonexistentKey(key))
        }
    }

    /// Get the [`String`] key's corresponding value.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(p) = self.index.get(&key) {
            let Entry { value, .. } = self.current_file.locate_entry(p)?;
            if !value.is_empty() {
                Ok(Some(value))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
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
            let e = self.current_file.locate_entry(p)?;
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

impl Drop for KvStore {
    fn drop(&mut self) {
        self.compact().expect("fail to compact `KvStore`");

        // create a 'hint file' to restore index in-memory the next time
        let buf_writer = BufWriter::new(
            File::create(self.dir_path.join("hint")).expect("fail to create file `hint`"),
        );

        serde_json::to_writer(buf_writer, &self.index)
            .expect("fail to write serialized data into file `hint`");
    }
}

struct DataFile {
    reader: BufReader<File>,
    writer: BufWriterPos<File>,
}

impl DataFile {
    fn new(path: &Path) -> Result<DataFile> {
        let path = path.join("data");

        // considering that a read-only empty file cannot be created by `OpenOptions`,
        // define the `writer` first to avoid resulting something like
        // "Os { code: 22, kind: InvalidInput, message: "Invalid argument" }"
        let writer = BufWriterPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&path)
                .expect("unable to create `writer`"),
        )?;

        let reader = BufReader::new(
            OpenOptions::new()
                .read(true)
                .open(&path)
                .expect("unable to create `reader`"),
        );

        Ok(DataFile { reader, writer })
    }

    fn append(&mut self, e: &Entry) -> Result<EntryPos> {
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, e)?;
        self.writer.flush()?;
        let sz = self.writer.pos - pos;

        Ok(EntryPos {
            pos,
            sz,
            timestamp: e.timestamp,
        })
    }

    fn locate_entry(&mut self, p: &EntryPos) -> Result<Entry> {
        let mut buf = vec![0; p.sz as usize];

        self.reader.seek(SeekFrom::Start(p.pos))?;
        self.reader.read_exact(&mut buf)?;

        Ok(serde_json::from_slice(buf.as_slice())?)
    }
}
