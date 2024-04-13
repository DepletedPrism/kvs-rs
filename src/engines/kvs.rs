use crate::{Error as KvsError, KvsEngine, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
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
        let index: HashMap<String, EntryPos> =
            if let Ok(index_file) = File::open(dir_path.join("hint")) {
                let buf_reader = BufReader::new(index_file);
                serde_json::from_reader(buf_reader)
                    .expect("unable to parse file `hint` into a `HashMap`")
            } else {
                // build HashMap from `data` when there is no `hint`
                Self::hint_from_data(&dir_path, &mut uncompacted_bytes)?
            };
        let current_file = DataFile::new(&dir_path)?;
        Ok(KvStore {
            dir_path,
            index,
            current_file,
            uncompacted_bytes,
        })
    }

    // generate an in-memory key index from `data`
    fn hint_from_data(
        dir_path: impl Into<PathBuf>,
        uncompacted_bytes: &mut u64,
    ) -> Result<HashMap<String, EntryPos>> {
        let mut index = HashMap::new();
        if let Ok(data_file) = File::open(dir_path.into().join("data")) {
            let mut reader = BufReader::new(data_file);
            reader.seek(SeekFrom::Start(0))?;
            let mut stream =
                Deserializer::from_reader(reader).into_iter::<Entry>();
            let mut pos = 0;
            while let Some(entry) = stream.next() {
                let next_pos = stream.byte_offset() as u64;
                let entry = entry?;
                if let Some(old_entry) = index.insert(
                    entry.key,
                    EntryPos {
                        pos,
                        sz: next_pos - pos,
                        timestamp: entry.timestamp,
                    },
                ) {
                    *uncompacted_bytes += old_entry.sz;
                }
                pos = next_pos;
            }
        }
        Ok(index)
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
    reader: BufReader<File>,
    writer: BufWriter<File>,
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

        Ok(DataFile { reader, writer })
    }

    fn append(&mut self, e: &Entry) -> Result<EntryPos> {
        let pos = self.writer.stream_position()?;
        serde_json::to_writer(&mut self.writer, e)?;
        self.writer.flush()?;
        let sz = self.writer.stream_position()? - pos;

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
