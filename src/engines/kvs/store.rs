use crate::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
};

pub struct Entry {
    pub key: String,
    // removing a value is considered as setting it to an empty string
    pub value: String,
    pub timestamp: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EntryPos {
    pub pos: u64,
    pub sz: u64,
    pub timestamp: i64,
}

impl Entry {
    pub fn new(key: String, value: String) -> Entry {
        Entry {
            key,
            value,
            timestamp: Utc::now().timestamp(),
        }
    }
}

pub struct DataWriter(pub BufWriter<File>);

impl std::ops::Deref for DataWriter {
    type Target = BufWriter<File>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for DataWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl DataWriter {
    pub fn append(&mut self, e: &Entry) -> Result<EntryPos> {
        let pos = self.stream_position()?;
        // | timestamp | key_sz | value_sz | key | value |
        self.write_all(&e.timestamp.to_ne_bytes())?;
        self.write_all(&e.key.len().to_ne_bytes())?;
        self.write_all(&e.value.len().to_ne_bytes())?;
        self.write_all(e.key.as_bytes())?;
        self.write_all(e.value.as_bytes())?;
        let sz = self.stream_position()? - pos;

        Ok(EntryPos {
            pos,
            sz,
            timestamp: e.timestamp,
        })
    }
}

pub struct DataReader(pub BufReader<File>);

impl std::ops::Deref for DataReader {
    type Target = BufReader<File>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for DataReader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl DataReader {
    pub fn locate_value(&mut self, pos: u64) -> Result<(i64, String)> {
        self.seek(SeekFrom::Start(pos))?;

        let mut i64_buf = [0; (i64::BITS as usize) / 8];
        self.read_exact(&mut i64_buf)?;
        let timestamp = i64::from_ne_bytes(i64_buf);

        let mut usize_buf = [0; (usize::BITS as usize) / 8];
        self.read_exact(&mut usize_buf)?;
        let key_len = usize::from_ne_bytes(usize_buf);
        self.read_exact(&mut usize_buf)?;
        let value_len = usize::from_ne_bytes(usize_buf);

        // skip key
        self.seek(SeekFrom::Current(key_len as i64))?;
        let mut value = vec![0; value_len];
        self.read_exact(value.as_mut())?;

        Ok((timestamp, String::from_utf8(value)?))
    }

    fn get_entry(&mut self) -> Result<Entry> {
        let mut i64_buf = [0; (i64::BITS as usize) / 8];
        self.read_exact(&mut i64_buf)?;
        let timestamp = i64::from_ne_bytes(i64_buf);

        let mut usize_buf = [0; (usize::BITS as usize) / 8];
        self.read_exact(&mut usize_buf)?;
        let key_len = usize::from_ne_bytes(usize_buf);
        self.read_exact(&mut usize_buf)?;
        let value_len = usize::from_ne_bytes(usize_buf);

        let mut key = vec![0; key_len];
        self.read_exact(key.as_mut())?;
        let mut value = vec![0; value_len];
        self.read_exact(value.as_mut())?;

        Ok(Entry {
            key: String::from_utf8(key)?,
            value: String::from_utf8(value)?,
            timestamp,
        })
    }

    pub fn locate_entry(&mut self, pos: u64) -> Result<Entry> {
        self.seek(SeekFrom::Start(pos))?;
        self.get_entry()
    }

    /// Generate in-memory index used in `KvStore`.
    pub fn generate_index(
        &mut self,
        uncompacted_bytes: &mut u64,
    ) -> Result<HashMap<String, EntryPos>> {
        let mut index = HashMap::new();
        let mut pos = 0;
        self.seek(SeekFrom::Start(0))?;
        while let Ok(entry) = self.get_entry() {
            let next_pos = self.stream_position()?;
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

        Ok(index)
    }
}
