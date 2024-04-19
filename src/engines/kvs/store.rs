use crate::{Error, Result};
use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use std::{
    cell::RefCell,
    collections::{btree_map, BTreeMap},
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

struct Entry {
    key: String,
    // removing a value is considered as setting it to an empty string
    value: String,
    timestamp: i64,
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

#[derive(Debug)]
pub struct EntryPos {
    file_id: u64,
    pos: u64,
    sz: u64,
    timestamp: i64,
}

fn append_entry(
    writer: &mut BufWriter<File>,
    file_id: u64,
    e: &Entry,
) -> Result<EntryPos> {
    let pos = writer.stream_position()?;
    // | timestamp | key_sz | value_sz | key | value |
    writer.write_all(&e.timestamp.to_ne_bytes())?;
    writer.write_all(&e.key.len().to_ne_bytes())?;
    writer.write_all(&e.value.len().to_ne_bytes())?;
    writer.write_all(e.key.as_bytes())?;
    writer.write_all(e.value.as_bytes())?;
    let sz = writer.stream_position()? - pos;

    Ok(EntryPos {
        file_id,
        pos,
        sz,
        timestamp: e.timestamp,
    })
}

fn read_entry(reader: &mut BufReader<File>) -> Result<Entry> {
    let mut i64_buf = [0; (i64::BITS as usize) / 8];
    reader.read_exact(&mut i64_buf)?;
    let timestamp = i64::from_ne_bytes(i64_buf);

    let mut usize_buf = [0; (usize::BITS as usize) / 8];
    reader.read_exact(&mut usize_buf)?;
    let key_len = usize::from_ne_bytes(usize_buf);
    reader.read_exact(&mut usize_buf)?;
    let value_len = usize::from_ne_bytes(usize_buf);

    let mut key = vec![0; key_len];
    reader.read_exact(key.as_mut())?;
    let mut value = vec![0; value_len];
    reader.read_exact(value.as_mut())?;

    Ok(Entry {
        key: String::from_utf8(key)?,
        value: String::from_utf8(value)?,
        timestamp,
    })
}

// get path to file `data-{file_id}`
fn data_file_path(dir_path: &Path, file_id: u64) -> PathBuf {
    dir_path.join(format!("data-{file_id}"))
}

/// Generate in-memory index used in `KvStore` for given reader.
pub fn generate_index(
    dir_path: &Path,
    file_id: u64,
    index: &SkipMap<String, EntryPos>,
) -> Result<u64> {
    let mut reader =
        BufReader::new(File::open(data_file_path(dir_path, file_id))?);
    let mut uncompacted_bytes = 0;
    let mut pos = 0;
    reader.seek(SeekFrom::Start(0))?;
    while let Ok(e) = read_entry(&mut reader) {
        let next_pos = reader.stream_position()?;
        if index.contains_key(&e.key) {
            let old_e = index.get(&e.key).unwrap();
            uncompacted_bytes += old_e.value().sz;
        }
        index.insert(
            e.key,
            EntryPos {
                file_id,
                pos,
                sz: next_pos - pos,
                timestamp: e.timestamp,
            },
        );
        pos = next_pos;
    }

    Ok(uncompacted_bytes)
}

pub fn sorted_file_id_list(dir_path: &std::path::Path) -> Result<Vec<u64>> {
    let mut id_list: Vec<u64> = std::fs::read_dir(dir_path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file())
        .flat_map(|path| {
            path.file_name()
                .and_then(std::ffi::OsStr::to_str)
                .map(|s| s.trim_start_matches("data-"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    id_list.sort_unstable();
    Ok(id_list)
}

const COMPACTION_THRESHOLD_BYTES: u64 = 1 << 20;

pub struct DataWriter {
    pub dir_path: Arc<PathBuf>,
    pub index: Arc<SkipMap<String, EntryPos>>,
    pub writer: BufWriter<File>,
    pub reader: DataReader,
    pub current_id: u64,
    pub uncompacted_bytes: u64,
}

pub fn new_entry_writer(
    dir_path: &Path,
    file_id: u64,
) -> Result<BufWriter<File>> {
    let mut writer = BufWriter::new(
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(data_file_path(dir_path, file_id))?,
    );
    writer.seek(SeekFrom::End(0))?;
    Ok(writer)
}

impl DataWriter {
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let e = Entry::new(key, value);
        let p = append_entry(&mut self.writer, self.current_id, &e)?;

        if let Some(old_p) = self.index.get(&e.key) {
            self.uncompacted_bytes += old_p.value().sz;
        }
        self.index.insert(e.key, p);
        if self.uncompacted_bytes > COMPACTION_THRESHOLD_BYTES {
            self.compact()?;
        }

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let e = Entry::new(key, "".into());
            let p = append_entry(&mut self.writer, self.current_id, &e)?;

            self.uncompacted_bytes += p.sz;
            if let Some(old_p) = self.index.get(&e.key) {
                self.uncompacted_bytes += old_p.value().sz;
            }
            self.index.insert(e.key, p);
            if self.uncompacted_bytes > COMPACTION_THRESHOLD_BYTES {
                self.compact()?;
            }

            Ok(())
        } else {
            Err(Error::NonexistentKey)
        }
    }

    // compact data to reduce meaningless disk cost
    fn compact(&mut self) -> Result<()> {
        let compact_id = self.current_id + 1;
        self.current_id += 2;
        self.writer = new_entry_writer(&self.dir_path, self.current_id)?;
        self.reader.last_id.store(self.current_id, Ordering::SeqCst);

        let mut compact_writer = new_entry_writer(&self.dir_path, compact_id)?;
        for p in self.index.iter() {
            let e = self.reader.locate_entry(p.value())?;
            if p.value().timestamp == e.timestamp {
                if e.value.is_empty() {
                    self.index.remove(p.key());
                } else {
                    self.index.insert(
                        p.key().clone(),
                        append_entry(&mut compact_writer, self.current_id, &e)?,
                    );
                }
            }
        }
        for file_id in sorted_file_id_list(&self.dir_path)?
            .into_iter()
            .filter(|x| *x < compact_id)
        {
            std::fs::remove_file(data_file_path(&self.dir_path, file_id))?;
        }
        self.uncompacted_bytes = 0;

        Ok(())
    }
}

/// A set of readers.
pub struct DataReader {
    pub dir_path: Arc<PathBuf>,
    pub readers: RefCell<BTreeMap<u64, BufReader<File>>>,
    // all readers whose id less than last_id is invalid
    pub last_id: Arc<AtomicU64>,
}

impl Clone for DataReader {
    fn clone(&self) -> Self {
        DataReader {
            dir_path: self.dir_path.clone(),
            readers: RefCell::new(BTreeMap::new()),
            last_id: self.last_id.clone(),
        }
    }
}

impl DataReader {
    /// Remove redundancy readers.
    fn remove_redundancy(&self) {
        let mut readers = self.readers.borrow_mut();

        while !readers.is_empty() {
            let key = *readers.first_entry().unwrap().key();
            if key >= self.last_id.load(Ordering::SeqCst) {
                break;
            }
            readers.remove(&key);
        }
    }

    fn check_availability<'a>(
        &self,
        readers: &'a mut std::cell::RefMut<'_, BTreeMap<u64, BufReader<File>>>,
        p: &EntryPos,
    ) -> Result<&'a mut BufReader<File>> {
        if let btree_map::Entry::Vacant(e) = readers.entry(p.file_id) {
            let reader = BufReader::new(File::open(data_file_path(
                &self.dir_path,
                p.file_id,
            ))?);
            e.insert(reader);
        }
        let reader = readers.get_mut(&p.file_id).unwrap();
        reader.seek(SeekFrom::Start(p.pos))?;
        Ok(reader)
    }

    pub fn locate_value(&self, p: &EntryPos) -> Result<(i64, String)> {
        self.remove_redundancy();
        let mut readers = self.readers.borrow_mut();
        let reader = self.check_availability(&mut readers, p)?;

        let mut i64_buf = [0; (i64::BITS as usize) / 8];
        reader.read_exact(&mut i64_buf)?;
        let timestamp = i64::from_ne_bytes(i64_buf);

        let mut usize_buf = [0; (usize::BITS as usize) / 8];
        reader.read_exact(&mut usize_buf)?;
        let key_len = usize::from_ne_bytes(usize_buf);
        reader.read_exact(&mut usize_buf)?;
        let value_len = usize::from_ne_bytes(usize_buf);

        // skip key
        reader.seek(SeekFrom::Current(key_len as i64))?;
        let mut value = vec![0; value_len];
        reader.read_exact(value.as_mut())?;

        Ok((timestamp, String::from_utf8(value)?))
    }

    fn locate_entry(&self, p: &EntryPos) -> Result<Entry> {
        self.remove_redundancy();
        let mut readers = self.readers.borrow_mut();
        let reader = self.check_availability(&mut readers, p)?;
        read_entry(reader)
    }
}
