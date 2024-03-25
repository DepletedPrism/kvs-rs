use std::io::{BufWriter, Seek, Write};

/// A `BufWriter` with a position `pos` that indicates where to `seek`.
pub struct BufWriterPos<W: Write + Seek> {
    writer: BufWriter<W>,
    // uses `u64` instead of `usize` to avoid potential platform differences
    pub pos: u64,
}

impl<W: Write + Seek> BufWriterPos<W> {
    pub fn new(mut inner: W) -> std::io::Result<Self> {
        let pos = inner.seek(std::io::SeekFrom::End(0))?;
        Ok(BufWriterPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterPos<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes = self.writer.write(buf)?;
        self.pos += bytes as u64;
        Ok(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterPos<W> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
