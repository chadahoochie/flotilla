use super::{ArchivedEntry, AsyncArchiveSink};
use crate::types::{LogIndex, Term};
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// No-op archive sink that discards entries, ideal for in-memory benchmarks and tests.
#[derive(Debug, Default, Clone)]
pub struct NullArchiveSink {
    pub total_written: u64,
}

impl NullArchiveSink {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AsyncArchiveSink for NullArchiveSink {
    type Error = io::Error;

    fn write_entries(&mut self, entries: &[ArchivedEntry]) -> Result<u64, Self::Error> {
        let count = entries.len() as u64;
        self.total_written += count;
        Ok(count)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Append-only Write-Ahead Log (WAL) sink that persists records sequentially to disk.
pub struct FileArchiveSink {
    pub path: PathBuf,
    pub writer: BufWriter<File>,
    pub total_entries: u64,
}

impl FileArchiveSink {
    /// Create a new WAL file at `path`, truncating any existing content.
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file = File::create(&path_buf)?;
        Ok(Self {
            path: path_buf,
            writer: BufWriter::new(file),
            total_entries: 0,
        })
    }

    /// Open an existing WAL file for appending.
    pub fn open_append<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path_buf)?;
        Ok(Self {
            path: path_buf,
            writer: BufWriter::new(file),
            total_entries: 0,
        })
    }

    /// Read all entries sequentially from a WAL file on disk.
    pub fn read_all<P: AsRef<Path>>(path: P) -> io::Result<Vec<ArchivedEntry>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut meta = [0u8; 20]; // term(8) + index(8) + payload_len(4)
            match reader.read_exact(&mut meta) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let term = u64::from_le_bytes(meta[0..8].try_into().unwrap());
            let index = u64::from_le_bytes(meta[8..16].try_into().unwrap());
            let payload_len = u32::from_le_bytes(meta[16..20].try_into().unwrap()) as usize;

            let mut payload = vec![0u8; payload_len];
            reader.read_exact(&mut payload)?;

            let mut crc_buf = [0u8; 4];
            reader.read_exact(&mut crc_buf)?;
            let expected_crc = u32::from_le_bytes(crc_buf);

            let mut hasher = Hasher::new();
            hasher.update(&meta);
            hasher.update(&payload);
            let calculated_crc = hasher.finalize();

            if expected_crc != calculated_crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "WAL record CRC mismatch",
                ));
            }

            entries.push(ArchivedEntry {
                index: LogIndex(index),
                term: Term(term),
                payload,
            });
        }

        Ok(entries)
    }
}

impl AsyncArchiveSink for FileArchiveSink {
    type Error = io::Error;

    fn write_entries(&mut self, entries: &[ArchivedEntry]) -> Result<u64, Self::Error> {
        for entry in entries {
            let mut meta = [0u8; 20];
            meta[0..8].copy_from_slice(&entry.term.0.to_le_bytes());
            meta[8..16].copy_from_slice(&entry.index.0.to_le_bytes());
            meta[16..20].copy_from_slice(&(entry.payload.len() as u32).to_le_bytes());

            let mut hasher = Hasher::new();
            hasher.update(&meta);
            hasher.update(&entry.payload);
            let crc = hasher.finalize();

            self.writer.write_all(&meta)?;
            self.writer.write_all(&entry.payload)?;
            self.writer.write_all(&crc.to_le_bytes())?;
            self.total_entries += 1;
        }

        Ok(entries.len() as u64)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }
}
