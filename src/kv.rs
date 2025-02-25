use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use thiserror::Error;

pub(crate) type Result<T> = std::result::Result<T, KvError>;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}

impl TryFrom<String> for KVPair {
    type Error = KvError;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        let kv_pair = serde_json::from_str::<KVPair>(&value)?;
        Ok(kv_pair)
    }
}

#[derive(Error, Debug)]
pub enum KvError {
    #[error(transparent)]
    JsonError(#[from] serde_json::error::Error),

    #[error(transparent)]
    FileIOError(#[from] std::io::Error),
}

pub trait KVFileIterator {
    fn file_as_mut(&mut self) -> &mut File;
    fn seek(&mut self, pos: u64) -> Result<()> {
        self.file_as_mut().seek(SeekFrom::Start(pos))?;
        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.seek(0)?;
        Ok(())
    }

    fn tell(&mut self) -> Result<u64> {
        let offset = self.file_as_mut().seek(SeekFrom::Current(0))?;
        return Ok(offset);
    }
}

pub trait KVFileReader: KVFileIterator {
    fn read(&mut self) -> Box<dyn Iterator<Item = Result<KVPair>> + '_> {
        let reader = BufReader::new(self.file_as_mut());

        return Box::new(reader.lines().map(|string| KVPair::try_from(string?)));
    }

    fn read_from_start(&mut self) -> Result<Box<dyn Iterator<Item = Result<KVPair>> + '_>> {
        self.seek(0)?;
        let reader = BufReader::new(self.file_as_mut());
        return Ok(Box::new(
            reader.lines().map(|string| KVPair::try_from(string?)),
        ));
    }
}

pub trait KVFileWriter: KVFileIterator {
    fn persist(&mut self, kv: KVPair) -> Result<u64> {
        let current_offset = self.tell()?;
        serde_json::to_writer(self.file_as_mut(), &kv)?;
        self.file_as_mut().write(b"\n")?;
        return Ok(current_offset);
    }
}
