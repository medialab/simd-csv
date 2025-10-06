use crate::core::{CoreReader, ReadResult};
use crate::error;
use crate::records::{ByteRecord, ByteRecordBuilder};

pub struct TotalReaderBuilder {
    delimiter: u8,
    quote: u8,
}

impl Default for TotalReaderBuilder {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
        }
    }
}

impl TotalReaderBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn delimiter(&mut self, delimiter: u8) -> &mut Self {
        self.delimiter = delimiter;
        self
    }

    pub fn quote(&mut self, quote: u8) -> &mut Self {
        self.quote = quote;
        self
    }

    pub fn from_bytes<'b>(&self, bytes: &'b [u8]) -> TotalReader<'b> {
        TotalReader {
            inner: CoreReader::new(self.delimiter, self.quote),
            bytes,
            pos: 0,
        }
    }
}

// NOTE: a reader to be used when the whole data fits into memory or when using
// memory maps.
pub struct TotalReader<'b> {
    inner: CoreReader,
    bytes: &'b [u8],
    pos: usize,
}

impl<'b> TotalReader<'b> {
    pub fn from_bytes(bytes: &'b [u8]) -> Self {
        TotalReaderBuilder::new().from_bytes(bytes)
    }

    pub fn count_records(&mut self) -> u64 {
        use ReadResult::*;

        let mut count: u64 = 0;

        loop {
            let (result, pos) = self.inner.split_record(&self.bytes[self.pos..]);

            self.pos += pos;

            match result {
                End => break,
                InputEmpty | Cr | Lf => continue,
                Record => {
                    count += 1;
                }
            };
        }

        count
    }

    pub fn read_byte_record(&mut self, record: &mut ByteRecord) -> error::Result<bool> {
        use ReadResult::*;

        record.clear();

        let mut record_builder = ByteRecordBuilder::wrap(record);

        loop {
            let (result, pos) = self
                .inner
                .read_record(&self.bytes[self.pos..], &mut record_builder);

            self.pos += pos;

            match result {
                End => {
                    return Ok(false);
                }
                Cr | Lf | InputEmpty => {
                    continue;
                }
                Record => {
                    return Ok(true);
                }
            };
        }
    }
}
