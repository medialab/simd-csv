use std::io::Read;

use crate::buffer::ScratchBuffer;
use crate::core::{CoreReader, ReadResult};
use crate::error::{self, Error};
use crate::records::ZeroCopyByteRecord;

pub struct ZeroCopyReaderBuilder {
    delimiter: u8,
    quote: u8,
    buffer_capacity: Option<usize>,
    flexible: bool,
}

impl Default for ZeroCopyReaderBuilder {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            buffer_capacity: None,
            flexible: false,
        }
    }
}

impl ZeroCopyReaderBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut reader = Self::default();
        reader.buffer_capacity(capacity);
        reader
    }

    pub fn delimiter(&mut self, delimiter: u8) -> &mut Self {
        self.delimiter = delimiter;
        self
    }

    pub fn quote(&mut self, quote: u8) -> &mut Self {
        self.quote = quote;
        self
    }

    pub fn buffer_capacity(&mut self, capacity: usize) -> &mut Self {
        self.buffer_capacity = Some(capacity);
        self
    }

    pub fn flexible(&mut self, yes: bool) -> &mut Self {
        self.flexible = yes;
        self
    }

    pub fn from_reader<R: Read>(&self, reader: R) -> ZeroCopyReader<R> {
        ZeroCopyReader {
            buffer: ScratchBuffer::with_optional_capacity(self.buffer_capacity, reader),
            inner: CoreReader::new(self.delimiter, self.quote),
            field_count: None,
            seps: Vec::new(),
            flexible: self.flexible,
        }
    }
}

pub struct ZeroCopyReader<R> {
    buffer: ScratchBuffer<R>,
    inner: CoreReader,
    field_count: Option<usize>,
    seps: Vec<usize>,
    flexible: bool,
}

impl<R: Read> ZeroCopyReader<R> {
    pub fn from_reader(reader: R) -> Self {
        ZeroCopyReaderBuilder::new().from_reader(reader)
    }

    #[inline]
    fn check_field_count(&mut self, written: usize) -> error::Result<()> {
        if self.flexible {
            return Ok(());
        }

        match self.field_count {
            Some(expected) => {
                if written != expected {
                    return Err(Error::unequal_lengths(expected, written));
                }
            }
            None => {
                self.field_count = Some(written);
            }
        }

        Ok(())
    }

    pub fn strip_bom(&mut self) -> error::Result<()> {
        self.buffer.strip_bom()?;
        Ok(())
    }

    pub fn read_byte_record(&mut self) -> error::Result<Option<ZeroCopyByteRecord<'_>>> {
        use ReadResult::*;

        self.buffer.reset();
        self.seps.clear();

        loop {
            let seps_offset = self.buffer.saved().len();
            let input = self.buffer.fill_buf()?;

            let (result, pos) =
                self.inner
                    .split_record_and_find_separators(input, seps_offset, &mut self.seps);

            match result {
                End => {
                    self.buffer.consume(pos);
                    return Ok(None);
                }
                Cr | Lf => {
                    self.buffer.consume(pos);
                }
                InputEmpty => {
                    self.buffer.save();
                }
                Record => {
                    self.check_field_count(self.seps.len() + 1)?;

                    let record = ZeroCopyByteRecord::new(self.buffer.flush(pos), &self.seps);

                    return Ok(Some(record));
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_read_zero_copy_byte_record() -> error::Result<()> {
        let csv = "name,surname,age\n\"john\",\"landy, the \"\"everlasting\"\" bastard\",45\nlucy,rose,\"67\"\njermaine,jackson,\"89\"\n\nkarine,loucan,\"52\"\nrose,\"glib\",12\n\"guillaume\",\"plique\",\"42\"\r\n";

        let mut reader = ZeroCopyReaderBuilder::with_capacity(32).from_reader(Cursor::new(csv));
        let mut records = Vec::new();

        let expected = vec![
            vec!["name", "surname", "age"],
            vec![
                "\"john\"",
                "\"landy, the \"\"everlasting\"\" bastard\"",
                "45",
            ],
            vec!["lucy", "rose", "\"67\""],
            vec!["jermaine", "jackson", "\"89\""],
            vec!["karine", "loucan", "\"52\""],
            vec!["rose", "\"glib\"", "12"],
            vec!["\"guillaume\"", "\"plique\"", "\"42\""],
        ]
        .into_iter()
        .map(|record| {
            record
                .into_iter()
                .map(|cell| cell.as_bytes().to_vec())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

        while let Some(record) = reader.read_byte_record()? {
            records.push(record.iter().map(|cell| cell.to_vec()).collect::<Vec<_>>());
        }

        assert_eq!(records, expected);

        Ok(())
    }

    #[test]
    fn test_empty_row() -> error::Result<()> {
        let data = "name\n\"\"\nlucy\n\"\"";

        // Zero-copy
        let mut reader = ZeroCopyReader::from_reader(Cursor::new(data));

        let expected = vec![
            vec!["name".as_bytes().to_vec()],
            vec!["\"\"".as_bytes().to_vec()],
            vec!["lucy".as_bytes().to_vec()],
            vec!["\"\"".as_bytes().to_vec()],
        ];

        // Read
        let mut records = Vec::new();

        while let Some(record) = reader.read_byte_record()? {
            records.push(vec![record.as_slice().to_vec()]);
        }

        assert_eq!(records, expected);

        Ok(())
    }
}
