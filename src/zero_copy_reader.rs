use std::io::{BufReader, Read};

use crate::buffer::ScratchBuffer;
use crate::core::{CoreReader, ReadResult};
use crate::error::{self, Error, ErrorKind};
use crate::records::{ByteRecord, ZeroCopyByteRecord};
use crate::utils::trim_bom;

pub struct ZeroCopyReaderBuilder {
    delimiter: u8,
    quote: u8,
    buffer_capacity: Option<usize>,
    flexible: bool,
    has_headers: bool,
}

impl Default for ZeroCopyReaderBuilder {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            buffer_capacity: None,
            flexible: false,
            has_headers: true,
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

    pub fn has_headers(&mut self, yes: bool) -> &mut Self {
        self.has_headers = yes;
        self
    }

    pub fn from_reader<R: Read>(&self, reader: R) -> ZeroCopyReader<R> {
        ZeroCopyReader {
            buffer: ScratchBuffer::with_optional_capacity(self.buffer_capacity, reader),
            inner: CoreReader::new(self.delimiter, self.quote),
            byte_headers: ByteRecord::new(),
            raw_headers: (Vec::new(), Vec::new()),
            seps: Vec::new(),
            flexible: self.flexible,
            has_read: false,
            must_reemit_headers: !self.has_headers,
            index: 0,
        }
    }
}

pub struct ZeroCopyReader<R> {
    buffer: ScratchBuffer<R>,
    inner: CoreReader,
    byte_headers: ByteRecord,
    raw_headers: (Vec<usize>, Vec<u8>),
    seps: Vec<usize>,
    flexible: bool,
    has_read: bool,
    must_reemit_headers: bool,
    index: u64,
}

impl<R: Read> ZeroCopyReader<R> {
    pub fn from_reader(reader: R) -> Self {
        ZeroCopyReaderBuilder::new().from_reader(reader)
    }

    #[inline]
    fn check_field_count(&mut self, byte: u64, written: usize) -> error::Result<()> {
        if self.flexible {
            return Ok(());
        }

        let headers_len = self.raw_headers.0.len() + 1;

        if self.has_read && written != headers_len {
            return Err(Error::new(ErrorKind::UnequalLengths {
                expected_len: headers_len,
                len: written,
                pos: Some((byte, self.index)),
            }));
        }

        Ok(())
    }

    #[inline]
    fn on_first_read(&mut self) -> error::Result<()> {
        if self.has_read {
            return Ok(());
        }

        // Trimming BOM
        let input = self.buffer.fill_buf()?;
        let bom_len = trim_bom(input);
        self.buffer.consume(bom_len);

        // Reading headers
        let mut headers_seps = Vec::new();
        let mut headers_slice = Vec::new();
        let mut byte_headers = ByteRecord::new();

        if let Some(headers) = self.read_byte_record_impl()? {
            (headers_seps, headers_slice) = headers.to_parts();
            byte_headers = headers.to_byte_record();
        } else {
            self.must_reemit_headers = false;
        }

        self.raw_headers = (headers_seps, headers_slice);
        self.byte_headers = byte_headers;

        self.has_read = true;

        Ok(())
    }

    #[inline]
    pub fn byte_headers(&mut self) -> error::Result<&ByteRecord> {
        self.on_first_read()?;

        Ok(&self.byte_headers)
    }

    fn read_byte_record_impl(&mut self) -> error::Result<Option<ZeroCopyByteRecord<'_>>> {
        use ReadResult::*;

        self.buffer.reset();
        self.seps.clear();

        let byte = self.position();

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
                    self.index += 1;
                    self.check_field_count(byte, self.seps.len() + 1)?;

                    let record = ZeroCopyByteRecord::new(
                        self.buffer.flush(pos),
                        &self.seps,
                        self.inner.quote,
                    );

                    return Ok(Some(record));
                }
            };
        }
    }

    #[inline(always)]
    pub fn read_byte_record(&mut self) -> error::Result<Option<ZeroCopyByteRecord<'_>>> {
        self.on_first_read()?;

        if self.must_reemit_headers {
            self.must_reemit_headers = false;
            return Ok(Some(ZeroCopyByteRecord::new(
                &self.raw_headers.1,
                &self.raw_headers.0,
                self.inner.quote,
            )));
        }

        self.read_byte_record_impl()
    }

    pub fn into_bufreader(self) -> BufReader<R> {
        self.buffer.into_bufreader()
    }

    #[inline(always)]
    pub fn position(&self) -> u64 {
        self.buffer.position()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::brec;

    use super::*;

    impl<R: Read> ZeroCopyReader<R> {
        fn from_reader_no_headers(reader: R) -> Self {
            ZeroCopyReaderBuilder::new()
                .has_headers(false)
                .from_reader(reader)
        }
    }

    #[test]
    fn test_read_zero_copy_byte_record() -> error::Result<()> {
        let csv = "name,surname,age\n\"john\",\"landy, the \"\"everlasting\"\" bastard\",45\nlucy,rose,\"67\"\njermaine,jackson,\"89\"\n\nkarine,loucan,\"52\"\nrose,\"glib\",12\n\"guillaume\",\"plique\",\"42\"\r\n";

        let mut reader = ZeroCopyReaderBuilder::with_capacity(32)
            .has_headers(false)
            .from_reader(Cursor::new(csv));
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
        let mut reader = ZeroCopyReader::from_reader_no_headers(Cursor::new(data));

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

    #[test]
    fn test_byte_headers() -> error::Result<()> {
        let data = b"name,surname\njohn,dandy";

        // Headers, call before read
        let mut reader = ZeroCopyReader::from_reader(Cursor::new(data));
        assert_eq!(reader.byte_headers()?, &brec!["name", "surname"]);
        assert_eq!(
            reader.read_byte_record()?.unwrap().to_byte_record(),
            brec!["john", "dandy"]
        );

        // Headers, call after read
        let mut reader = ZeroCopyReader::from_reader(Cursor::new(data));
        assert_eq!(
            reader.read_byte_record()?.unwrap().to_byte_record(),
            brec!["john", "dandy"]
        );
        assert_eq!(reader.byte_headers()?, &brec!["name", "surname"]);

        // No headers, call before read
        let mut reader = ZeroCopyReader::from_reader_no_headers(Cursor::new(data));
        assert_eq!(reader.byte_headers()?, &brec!["name", "surname"]);
        assert_eq!(
            reader.read_byte_record()?.unwrap().to_byte_record(),
            brec!["name", "surname"]
        );

        // No headers, call after read
        let mut reader = ZeroCopyReader::from_reader_no_headers(Cursor::new(data));
        assert_eq!(
            reader.read_byte_record()?.unwrap().to_byte_record(),
            brec!["name", "surname"]
        );
        assert_eq!(reader.byte_headers()?, &brec!["name", "surname"]);

        // Headers, empty
        let mut reader = ZeroCopyReader::from_reader(Cursor::new(b""));
        assert_eq!(reader.byte_headers()?, &brec![]);
        assert!(reader.read_byte_record()?.is_none());

        // No headers, empty
        let mut reader = ZeroCopyReader::from_reader_no_headers(Cursor::new(b""));
        assert_eq!(reader.byte_headers()?, &brec![]);
        assert!(reader.read_byte_record()?.is_none());

        Ok(())
    }
}
