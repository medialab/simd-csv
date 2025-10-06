use std::io::{BufRead, BufReader, Read};

use crate::core::{self, ReadResult};
use crate::error::{self, Error};
use crate::ext::StripBom;
use crate::records::{ByteRecord, ByteRecordBuilder, ZeroCopyByteRecord};

pub struct BufferedReader<R> {
    buffer: BufReader<R>,
    scratch: Vec<u8>,
    seps: Vec<usize>,
    actual_buffer_position: Option<usize>,
    inner: core::CoreReader,
    field_count: Option<usize>,
}

impl<R: Read> BufferedReader<R> {
    pub fn new(reader: R, delimiter: u8, quote: u8) -> Self {
        Self {
            buffer: BufReader::new(reader),
            scratch: Vec::new(),
            seps: Vec::new(),
            actual_buffer_position: None,
            inner: core::CoreReader::new(delimiter, quote),
            field_count: None,
        }
    }

    pub fn with_capacity(capacity: usize, reader: R, delimiter: u8, quote: u8) -> Self {
        Self {
            buffer: BufReader::with_capacity(capacity, reader),
            scratch: Vec::new(),
            seps: Vec::new(),
            actual_buffer_position: None,
            inner: core::CoreReader::new(delimiter, quote),
            field_count: None,
        }
    }

    #[inline]
    fn check_field_count(&mut self, written: usize) -> error::Result<()> {
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

    pub fn first_byte_record(&mut self, consume: bool) -> error::Result<ByteRecord> {
        use ReadResult::*;

        let mut record = ByteRecord::new();
        let mut record_builder = ByteRecordBuilder::wrap(&mut record);

        let input = self.buffer.fill_buf()?;

        let (result, pos) = self.inner.read_record(input, &mut record_builder);

        match result {
            End => Ok(ByteRecord::new()),

            // TODO: we could expand the capacity of the buffer automagically here
            // if this becomes an issue.
            Cr | Lf | ReadResult::InputEmpty => Err(Error::invalid_headers()),
            Record => {
                if consume {
                    self.buffer.consume(pos);
                }

                Ok(record)
            }
        }
    }

    pub fn read_zero_copy_byte_record(&mut self) -> error::Result<Option<ZeroCopyByteRecord<'_>>> {
        use ReadResult::*;

        self.scratch.clear();
        self.seps.clear();

        if let Some(last_pos) = self.actual_buffer_position.take() {
            self.buffer.consume(last_pos);
        }

        loop {
            let input = self.buffer.fill_buf()?;

            let (result, pos) = self.inner.split_record_and_find_separators(
                input,
                self.scratch.len(),
                &mut self.seps,
            );

            match result {
                End => {
                    self.buffer.consume(pos);
                    return Ok(None);
                }
                Cr | Lf => {
                    self.buffer.consume(pos);
                }
                InputEmpty => {
                    self.scratch.extend_from_slice(input);
                    self.buffer.consume(pos);
                }
                Record => {
                    if self.scratch.is_empty() {
                        self.check_field_count(self.seps.len() + 1)?;
                        self.actual_buffer_position = Some(pos);
                        return Ok(Some(ZeroCopyByteRecord::new(
                            &self.buffer.buffer()[..pos],
                            &self.seps,
                        )));
                    } else {
                        self.scratch.extend_from_slice(&input[..pos]);
                        self.buffer.consume(pos);
                        self.check_field_count(self.seps.len() + 1)?;
                        return Ok(Some(ZeroCopyByteRecord::new(&self.scratch, &self.seps)));
                    }
                }
            };
        }
    }

    pub fn read_byte_record(&mut self, record: &mut ByteRecord) -> error::Result<bool> {
        use ReadResult::*;

        record.clear();

        let mut record_builder = ByteRecordBuilder::wrap(record);

        if let Some(last_pos) = self.actual_buffer_position.take() {
            self.buffer.consume(last_pos);
        }

        loop {
            let input = self.buffer.fill_buf()?;

            let (result, pos) = self.inner.read_record(input, &mut record_builder);

            self.buffer.consume(pos);

            match result {
                End => {
                    return Ok(false);
                }
                Cr | Lf | InputEmpty => {
                    continue;
                }
                Record => {
                    self.check_field_count(record.len())?;
                    return Ok(true);
                }
            };
        }
    }

    pub fn byte_records(&mut self) -> ByteRecordsIter<'_, R> {
        ByteRecordsIter {
            reader: self,
            record: ByteRecord::new(),
        }
    }

    pub fn into_byte_records(self) -> ByteRecordsIntoIter<R> {
        ByteRecordsIntoIter {
            reader: self,
            record: ByteRecord::new(),
        }
    }
}

pub struct ByteRecordsIter<'r, R> {
    reader: &'r mut BufferedReader<R>,
    record: ByteRecord,
}

impl<'r, R: Read> Iterator for ByteRecordsIter<'r, R> {
    type Item = error::Result<ByteRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        // NOTE: cloning the record will not carry over excess capacity
        // because the record only contains `Vec` currently.
        match self.reader.read_byte_record(&mut self.record) {
            Err(err) => Some(Err(err)),
            Ok(true) => Some(Ok(self.record.clone())),
            Ok(false) => None,
        }
    }
}

pub struct ByteRecordsIntoIter<R> {
    reader: BufferedReader<R>,
    record: ByteRecord,
}

impl<R: Read> Iterator for ByteRecordsIntoIter<R> {
    type Item = error::Result<ByteRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        // NOTE: cloning the record will not carry over excess capacity
        // because the record only contains `Vec` currently.
        match self.reader.read_byte_record(&mut self.record) {
            Err(err) => Some(Err(err)),
            Ok(true) => Some(Ok(self.record.clone())),
            Ok(false) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::brec;

    use super::*;

    #[test]
    fn test_read_zero_copy_byte_record() -> error::Result<()> {
        let csv = "name,surname,age\n\"john\",\"landy, the \"\"everlasting\"\" bastard\",45\nlucy,rose,\"67\"\njermaine,jackson,\"89\"\n\nkarine,loucan,\"52\"\nrose,\"glib\",12\n\"guillaume\",\"plique\",\"42\"\r\n";

        let mut reader = BufferedReader::with_capacity(32, Cursor::new(csv), b',', b'"');
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

        while let Some(record) = reader.read_zero_copy_byte_record()? {
            records.push(record.iter().map(|cell| cell.to_vec()).collect::<Vec<_>>());
        }

        assert_eq!(records, expected);

        Ok(())
    }

    #[test]
    fn test_read_byte_record() -> error::Result<()> {
        let csv = "name,surname,age\n\"john\",\"landy, the \"\"everlasting\"\" bastard\",45\n\"\"\"ok\"\"\",whatever,dude\nlucy,rose,\"67\"\njermaine,jackson,\"89\"\n\nkarine,loucan,\"52\"\nrose,\"glib\",12\n\"guillaume\",\"plique\",\"42\"\r\n";

        let expected = vec![
            brec!["name", "surname", "age"],
            brec!["john", "landy, the \"everlasting\" bastard", "45"],
            brec!["\"ok\"", "whatever", "dude"],
            brec!["lucy", "rose", "67"],
            brec!["jermaine", "jackson", "89"],
            brec!["karine", "loucan", "52"],
            brec!["rose", "glib", "12"],
            brec!["guillaume", "plique", "42"],
        ];

        for capacity in [32usize, 4, 3, 2, 1] {
            let mut reader = BufferedReader::with_capacity(capacity, Cursor::new(csv), b',', b'"');

            assert_eq!(
                reader.byte_records().collect::<Result<Vec<_>, _>>()?,
                expected
            );
        }

        Ok(())
    }

    #[test]
    fn test_strip_bom() -> error::Result<()> {
        let mut reader = BufferedReader::new(Cursor::new("name,surname,age"), b',', b'"');
        reader.strip_bom()?;

        assert_eq!(
            reader.byte_records().next().unwrap()?,
            brec!["name", "surname", "age"]
        );

        let mut reader =
            BufferedReader::new(Cursor::new(b"\xef\xbb\xbfname,surname,age"), b',', b'"');
        reader.strip_bom()?;

        assert_eq!(
            reader.byte_records().next().unwrap()?,
            brec!["name", "surname", "age"]
        );

        Ok(())
    }

    #[test]
    fn test_empty_row() -> error::Result<()> {
        let data = "name\n\"\"\nlucy\n\"\"";

        // Zero-copy
        let mut reader = BufferedReader::new(Cursor::new(data), b',', b'"');

        let expected = vec![
            vec!["name".as_bytes().to_vec()],
            vec!["\"\"".as_bytes().to_vec()],
            vec!["lucy".as_bytes().to_vec()],
            vec!["\"\"".as_bytes().to_vec()],
        ];

        // Read
        let mut records = Vec::new();

        while let Some(record) = reader.read_zero_copy_byte_record()? {
            records.push(vec![record.as_slice().to_vec()]);
        }

        assert_eq!(records, expected);

        let reader = BufferedReader::new(Cursor::new(data), b',', b'"');

        let expected = vec![brec!["name"], brec![""], brec!["lucy"], brec![""]];

        let records = reader.into_byte_records().collect::<Result<Vec<_>, _>>()?;

        assert_eq!(records, expected);

        Ok(())
    }

    #[test]
    fn test_crlf() -> error::Result<()> {
        let reader = BufferedReader::new(
            Cursor::new("name,surname\r\nlucy,\"john\"\r\nevan,zhong\r\nbéatrice,glougou\r\n"),
            b',',
            b'"',
        );

        let expected = vec![
            brec!["name", "surname"],
            brec!["lucy", "john"],
            brec!["evan", "zhong"],
            brec!["béatrice", "glougou"],
        ];

        let records = reader.into_byte_records().collect::<Result<Vec<_>, _>>()?;

        assert_eq!(records, expected);

        Ok(())
    }

    #[test]
    fn test_quote_always() -> error::Result<()> {
        let reader = BufferedReader::new(
            Cursor::new("\"name\",\"surname\"\n\"lucy\",\"rose\"\n\"john\",\"mayhew\""),
            b',',
            b'"',
        );

        let expected = vec![
            brec!["name", "surname"],
            brec!["lucy", "rose"],
            brec!["john", "mayhew"],
        ];

        let records = reader.into_byte_records().collect::<Result<Vec<_>, _>>()?;

        assert_eq!(records, expected);

        Ok(())
    }
}
