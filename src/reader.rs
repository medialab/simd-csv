use std::io::{BufRead, BufReader, Read};

use crate::core::{CoreReader, ReadResult};
use crate::error::{self, Error};
use crate::records::{ByteRecord, ByteRecordBuilder};
use crate::utils::trim_bom;

pub struct ReaderBuilder {
    delimiter: u8,
    quote: u8,
    buffer_capacity: Option<usize>,
    flexible: bool,
    has_headers: bool,
}

impl Default for ReaderBuilder {
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

impl ReaderBuilder {
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

    fn bufreader<R: Read>(&self, reader: R) -> BufReader<R> {
        match self.buffer_capacity {
            None => BufReader::new(reader),
            Some(capacity) => BufReader::with_capacity(capacity, reader),
        }
    }

    pub fn from_reader<R: Read>(&self, reader: R) -> Reader<R> {
        Reader {
            buffer: self.bufreader(reader),
            inner: CoreReader::new(self.delimiter, self.quote),
            flexible: self.flexible,
            headers: ByteRecord::new(),
            has_read: false,
            must_reemit_headers: !self.has_headers,
        }
    }
}

pub struct Reader<R> {
    buffer: BufReader<R>,
    inner: CoreReader,
    flexible: bool,
    headers: ByteRecord,
    has_read: bool,
    must_reemit_headers: bool,
}

impl<R: Read> Reader<R> {
    pub fn from_reader(reader: R) -> Self {
        ReaderBuilder::new().from_reader(reader)
    }

    #[inline]
    fn check_field_count(&mut self, written: usize) -> error::Result<()> {
        if self.flexible {
            return Ok(());
        }

        if self.has_read && written != self.headers.len() {
            return Err(Error::unequal_lengths(self.headers.len(), written));
        }

        Ok(())
    }

    fn read_byte_record_impl(&mut self, record: &mut ByteRecord) -> error::Result<bool> {
        use ReadResult::*;

        record.clear();

        let mut record_builder = ByteRecordBuilder::wrap(record);

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
        let mut headers = ByteRecord::new();

        let has_data = self.read_byte_record_impl(&mut headers)?;

        if !has_data {
            self.must_reemit_headers = false;
        }

        self.headers = headers;
        self.has_read = true;

        Ok(())
    }

    #[inline]
    pub fn byte_headers(&mut self) -> error::Result<&ByteRecord> {
        self.on_first_read()?;

        Ok(&self.headers)
    }

    #[inline(always)]
    pub fn read_byte_record(&mut self, record: &mut ByteRecord) -> error::Result<bool> {
        self.on_first_read()?;

        if self.must_reemit_headers {
            self.headers.clone_into(record);
            self.must_reemit_headers = false;
            return Ok(true);
        }

        self.read_byte_record_impl(record)
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

    pub fn get_ref(&self) -> &R {
        self.buffer.get_ref()
    }

    pub fn get_mut(&mut self) -> &mut R {
        self.buffer.get_mut()
    }

    pub fn into_inner(self) -> R {
        self.buffer.into_inner()
    }

    pub fn into_bufreader(self) -> BufReader<R> {
        self.buffer
    }
}

pub struct ByteRecordsIter<'r, R> {
    reader: &'r mut Reader<R>,
    record: ByteRecord,
}

impl<'r, R: Read> Iterator for ByteRecordsIter<'r, R> {
    type Item = error::Result<ByteRecord>;

    #[inline]
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
    reader: Reader<R>,
    record: ByteRecord,
}

impl<R: Read> Iterator for ByteRecordsIntoIter<R> {
    type Item = error::Result<ByteRecord>;

    #[inline]
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

    impl<R: Read> Reader<R> {
        fn from_reader_no_headers(reader: R) -> Self {
            ReaderBuilder::new().has_headers(false).from_reader(reader)
        }
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
            let mut reader = ReaderBuilder::with_capacity(capacity)
                .has_headers(false)
                .from_reader(Cursor::new(csv));

            assert_eq!(
                reader.byte_records().collect::<Result<Vec<_>, _>>()?,
                expected,
            );
        }

        Ok(())
    }

    #[test]
    fn test_strip_bom() -> error::Result<()> {
        let mut reader = Reader::from_reader_no_headers(Cursor::new("name,surname,age"));

        assert_eq!(
            reader.byte_records().next().unwrap()?,
            brec!["name", "surname", "age"]
        );

        let mut reader =
            Reader::from_reader_no_headers(Cursor::new(b"\xef\xbb\xbfname,surname,age"));

        assert_eq!(
            reader.byte_records().next().unwrap()?,
            brec!["name", "surname", "age"]
        );

        Ok(())
    }

    #[test]
    fn test_empty_row() -> error::Result<()> {
        let data = "name\n\"\"\nlucy\n\"\"";

        // Read
        let reader = Reader::from_reader_no_headers(Cursor::new(data));

        let expected = vec![brec!["name"], brec![""], brec!["lucy"], brec![""]];

        let records = reader.into_byte_records().collect::<Result<Vec<_>, _>>()?;

        assert_eq!(records, expected);

        Ok(())
    }

    #[test]
    fn test_crlf() -> error::Result<()> {
        let reader = Reader::from_reader_no_headers(Cursor::new(
            "name,surname\r\nlucy,\"john\"\r\nevan,zhong\r\nbéatrice,glougou\r\n",
        ));

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
        let reader = Reader::from_reader_no_headers(Cursor::new(
            "\"name\",\"surname\"\n\"lucy\",\"rose\"\n\"john\",\"mayhew\"",
        ));

        let expected = vec![
            brec!["name", "surname"],
            brec!["lucy", "rose"],
            brec!["john", "mayhew"],
        ];

        let records = reader.into_byte_records().collect::<Result<Vec<_>, _>>()?;

        assert_eq!(records, expected);

        Ok(())
    }

    #[test]
    fn test_byte_headers() -> error::Result<()> {
        let data = b"name,surname\njohn,dandy";

        // Headers, call before read
        let mut reader = Reader::from_reader(Cursor::new(data));
        assert_eq!(reader.byte_headers()?, &brec!["name", "surname"]);
        assert_eq!(
            reader.byte_records().next().unwrap()?,
            brec!["john", "dandy"]
        );

        // Headers, call after read
        let mut reader = Reader::from_reader(Cursor::new(data));
        assert_eq!(
            reader.byte_records().next().unwrap()?,
            brec!["john", "dandy"]
        );
        assert_eq!(reader.byte_headers()?, &brec!["name", "surname"]);

        // No headers, call before read
        let mut reader = Reader::from_reader_no_headers(Cursor::new(data));
        assert_eq!(reader.byte_headers()?, &brec!["name", "surname"]);
        assert_eq!(
            reader.byte_records().next().unwrap()?,
            brec!["name", "surname"]
        );

        // No headers, call after read
        let mut reader = Reader::from_reader_no_headers(Cursor::new(data));
        assert_eq!(
            reader.byte_records().next().unwrap()?,
            brec!["name", "surname"]
        );
        assert_eq!(reader.byte_headers()?, &brec!["name", "surname"]);

        // Headers, empty
        let mut reader = Reader::from_reader(Cursor::new(b""));
        assert_eq!(reader.byte_headers()?, &brec![]);
        assert!(reader.byte_records().next().is_none());

        // No headers, empty
        let mut reader = Reader::from_reader_no_headers(Cursor::new(b""));
        assert_eq!(reader.byte_headers()?, &brec![]);
        assert!(reader.byte_records().next().is_none());

        Ok(())
    }

    #[test]
    fn test_weirdness() -> error::Result<()> {
        // Data after quotes, before next delimiter
        let data =
            b"name,surname\n\"test\"  \"wat\", ok\ntest \"wat\",ok  \ntest,\"whatever\"  ok\n\"test\"   there,\"ok\"\r\n";
        let mut reader = Reader::from_reader_no_headers(Cursor::new(data));

        let records = reader.byte_records().collect::<Result<Vec<_>, _>>()?;

        let expected = vec![
            brec!["name", "surname"],
            brec!["test  \"wat", " ok"],
            brec!["test \"wat", "ok  "],
            brec!["test", "whatever  ok"],
            brec!["test   there", "ok"],
        ];

        assert_eq!(records, expected);

        // let data = "aaa\"aaa,bbb";
        // let mut reader = Reader::from_reader_no_headers(Cursor::new(data));
        // let record = reader.byte_records().next().unwrap().unwrap();

        // assert_eq!(record, brec!["aaa\"aaa", "bbb"]);

        let data = b"name,surname\n\r\rjohn,coucou";
        let mut reader = Reader::from_reader_no_headers(Cursor::new(data));
        let records = reader.byte_records().collect::<Result<Vec<_>, _>>()?;

        assert_eq!(
            records,
            vec![brec!["name", "surname"], brec!["john", "coucou"]]
        );

        Ok(())
    }
}
