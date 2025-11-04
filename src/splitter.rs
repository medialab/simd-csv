use std::io::{BufReader, Read};

use crate::buffer::ScratchBuffer;
use crate::core::{CoreReader, ReadResult};
use crate::error;
use crate::utils::{trim_bom, trim_trailing_crlf};

pub struct SplitterBuilder {
    delimiter: u8,
    quote: u8,
    buffer_capacity: Option<usize>,
    has_headers: bool,
}

impl Default for SplitterBuilder {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            buffer_capacity: None,
            has_headers: true,
        }
    }
}

impl SplitterBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut splitter = Self::default();
        splitter.buffer_capacity(capacity);
        splitter
    }

    pub fn delimiter(&mut self, delimiter: u8) -> &mut Self {
        self.delimiter = delimiter;
        self
    }

    pub fn quote(&mut self, quote: u8) -> &mut Self {
        self.quote = quote;
        self
    }

    pub fn has_headers(&mut self, yes: bool) -> &mut Self {
        self.has_headers = yes;
        self
    }

    pub fn buffer_capacity(&mut self, capacity: usize) -> &mut Self {
        self.buffer_capacity = Some(capacity);
        self
    }

    pub fn from_reader<R: Read>(&self, reader: R) -> Splitter<R> {
        Splitter {
            buffer: ScratchBuffer::with_optional_capacity(self.buffer_capacity, reader),
            inner: CoreReader::new(self.delimiter, self.quote),
            headers: Vec::new(),
            has_read: false,
            has_headers: self.has_headers,
            must_reemit_headers: !self.has_headers,
        }
    }
}

#[derive(Debug)]
pub struct Splitter<R> {
    buffer: ScratchBuffer<R>,
    inner: CoreReader,
    headers: Vec<u8>,
    has_read: bool,
    has_headers: bool,
    must_reemit_headers: bool,
}

impl<R: Read> Splitter<R> {
    pub fn from_reader(reader: R) -> Self {
        SplitterBuilder::new().from_reader(reader)
    }

    pub fn has_headers(&self) -> bool {
        self.has_headers
    }

    pub fn byte_headers(&mut self) -> error::Result<&[u8]> {
        self.on_first_read()?;

        Ok(&self.headers)
    }

    #[inline(always)]
    fn on_first_read(&mut self) -> error::Result<()> {
        if self.has_read {
            return Ok(());
        }

        let input = self.buffer.fill_buf()?;
        let bom_len = trim_bom(input);
        self.buffer.consume(bom_len);

        if let Some(record) = self.split_record_impl()? {
            self.headers = record.to_vec();
        } else {
            self.must_reemit_headers = false;
        }

        self.has_read = true;

        Ok(())
    }

    pub fn count_records(&mut self) -> error::Result<u64> {
        use ReadResult::*;

        self.on_first_read()?;
        self.buffer.reset();

        let mut count: u64 = 0;

        if self.must_reemit_headers {
            count += 1;
            self.must_reemit_headers = false;
        }

        loop {
            let input = self.buffer.fill_buf()?;

            let (result, pos) = self.inner.split_record(input);

            self.buffer.consume(pos);

            match result {
                End => break,
                InputEmpty | Cr | Lf => continue,
                Record => {
                    count += 1;
                }
            };
        }

        Ok(count)
    }

    pub fn split_record_impl(&mut self) -> error::Result<Option<&[u8]>> {
        use ReadResult::*;

        self.buffer.reset();

        loop {
            let input = self.buffer.fill_buf()?;

            let (result, pos) = self.inner.split_record(input);

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
                    return Ok(Some(trim_trailing_crlf(self.buffer.flush(pos))));
                }
            };
        }
    }

    pub fn split_record(&mut self) -> error::Result<Option<&[u8]>> {
        self.on_first_read()?;

        if self.must_reemit_headers {
            self.must_reemit_headers = false;
            return Ok(Some(&self.headers));
        }

        self.split_record_impl()
    }

    pub fn split_record_with_position(&mut self) -> error::Result<Option<(u64, &[u8])>> {
        self.on_first_read()?;

        let pos = self.position();

        if self.must_reemit_headers {
            self.must_reemit_headers = false;
            return Ok(Some((pos, &self.headers)));
        }

        match self.split_record_impl() {
            Ok(Some(record)) => Ok(Some((pos, record))),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn into_bufreader(self) -> BufReader<R> {
        self.buffer.into_bufreader()
    }

    #[inline(always)]
    pub fn position(&self) -> u64 {
        if self.must_reemit_headers {
            0
        } else {
            self.buffer.position()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn count_records(data: &str, capacity: usize) -> u64 {
        let mut splitter = SplitterBuilder::with_capacity(capacity)
            .has_headers(false)
            .from_reader(Cursor::new(data));
        splitter.count_records().unwrap()
    }

    fn split_records(data: &str, capacity: usize) -> u64 {
        let mut splitter = SplitterBuilder::with_capacity(capacity)
            .has_headers(false)
            .from_reader(Cursor::new(data));
        let mut count: u64 = 0;

        while let Some(_) = splitter.split_record().unwrap() {
            count += 1;
        }

        count
    }

    #[test]
    fn test_count() {
        // Empty
        assert_eq!(count_records("", 1024), 0);

        // Single cells with various empty lines
        let tests = vec![
            "name\njohn\nlucy",
            "name\njohn\nlucy\n",
            "name\n\njohn\r\nlucy\n",
            "name\n\njohn\r\nlucy\n\n",
            "name\n\n\njohn\r\n\r\nlucy\n\n\n",
            "\nname\njohn\nlucy",
            "\n\nname\njohn\nlucy",
            "\r\n\r\nname\njohn\nlucy",
            "name\njohn\nlucy\r\n",
            "name\njohn\nlucy\r\n\r\n",
        ];

        for capacity in [32usize, 4, 3, 2, 1] {
            for test in tests.iter() {
                assert_eq!(
                    count_records(test, capacity),
                    3,
                    "capacity={} string={:?}",
                    capacity,
                    test
                );
            }
        }

        // Multiple cells
        let data = "name,surname,age\njohn,landy,45\nlucy,rose,67";
        assert_eq!(count_records(data, 1024), 3);
        assert_eq!(split_records(data, 1024), 3);

        // Quoting
        for capacity in [1024usize, 32usize, 4, 3, 2, 1] {
            let data = "name,surname,age\n\"john\",\"landy, the \"\"everlasting\"\" bastard\",45\nlucy,rose,\"67\"\njermaine,jackson,\"89\"\n\nkarine,loucan,\"52\"\r\n";

            assert_eq!(count_records(data, capacity), 5, "capacity={}", capacity);
            assert_eq!(split_records(data, capacity), 5, "capacity={}", capacity);
        }

        // Different separator
        let data = "name\tsurname\tage\njohn\tlandy\t45\nlucy\trose\t67";
        assert_eq!(count_records(data, 1024), 3);
        assert_eq!(split_records(data, 1024), 3);
    }

    #[test]
    fn test_empty_row() -> error::Result<()> {
        let data = "name\n\"\"\nlucy\n\"\"";

        // Counting
        let mut reader = Splitter::from_reader(Cursor::new(data));

        assert_eq!(reader.count_records()?, 3);

        Ok(())
    }
}
