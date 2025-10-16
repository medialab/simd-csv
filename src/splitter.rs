use std::io::Read;

use crate::buffer::ScratchBuffer;
use crate::core::{CoreReader, ReadResult};
use crate::error;
use crate::utils::{trim_bom, trim_trailing_crlf};

pub struct SplitterBuilder {
    delimiter: u8,
    quote: u8,
    buffer_capacity: Option<usize>,
}

impl Default for SplitterBuilder {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            buffer_capacity: None,
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

    pub fn buffer_capacity(&mut self, capacity: usize) -> &mut Self {
        self.buffer_capacity = Some(capacity);
        self
    }

    pub fn from_reader<R: Read>(&self, reader: R) -> Splitter<R> {
        Splitter {
            buffer: ScratchBuffer::with_optional_capacity(self.buffer_capacity, reader),
            inner: CoreReader::new(self.delimiter, self.quote),
            has_read: false,
        }
    }
}

pub struct Splitter<R> {
    buffer: ScratchBuffer<R>,
    inner: CoreReader,
    has_read: bool,
}

impl<R: Read> Splitter<R> {
    pub fn from_reader(reader: R) -> Self {
        SplitterBuilder::new().from_reader(reader)
    }

    #[inline(always)]
    fn on_first_read(&mut self) -> error::Result<()> {
        if self.has_read {
            return Ok(());
        }

        let input = self.buffer.fill_buf()?;
        let bom_len = trim_bom(input);
        self.buffer.consume(bom_len);
        self.has_read = true;

        Ok(())
    }

    pub fn count_records(&mut self) -> error::Result<u64> {
        use ReadResult::*;

        self.on_first_read()?;

        let mut count: u64 = 0;

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

    pub fn split_record(&mut self) -> error::Result<Option<&[u8]>> {
        use ReadResult::*;

        self.on_first_read()?;

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
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn count_records(data: &str, capacity: usize) -> u64 {
        let mut splitter = SplitterBuilder::with_capacity(capacity).from_reader(Cursor::new(data));
        splitter.count_records().unwrap()
    }

    fn split_records(data: &str, capacity: usize) -> u64 {
        let mut splitter = SplitterBuilder::with_capacity(capacity).from_reader(Cursor::new(data));
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

        assert_eq!(reader.count_records()?, 4);

        Ok(())
    }
}
