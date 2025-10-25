use crate::core::{CoreReader, ReadResult};
use crate::records::{ByteRecord, ByteRecordBuilder};
use crate::utils::trim_bom;

pub struct TotalReaderBuilder {
    delimiter: u8,
    quote: u8,
    has_headers: bool,
}

impl Default for TotalReaderBuilder {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            has_headers: true,
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

    pub fn has_headers(&mut self, yes: bool) -> &mut Self {
        self.has_headers = yes;
        self
    }

    pub fn from_bytes<'b>(&self, bytes: &'b [u8]) -> TotalReader<'b> {
        TotalReader {
            inner: CoreReader::new(self.delimiter, self.quote, None),
            bytes,
            pos: 0,
            headers: ByteRecord::new(),
            has_read: false,
            has_headers: self.has_headers,
        }
    }
}

// NOTE: a reader to be used when the whole data fits into memory or when using
// memory maps.
pub struct TotalReader<'b> {
    inner: CoreReader,
    bytes: &'b [u8],
    pos: usize,
    headers: ByteRecord,
    has_read: bool,
    has_headers: bool,
}

impl<'b> TotalReader<'b> {
    pub fn from_bytes(bytes: &'b [u8]) -> Self {
        TotalReaderBuilder::new().from_bytes(bytes)
    }

    #[inline]
    fn on_first_read(&mut self) {
        if self.has_read {
            return;
        }

        // Trimming BOM
        let bom_len = trim_bom(self.bytes);
        self.pos += bom_len;

        // Reading headers
        let mut headers = ByteRecord::new();

        let has_data = self.read_byte_record_impl(&mut headers);

        if has_data && !self.has_headers {
            self.pos = bom_len;
        }

        self.headers = headers;
        self.has_read = true;
    }

    #[inline]
    pub fn byte_headers(&mut self) -> &ByteRecord {
        self.on_first_read();

        &self.headers
    }

    pub fn count_records(&mut self) -> u64 {
        use ReadResult::*;

        self.on_first_read();

        let mut count: u64 = 0;

        loop {
            let (result, pos) = self.inner.split_record(&self.bytes[self.pos..]);

            self.pos += pos;

            match result {
                End => break,
                InputEmpty | Skip => continue,
                Record => {
                    count += 1;
                }
            };
        }

        count.saturating_sub(if self.has_headers { 1 } else { 0 })
    }

    pub fn split_record(&mut self) -> Option<&[u8]> {
        use ReadResult::*;

        self.on_first_read();

        let starting_pos = self.pos;

        loop {
            let (result, pos) = self.inner.split_record(&self.bytes[self.pos..]);

            self.pos += pos;

            match result {
                End => return None,
                InputEmpty | Skip => continue,
                Record => return Some(&self.bytes[starting_pos..self.pos]),
            }
        }
    }

    fn read_byte_record_impl(&mut self, record: &mut ByteRecord) -> bool {
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
                    return false;
                }
                InputEmpty | Skip => {
                    continue;
                }
                Record => {
                    return true;
                }
            };
        }
    }

    #[inline(always)]
    pub fn read_byte_record(&mut self, record: &mut ByteRecord) -> bool {
        self.on_first_read();
        self.read_byte_record_impl(record)
    }

    #[inline(always)]
    pub fn byte_records<'r>(&'r mut self) -> ByteRecordsIter<'r, 'b> {
        ByteRecordsIter {
            reader: self,
            record: ByteRecord::new(),
        }
    }
}

pub struct ByteRecordsIter<'r, 'b> {
    reader: &'r mut TotalReader<'b>,
    record: ByteRecord,
}

impl Iterator for ByteRecordsIter<'_, '_> {
    type Item = ByteRecord;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // NOTE: cloning the record will not carry over excess capacity
        // because the record only contains `Vec` currently.
        if self.reader.read_byte_record(&mut self.record) {
            Some(self.record.clone())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::brec;

    impl<'b> TotalReader<'b> {
        fn from_bytes_no_headers(bytes: &'b [u8]) -> Self {
            TotalReaderBuilder::new()
                .has_headers(false)
                .from_bytes(bytes)
        }
    }

    fn count_records(data: &str) -> u64 {
        let mut reader = TotalReader::from_bytes_no_headers(data.as_bytes());
        reader.count_records()
    }

    fn split_records(data: &str) -> u64 {
        let mut reader = TotalReader::from_bytes_no_headers(data.as_bytes());

        let mut count: u64 = 0;

        while reader.split_record().is_some() {
            count += 1;
        }

        count
    }

    #[test]
    fn test_count() {
        // Empty
        assert_eq!(count_records(""), 0);

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

        for test in tests.iter() {
            assert_eq!(count_records(test), 3, "string={:?}", test);
            assert_eq!(split_records(test), 3, "string={:?}", test);
        }
    }

    #[test]
    fn test_byte_headers() {
        let data = b"name,surname\njohn,dandy";

        // Headers, call before read
        let mut reader = TotalReader::from_bytes(data);
        assert_eq!(reader.byte_headers(), &brec!["name", "surname"]);
        assert_eq!(
            reader.byte_records().next().unwrap(),
            brec!["john", "dandy"]
        );

        // Headers, call after read
        let mut reader = TotalReader::from_bytes(data);
        assert_eq!(
            reader.byte_records().next().unwrap(),
            brec!["john", "dandy"]
        );
        assert_eq!(reader.byte_headers(), &brec!["name", "surname"]);

        // No headers, call before read
        let mut reader = TotalReader::from_bytes_no_headers(data);
        assert_eq!(reader.byte_headers(), &brec!["name", "surname"]);
        assert_eq!(
            reader.byte_records().next().unwrap(),
            brec!["name", "surname"]
        );

        // No headers, call after read
        let mut reader = TotalReader::from_bytes_no_headers(data);
        assert_eq!(
            reader.byte_records().next().unwrap(),
            brec!["name", "surname"]
        );
        assert_eq!(reader.byte_headers(), &brec!["name", "surname"]);

        // Headers, empty
        let mut reader = TotalReader::from_bytes(b"");
        assert_eq!(reader.byte_headers(), &brec![]);
        assert!(reader.byte_records().next().is_none());

        // No headers, empty
        let mut reader = TotalReader::from_bytes_no_headers(b"");
        assert_eq!(reader.byte_headers(), &brec![]);
        assert!(reader.byte_records().next().is_none());
    }
}
