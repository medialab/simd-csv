use std::io::{self, BufRead, BufReader, Read};

use memchr::{memchr, memchr2};

use crate::records::{ByteRecord, ZeroCopyByteRecord};
use crate::searcher::Searcher;

#[derive(Debug)]
enum ReadResult {
    InputEmpty,
    Cr,
    Lf,
    Record,
    End,
}

#[derive(Debug)]
enum ReadState {
    Unquoted,
    Quoted,
    Quote,
}

// NOTE: funnily enough, knowing the delimiter is not required to split the records,
// but since we expose a single unified `struct` here, it is simpler to include it.
struct Reader {
    delimiter: u8,
    quote: u8,
    state: ReadState,
    record_was_read: bool,
    searcher: Searcher,
}

impl Reader {
    fn new(delimiter: u8, quote: u8) -> Self {
        Self {
            delimiter,
            quote,
            state: ReadState::Unquoted,
            // Must be true at the beginning to avoid counting one record for empty input
            record_was_read: true,
            searcher: Searcher::new(delimiter, b'\n', quote),
        }
    }

    fn split_record(&mut self, input: &[u8]) -> (ReadResult, usize) {
        use ReadState::*;

        if input.is_empty() {
            if !self.record_was_read {
                self.record_was_read = true;
                return (ReadResult::Record, 0);
            }

            return (ReadResult::End, 0);
        }

        if self.record_was_read {
            if input[0] == b'\n' {
                return (ReadResult::Lf, 1);
            } else if input[0] == b'\r' {
                return (ReadResult::Cr, 1);
            }
        }

        self.record_was_read = false;

        let mut pos: usize = 0;

        while pos < input.len() {
            match self.state {
                Unquoted => {
                    // Here we are moving to next quote or end of line
                    if let Some(offset) = memchr2(b'\n', self.quote, &input[pos..]) {
                        pos += offset;

                        let byte = input[pos];

                        pos += 1;

                        if byte == b'\n' {
                            self.record_was_read = true;
                            return (ReadResult::Record, pos);
                        }

                        // Here, `byte` is guaranteed to be a quote
                        self.state = Quoted;
                    } else {
                        break;
                    }
                }
                Quoted => {
                    // Here we moving to next quote
                    if let Some(offset) = memchr(self.quote, &input[pos..]) {
                        pos += offset + 1;
                        self.state = Quote;
                    } else {
                        break;
                    }
                }
                Quote => {
                    let byte = input[pos];

                    pos += 1;

                    if byte == self.quote {
                        self.state = Quoted;
                    } else if byte == b'\n' {
                        self.record_was_read = true;
                        self.state = Unquoted;
                        return (ReadResult::Record, pos);
                    } else {
                        self.state = Unquoted;
                    }
                }
            }
        }

        (ReadResult::InputEmpty, input.len())
    }

    fn split_record_and_find_separators(
        &mut self,
        input: &[u8],
        seps_offset: usize,
        seps: &mut Vec<usize>,
    ) -> (ReadResult, usize) {
        use ReadState::*;

        if input.is_empty() {
            if !self.record_was_read {
                self.record_was_read = true;
                return (ReadResult::Record, 0);
            }

            return (ReadResult::End, 0);
        }

        if self.record_was_read {
            if input[0] == b'\n' {
                return (ReadResult::Lf, 1);
            } else if input[0] == b'\r' {
                return (ReadResult::Cr, 1);
            }
        }

        self.record_was_read = false;

        let mut pos: usize = 0;

        while pos < input.len() {
            match self.state {
                Unquoted => {
                    // Here we are moving to next quote or end of line
                    let mut last_offset: Option<usize> = None;

                    for offset in self.searcher.search(&input[pos..]) {
                        last_offset = Some(offset);

                        let byte = input[pos + offset];

                        if byte == self.delimiter {
                            seps.push(seps_offset + pos + offset);
                            continue;
                        }

                        if byte == b'\n' {
                            self.record_was_read = true;
                            return (ReadResult::Record, pos + offset + 1);
                        }

                        // Here, `byte` is guaranteed to be a quote
                        self.state = Quoted;
                        break;
                    }

                    if let Some(offset) = last_offset {
                        pos += offset + 1;
                    } else {
                        break;
                    }
                }
                Quoted => {
                    // Here we moving to next quote
                    if let Some(offset) = memchr(self.quote, &input[pos..]) {
                        pos += offset + 1;
                        self.state = Quote;
                    } else {
                        break;
                    }
                }
                Quote => {
                    let byte = input[pos];

                    pos += 1;

                    if byte == self.quote {
                        self.state = Quoted;
                    } else if byte == self.delimiter {
                        seps.push(seps_offset + pos - 1);
                        self.state = Unquoted;
                    } else if byte == b'\n' {
                        self.record_was_read = true;
                        self.state = Unquoted;
                        return (ReadResult::Record, pos);
                    } else {
                        self.state = Unquoted;
                    }
                }
            }
        }

        (ReadResult::InputEmpty, input.len())
    }

    fn read_record(&mut self, input: &[u8], record: &mut ByteRecord) -> (ReadResult, usize) {
        use ReadState::*;

        if input.is_empty() {
            if !self.record_was_read {
                self.record_was_read = true;
                record.finalize_field();
                return (ReadResult::Record, 0);
            }

            return (ReadResult::End, 0);
        }

        if self.record_was_read {
            if input[0] == b'\n' {
                return (ReadResult::Lf, 1);
            } else if input[0] == b'\r' {
                return (ReadResult::Cr, 1);
            }
        }

        self.record_was_read = false;

        let mut pos: usize = 0;

        while pos < input.len() {
            match self.state {
                Unquoted => {
                    // Here we are moving to next quote or end of line
                    let mut last_offset: Option<usize> = None;

                    for offset in self.searcher.search(&input[pos..]) {
                        if let Some(o) = last_offset {
                            record.extend_from_slice(&input[pos + o + 1..pos + offset]);
                        } else {
                            record.extend_from_slice(&input[pos..pos + offset]);
                        }

                        last_offset = Some(offset);

                        let byte = input[pos + offset];

                        if byte == self.delimiter {
                            record.finalize_field();
                            continue;
                        }

                        if byte == b'\n' {
                            record.finalize_field();
                            self.record_was_read = true;
                            return (ReadResult::Record, pos + offset + 1);
                        }

                        // Here, `byte` is guaranteed to be a quote
                        self.state = Quoted;
                        break;
                    }

                    if let Some(offset) = last_offset {
                        pos += offset + 1;
                    } else {
                        break;
                    }
                }
                Quoted => {
                    // Here we moving to next quote
                    if let Some(offset) = memchr(self.quote, &input[pos..]) {
                        record.extend_from_slice(&input[pos..pos + offset]);
                        pos += offset + 1;
                        self.state = Quote;
                    } else {
                        break;
                    }
                }
                Quote => {
                    let byte = input[pos];

                    pos += 1;

                    if byte == self.quote {
                        self.state = Quoted;
                        record.push_byte(byte);
                    } else if byte == self.delimiter {
                        record.finalize_field();
                        self.state = Unquoted;
                    } else if byte == b'\n' {
                        self.record_was_read = true;
                        self.state = Unquoted;
                        record.finalize_field();
                        return (ReadResult::Record, pos);
                    } else {
                        self.state = Unquoted;
                    }
                }
            }
        }

        record.extend_from_slice(&input[pos..]);

        (ReadResult::InputEmpty, input.len())
    }

    // NOTE: this version of the method wraps the state machine logic within the
    // SIMD iteration logic. Ironically it seems slower than the multiple-speed
    // stop-and-go implementation above.
    // Be advised that this code does not handle final \r correctly yet.
    // fn read_record(&mut self, input: &[u8], record: &mut ByteRecord) -> (ReadResult, usize) {
    //     use ReadState::*;

    //     if input.is_empty() {
    //         if !self.record_was_read {
    //             self.record_was_read = true;
    //             record.finalize_field();
    //             return (ReadResult::Record, 0);
    //         }

    //         return (ReadResult::End, 0);
    //     }

    //     if self.record_was_read {
    //         if input[0] == b'\n' {
    //             return (ReadResult::Lf, 1);
    //         } else if input[0] == b'\r' {
    //             return (ReadResult::Cr, 1);
    //         }
    //     }

    //     self.record_was_read = false;

    //     let mut last_offset: Option<usize> = None;
    //     let mut start: usize;

    //     for offset in self.searcher.search(input) {
    //         let byte = input[offset];

    //         if let Quote = self.state {
    //             if byte == self.quote {
    //                 let was_previously_a_byte = match last_offset {
    //                     None => true,
    //                     Some(o) => o == offset - 1,
    //                 };

    //                 if was_previously_a_byte {
    //                     self.state = Quoted;
    //                     continue;
    //                 } else {
    //                     self.state = Unquoted;
    //                 }
    //             } else {
    //                 self.state = Unquoted;
    //             }
    //         }

    //         start = last_offset.map(|o| o + 1).unwrap_or(0);
    //         last_offset = Some(offset);

    //         match self.state {
    //             Unquoted => {
    //                 record.extend_from_slice(&input[start..offset]);

    //                 last_offset = Some(offset);

    //                 if byte == self.delimiter {
    //                     record.finalize_field();
    //                     continue;
    //                 }

    //                 if byte == b'\n' {
    //                     record.finalize_field();
    //                     self.record_was_read = true;
    //                     return (ReadResult::Record, offset + 1);
    //                 }

    //                 // Here, `byte` is guaranteed to be a quote
    //                 self.state = Quoted;
    //             }
    //             Quoted => {
    //                 record.extend_from_slice(&input[start..offset]);

    //                 if byte != self.quote {
    //                     record.push_byte(byte);
    //                     continue;
    //                 }

    //                 self.state = Quote;
    //             }
    //             _ => unreachable!(),
    //         }
    //     }

    //     start = last_offset.map(|o| o + 1).unwrap_or(0);
    //     record.extend_from_slice(&input[start..]);

    //     (ReadResult::InputEmpty, input.len())
    // }
}

pub struct BufferedReader<R> {
    buffer: BufReader<R>,
    scratch: Vec<u8>,
    seps: Vec<usize>,
    actual_buffer_position: Option<usize>,
    inner: Reader,
}

impl<R: Read> BufferedReader<R> {
    pub fn new(reader: R, delimiter: u8, quote: u8) -> Self {
        Self {
            buffer: BufReader::new(reader),
            scratch: Vec::new(),
            seps: Vec::new(),
            actual_buffer_position: None,
            inner: Reader::new(delimiter, quote),
        }
    }

    pub fn with_capacity(reader: R, capacity: usize, delimiter: u8, quote: u8) -> Self {
        Self {
            buffer: BufReader::with_capacity(capacity, reader),
            scratch: Vec::new(),
            seps: Vec::new(),
            actual_buffer_position: None,
            inner: Reader::new(delimiter, quote),
        }
    }

    pub fn strip_bom(&mut self) -> io::Result<()> {
        let input = self.buffer.fill_buf()?;

        if input.len() >= 3 && &input[..3] == b"\xef\xbb\xbf" {
            self.buffer.consume(3);
        }

        Ok(())
    }

    pub fn first_byte_record(&mut self, consume: bool) -> io::Result<ByteRecord> {
        use ReadResult::*;

        let mut record = ByteRecord::new();

        let input = self.buffer.fill_buf()?;

        let (result, pos) = self.inner.read_record(input, &mut record);

        match result {
            End => Ok(ByteRecord::new()),

            // TODO: we could expand the capacity of the buffer automagically here
            // if this becomes an issue.
            Cr | Lf | ReadResult::InputEmpty => Err(io::Error::other(
                "invalid headers or headers too long for buffer",
            )),
            Record => {
                if consume {
                    self.buffer.consume(pos);
                }

                Ok(record)
            }
        }
    }

    pub fn count_records(&mut self) -> io::Result<u64> {
        use ReadResult::*;

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

    pub fn split_record(&mut self) -> io::Result<Option<&[u8]>> {
        use ReadResult::*;

        self.scratch.clear();

        if let Some(last_pos) = self.actual_buffer_position.take() {
            self.buffer.consume(last_pos);
        }

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
                    self.scratch.extend(&input[..pos]);
                    self.buffer.consume(pos);
                }
                Record => {
                    if self.scratch.is_empty() {
                        self.actual_buffer_position = Some(pos);
                        return Ok(Some(&self.buffer.buffer()[..pos]));
                    } else {
                        self.scratch.extend(&input[..pos]);
                        self.buffer.consume(pos);

                        return Ok(Some(&self.scratch));
                    }
                }
            };
        }
    }

    pub fn read_zero_copy_byte_record(&mut self) -> io::Result<Option<ZeroCopyByteRecord<'_>>> {
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
                    self.scratch.extend(&input[..pos]);
                    self.buffer.consume(pos);
                }
                Record => {
                    if self.scratch.is_empty() {
                        self.actual_buffer_position = Some(pos);
                        return Ok(Some(ZeroCopyByteRecord::new(
                            &self.buffer.buffer()[..pos],
                            &self.seps,
                        )));
                    } else {
                        self.scratch.extend(&input[..pos]);
                        self.buffer.consume(pos);

                        return Ok(Some(ZeroCopyByteRecord::new(&self.scratch, &self.seps)));
                    }
                }
            };
        }
    }

    pub fn read_byte_record(&mut self, record: &mut ByteRecord) -> io::Result<bool> {
        use ReadResult::*;

        record.clear();

        if let Some(last_pos) = self.actual_buffer_position.take() {
            self.buffer.consume(last_pos);
        }

        loop {
            let input = self.buffer.fill_buf()?;

            let (result, pos) = self.inner.read_record(input, record);

            self.buffer.consume(pos);

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
    type Item = io::Result<ByteRecord>;

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
    type Item = io::Result<ByteRecord>;

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

// NOTE: a reader to be used when the whole data fits into memory or when using
// memory maps.
pub struct TotalReader {
    inner: Reader,
}

impl TotalReader {
    pub fn new(delimiter: u8, quote: u8) -> Self {
        Self {
            inner: Reader::new(delimiter, quote),
        }
    }

    pub fn count_records(&mut self, bytes: &[u8]) -> u64 {
        use ReadResult::*;

        let mut i: usize = 0;
        let mut count: u64 = 0;

        loop {
            let (result, pos) = self.inner.split_record(&bytes[i..]);

            i += pos;

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
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::brec;

    use super::*;

    fn count_records(data: &str, capacity: usize) -> u64 {
        let mut splitter = BufferedReader::with_capacity(Cursor::new(data), capacity, b',', b'"');
        splitter.count_records().unwrap()
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

        // Quoting
        for capacity in [1024usize, 32usize, 4, 3, 2, 1] {
            let data = "name,surname,age\n\"john\",\"landy, the \"\"everlasting\"\" bastard\",45\nlucy,rose,\"67\"\njermaine,jackson,\"89\"\n\nkarine,loucan,\"52\"\r\n";

            assert_eq!(count_records(data, capacity), 5, "capacity={}", capacity);
        }

        // Different separator
        let data = "name\tsurname\tage\njohn\tlandy\t45\nlucy\trose\t67";
        assert_eq!(count_records(data, 1024), 3);
    }

    #[test]
    fn test_read_zero_copy_byte_record() -> io::Result<()> {
        let csv = "name,surname,age\n\"john\",\"landy, the \"\"everlasting\"\" bastard\",45\nlucy,rose,\"67\"\njermaine,jackson,\"89\"\n\nkarine,loucan,\"52\"\nrose,\"glib\",12\n\"guillaume\",\"plique\",\"42\"\r\n";

        let mut reader = BufferedReader::with_capacity(Cursor::new(csv), 32, b',', b'"');
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
    fn test_read_byte_record() -> io::Result<()> {
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
            let mut reader = BufferedReader::with_capacity(Cursor::new(csv), capacity, b',', b'"');

            assert_eq!(
                reader.byte_records().collect::<Result<Vec<_>, _>>()?,
                expected
            );
        }

        Ok(())
    }

    #[test]
    fn test_strip_bom() -> io::Result<()> {
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
    fn test_empty_row() -> io::Result<()> {
        let data = "name\n\"\"\nlucy\n\"\"";

        // Counting
        let mut reader = BufferedReader::new(Cursor::new(data), b',', b'"');

        assert_eq!(reader.count_records()?, 4);

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

    // #[test]
    // fn test_crlf() -> io::Result<()> {
    //     let reader = BufferedReader::new(
    //         Cursor::new("name,surname\r\nlucy,\"john\"\r\nevan,zhong\r\nbéatrice,glougou\r\n"),
    //         b',',
    //         b'"',
    //     );

    //     let expected = vec![
    //         brec!["name", "surname"],
    //         brec!["lucy", "john"],
    //         brec!["evan", "zhong"],
    //         brec!["béatrice", "glougou"],
    //     ];

    //     let records = reader.into_byte_records().collect::<Result<Vec<_>, _>>()?;

    //     assert_eq!(records, expected);

    //     Ok(())
    // }
}
