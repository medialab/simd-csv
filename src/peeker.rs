use std::io::{Chain, Cursor, Read};

use crate::buffer::ScratchBuffer;
use crate::core::{CoreReader, ReadResult};
use crate::error;
use crate::records::{ByteRecord, ZeroCopyByteRecord};
use crate::utils::trim_bom;

/// Builds a [`Peeker`] with given configuration.
pub struct PeekerBuilder {
    delimiter: u8,
    quote: u8,
    buffer_capacity: usize,
    has_headers: bool,
}

impl Default for PeekerBuilder {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            buffer_capacity: 8192,
            has_headers: true,
        }
    }
}

impl PeekerBuilder {
    /// Create a new [`PeekerBuilder`] with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new [`PeekerBuilder`] with provided `capacity`.
    pub fn with_capacity(capacity: usize) -> Self {
        let mut reader = Self::default();
        reader.buffer_capacity(capacity);
        reader
    }

    /// Set the delimiter to be used by the created [`Peeker`].
    ///
    /// This delimiter must be a single byte.
    ///
    /// Will default to a comma.
    pub fn delimiter(&mut self, delimiter: u8) -> &mut Self {
        self.delimiter = delimiter;
        self
    }

    /// Set the quote char to be used by the created [`Peeker`].
    ///
    /// This char must be a single byte.
    ///
    /// Will default to a double quote.
    pub fn quote(&mut self, quote: u8) -> &mut Self {
        self.quote = quote;
        self
    }

    /// Set the capacity of the created [`Peeker`]'s buffer
    pub fn buffer_capacity(&mut self, capacity: usize) -> &mut Self {
        self.buffer_capacity = capacity;
        self
    }

    /// Indicate whether first record must be understood as a header.
    ///
    /// Will default to `true`.
    pub fn has_headers(&mut self, yes: bool) -> &mut Self {
        self.has_headers = yes;
        self
    }

    /// Create a new [`Peeker`] using the provided reader implementing
    /// [`std::io::Read`].
    pub fn from_reader<R: Read>(&self, reader: R) -> Peeker<R> {
        Peeker {
            buffer: ScratchBuffer::with_capacity(self.buffer_capacity, reader),
            inner: CoreReader::new(self.delimiter, self.quote),
            headers: ByteRecord::new(),
            rest: Vec::new(),
            has_headers: self.has_headers,
            has_read: false,
            has_crlf_newlines: false,
            must_reemit_headers: !self.has_headers,
        }
    }
}

/// A [`Read`] stream peeker that can be used to check a CSV file's header.
pub struct Peeker<R> {
    buffer: ScratchBuffer<R>,
    inner: CoreReader,
    headers: ByteRecord,
    rest: Vec<u8>,
    has_headers: bool,
    has_read: bool,
    has_crlf_newlines: bool,
    must_reemit_headers: bool,
}

impl<R: Read> Peeker<R> {
    /// Create a new peeker with default configuration using the provided reader
    /// implementing [`std::io::Read`].
    pub fn from_reader(reader: R) -> Self {
        PeekerBuilder::new().from_reader(reader)
    }

    fn read_byte_record_impl(&mut self) -> error::Result<bool> {
        use ReadResult::*;

        let mut seps = vec![];

        loop {
            let seps_offset = self.buffer.saved().len();
            let input = self.buffer.fill_buf()?;

            let (result, pos) =
                self.inner
                    .split_record_and_find_separators(input, seps_offset, &mut seps);

            match result {
                End => {
                    self.buffer.consume(pos);
                    return Ok(false);
                }
                Cr | Lf => {
                    self.buffer.consume(pos);
                }
                InputEmpty => {
                    self.buffer.save();
                }
                Record => {
                    let bytes = self.buffer.flush(pos);

                    let record = ZeroCopyByteRecord::new(bytes, &seps, self.inner.quote);

                    if bytes.len().saturating_sub(2) == record.as_slice().len() {
                        self.has_crlf_newlines = true;
                    }

                    self.rest = bytes.to_vec();
                    self.headers = record.to_byte_record();

                    return Ok(true);
                }
            };
        }
    }

    fn on_first_read(&mut self) -> error::Result<()> {
        if self.has_read {
            return Ok(());
        }

        // Trimming BOM
        let input = self.buffer.fill_buf()?;
        let bom_len = trim_bom(input);
        self.buffer.consume(bom_len);

        // Reading headers
        let has_data = self.read_byte_record_impl()?;

        if !has_data {
            self.must_reemit_headers = false;
        }

        self.has_read = true;

        Ok(())
    }

    /// Returns whether this peeker has been configured to interpret the first
    /// record as a header.
    #[inline]
    pub fn has_headers(&self) -> bool {
        self.has_headers
    }

    /// Returns whether this peeker seems to be reading from a stream having
    /// CRLF newlines.
    #[inline]
    pub fn has_crlf_newlines(&mut self) -> error::Result<bool> {
        self.on_first_read()?;

        Ok(self.has_crlf_newlines)
    }

    /// Attempt to read the first record of the stream without consuming related
    /// bytes.
    pub fn peek_byte_record(&mut self) -> error::Result<&ByteRecord> {
        self.on_first_read()?;

        Ok(&self.headers)
    }

    /// Attempt to return the first record of the stream as bytes without consuming
    /// them.
    pub fn peek(&mut self) -> error::Result<&[u8]> {
        self.on_first_read()?;

        Ok(&self.rest)
    }

    pub fn into_reader(mut self) -> Chain<Cursor<Vec<u8>>, R> {
        let bufreader = self.buffer.into_bufreader();

        if !self.must_reemit_headers {
            self.rest.clear();
        }

        self.rest.extend_from_slice(bufreader.buffer());

        Cursor::new(self.rest).chain(bufreader.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peeker() -> error::Result<()> {
        // LF, headers
        let mut buffer: Vec<u8> = Vec::new();

        let mut peeker = Peeker::from_reader(&b"name,surname\nhello,world\njohn,lucy"[..]);

        assert_eq!(peeker.peek_byte_record()?, &brec!["name", "surname"]);
        assert_eq!(peeker.has_crlf_newlines()?, false);

        peeker.into_reader().read_to_end(&mut buffer)?;
        assert_eq!(&buffer, b"hello,world\njohn,lucy");

        // CRLF, headers
        let mut peeker = Peeker::from_reader(&b"name,surname\r\nhello,world\r\njohn,lucy"[..]);

        assert_eq!(peeker.peek_byte_record()?, &brec!["name", "surname"]);
        assert_eq!(peeker.has_crlf_newlines()?, true);

        buffer.clear();
        peeker.into_reader().read_to_end(&mut buffer)?;
        assert_eq!(&buffer, b"hello,world\r\njohn,lucy");

        // LF, no headers
        let mut peeker = PeekerBuilder::new()
            .has_headers(false)
            .from_reader(&b"bonjour,le monde\nhello,world\njohn,lucy"[..]);

        assert_eq!(peeker.peek_byte_record()?, &brec!["bonjour", "le monde"]);
        assert_eq!(peeker.has_crlf_newlines()?, false);

        buffer.clear();
        peeker.into_reader().read_to_end(&mut buffer)?;
        assert_eq!(&buffer, b"bonjour,le monde\nhello,world\njohn,lucy");

        // CRLF, no headers
        let mut peeker = PeekerBuilder::new()
            .has_headers(false)
            .from_reader(&b"bonjour,le monde\r\nhello,world\r\njohn,lucy"[..]);

        assert_eq!(peeker.peek_byte_record()?, &brec!["bonjour", "le monde"]);
        assert_eq!(peeker.has_crlf_newlines()?, true);

        buffer.clear();
        peeker.into_reader().read_to_end(&mut buffer)?;
        assert_eq!(&buffer, b"bonjour,le monde\r\nhello,world\r\njohn,lucy");

        Ok(())
    }
}
