use std::io::{self, BufWriter, IntoInnerError, Write};

use memchr::memchr;

use crate::error::{self, Error, ErrorKind};
use crate::records::{ByteRecord, ZeroCopyByteRecord};

/// Builds a [`Writer`] with given configuration.
pub struct WriterBuilder {
    delimiter: u8,
    quote: u8,
    buffer_capacity: usize,
    flexible: bool,
}

impl Default for WriterBuilder {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            buffer_capacity: 8192,
            flexible: false,
        }
    }
}

impl WriterBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut builder = Self::default();
        builder.buffer_capacity(capacity);
        builder
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
        self.buffer_capacity = capacity;
        self
    }

    pub fn flexible(&mut self, yes: bool) -> &mut Self {
        self.flexible = yes;
        self
    }

    pub fn from_writer<W: Write>(&self, writer: W) -> Writer<W> {
        let mut must_quote = [false; 256];
        must_quote[b'\r' as usize] = true;
        must_quote[b'\n' as usize] = true;
        must_quote[self.delimiter as usize] = true;
        must_quote[self.quote as usize] = true;

        Writer {
            delimiter: self.delimiter,
            quote: self.quote,
            buffer: BufWriter::with_capacity(self.buffer_capacity, writer),
            flexible: self.flexible,
            field_count: None,
            must_quote,
        }
    }
}

/// An already configured CSV writer.
///
/// # Configuration
///
/// To configure a [`Writer`], if you need a custom delimiter for instance of if
/// you want to tweak the size of the inner buffer. Check out the
/// [`WriterBuilder`].
pub struct Writer<W: Write> {
    delimiter: u8,
    quote: u8,
    buffer: BufWriter<W>,
    flexible: bool,
    field_count: Option<usize>,
    must_quote: [bool; 256],
}

impl<W: Write> Writer<W> {
    pub fn from_writer(writer: W) -> Self {
        WriterBuilder::new().from_writer(writer)
    }

    #[inline(always)]
    pub fn flush(&mut self) -> io::Result<()> {
        self.buffer.flush()
    }

    #[inline]
    fn check_field_count(&mut self, written: usize) -> error::Result<()> {
        if self.flexible {
            return Ok(());
        }

        match self.field_count {
            Some(expected) => {
                if written != expected {
                    return Err(Error::new(ErrorKind::UnequalLengths {
                        expected_len: expected,
                        len: written,
                        pos: None,
                    }));
                }
            }
            None => {
                self.field_count = Some(written);
            }
        }

        Ok(())
    }

    pub fn write_record_no_quoting<I, T>(&mut self, record: I) -> error::Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        let mut first = true;
        let mut written: usize = 0;
        let mut empty = false;

        for cell in record.into_iter() {
            if first {
                first = false;
            } else {
                self.buffer.write_all(&[self.delimiter])?;
            }

            let cell = cell.as_ref();

            if cell.is_empty() {
                empty = true;
            }

            self.buffer.write_all(cell)?;

            written += 1;
        }

        if written == 1 && empty {
            self.buffer.write_all(&[self.quote, self.quote])?;
        }

        self.check_field_count(written)?;

        self.buffer.write_all(b"\n")?;

        Ok(())
    }

    #[inline(always)]
    pub fn write_byte_record_no_quoting(&mut self, record: &ByteRecord) -> error::Result<()> {
        self.write_record_no_quoting(record.iter())
    }

    #[inline]
    fn should_quote(&self, mut cell: &[u8]) -> bool {
        // This strategy comes directly from `rust-csv`
        let mut yes = false;
        while !yes && cell.len() >= 8 {
            yes = self.must_quote[cell[0] as usize]
                || self.must_quote[cell[1] as usize]
                || self.must_quote[cell[2] as usize]
                || self.must_quote[cell[3] as usize]
                || self.must_quote[cell[4] as usize]
                || self.must_quote[cell[5] as usize]
                || self.must_quote[cell[6] as usize]
                || self.must_quote[cell[7] as usize];
            cell = &cell[8..];
        }
        yes || cell.iter().any(|&b| self.must_quote[b as usize])
    }

    fn write_quoted_cell(&mut self, cell: &[u8]) -> error::Result<()> {
        self.buffer.write_all(&[self.quote])?;

        let mut i: usize = 0;

        if cell.len() < 8 {
            while i < cell.len() {
                match cell[i..].iter().copied().position(|b| b == self.quote) {
                    None => {
                        self.buffer.write_all(&cell[i..])?;
                        break;
                    }
                    Some(offset) => {
                        self.buffer.write_all(&cell[i..i + offset + 1])?;
                        self.buffer.write_all(&[self.quote])?;
                        i += offset + 1;
                    }
                }
            }
        } else {
            while i < cell.len() {
                match memchr(self.quote, &cell[i..]) {
                    None => {
                        self.buffer.write_all(&cell[i..])?;
                        break;
                    }
                    Some(offset) => {
                        self.buffer.write_all(&cell[i..i + offset + 1])?;
                        self.buffer.write_all(&[self.quote])?;
                        i += offset + 1;
                    }
                };
            }
        }

        self.buffer.write_all(&[self.quote])?;

        Ok(())
    }

    pub fn write_record<I, T>(&mut self, record: I) -> error::Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        let mut first = true;
        let mut written: usize = 0;
        let mut empty = false;

        for cell in record.into_iter() {
            if first {
                first = false;
            } else {
                self.buffer.write_all(&[self.delimiter])?;
            }

            let cell = cell.as_ref();

            if cell.is_empty() {
                empty = true;
            }

            if self.should_quote(cell) {
                self.write_quoted_cell(cell)?;
            } else {
                self.buffer.write_all(cell)?;
            }

            written += 1;
        }

        if written == 1 && empty {
            self.buffer.write_all(&[self.quote, self.quote])?;
        }

        self.check_field_count(written)?;

        self.buffer.write_all(b"\n")?;

        Ok(())
    }

    #[inline(always)]
    pub fn write_byte_record(&mut self, record: &ByteRecord) -> error::Result<()> {
        self.write_record(record.iter())
    }

    #[inline]
    pub fn write_zero_copy_byte_record(
        &mut self,
        record: &ZeroCopyByteRecord,
    ) -> error::Result<()> {
        if record.quote == self.quote {
            self.write_record_no_quoting(record.iter())
        } else {
            self.write_record(record.unescaped_iter())
        }
    }

    #[inline(always)]
    pub fn write_splitted_record(&mut self, record: &[u8]) -> error::Result<()> {
        self.buffer.write_all(record)?;
        self.buffer.write_all(b"\n")?;

        Ok(())
    }

    #[inline]
    pub fn into_inner(self) -> Result<W, IntoInnerError<BufWriter<W>>> {
        self.buffer.into_inner()
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};

    use super::*;

    #[test]
    fn test_write_byte_record() -> io::Result<()> {
        let output = Cursor::new(Vec::<u8>::new());
        let mut writer = WriterBuilder::with_capacity(32).from_writer(output);

        writer.write_byte_record_no_quoting(&brec!["name", "surname", "age"])?;
        writer.write_byte_record(&brec!["john,", "landis", "45"])?;
        writer.write_byte_record(&brec!["lucy", "get\ngot", "\"te,\"st\""])?;

        assert_eq!(
            std::str::from_utf8(writer.into_inner()?.get_ref()).unwrap(),
            "name,surname,age\n\"john,\",landis,45\nlucy,\"get\ngot\",\"\"\"te,\"\"st\"\"\"\n",
        );

        Ok(())
    }

    #[test]
    fn test_write_empty_cells() {
        fn write(record: &ByteRecord) -> String {
            let output = Cursor::new(Vec::<u8>::new());
            let mut writer = Writer::from_writer(output);
            writer.write_byte_record(record).unwrap();
            String::from_utf8_lossy(&writer.into_inner().unwrap().into_inner()).into_owned()
        }

        assert_eq!(write(&brec![]), "\n");
        assert_eq!(write(&brec![""]), "\"\"\n");
        assert_eq!(write(&brec!["", "", ""]), ",,\n");
        assert_eq!(write(&brec!["name", "", "age"]), "name,,age\n");
        assert_eq!(write(&brec!["name", ""]), "name,\n");
    }

    #[test]
    fn should_quote() {
        let writer = Writer::from_writer(Cursor::new(Vec::<u8>::new()));

        assert_eq!(writer.should_quote(b"test"), false);
        assert_eq!(writer.should_quote(b"test,"), true);
        assert_eq!(writer.should_quote(b"te\"st"), true);
        assert_eq!(writer.should_quote(b"te\nst"), true);
        assert_eq!(
            writer.should_quote(b"testtesttesttesttesttesttesttest\n"),
            true
        );
        assert_eq!(writer.should_quote(b"te\rst"), true);
    }
}
