use std::io::{self, BufWriter, IntoInnerError, Write};

use memchr::{memchr, memchr3};

use crate::records::ByteRecord;

pub struct Writer<W: Write> {
    delimiter: u8,
    quote: u8,
    buffer: BufWriter<W>,
    field_count: Option<usize>,
}

impl<W: Write> Writer<W> {
    pub fn new(writer: W, delimiter: u8, quote: u8) -> Self {
        Self {
            buffer: BufWriter::new(writer),
            quote,
            delimiter,
            field_count: None,
        }
    }

    pub fn with_capacity(writer: W, capacity: usize, delimiter: u8, quote: u8) -> Self {
        Self {
            buffer: BufWriter::with_capacity(capacity, writer),
            quote,
            delimiter,
            field_count: None,
        }
    }

    #[inline(always)]
    pub fn flush(&mut self) -> io::Result<()> {
        self.buffer.flush()
    }

    #[inline]
    fn check_field_count(&mut self, written: usize) -> io::Result<()> {
        match self.field_count {
            Some(expected) => {
                if written != expected {
                    return Err(io::Error::other(format!("attempted to write record with {} fields, but the previous record had {} fields", written, expected)));
                }
            }
            None => {
                self.field_count = Some(written);
            }
        }

        Ok(())
    }

    pub fn write_record_no_quoting<I, T>(&mut self, record: I) -> io::Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        let mut first = true;
        let mut written: usize = 0;

        for cell in record.into_iter() {
            if first {
                first = false;
            } else {
                self.buffer.write_all(&[self.delimiter])?;
            }

            self.buffer.write_all(cell.as_ref())?;

            written += 1;
        }

        self.check_field_count(written)?;

        self.buffer.write_all(b"\n")?;

        Ok(())
    }

    #[inline(always)]
    pub fn write_byte_record_no_quoting(&mut self, record: &ByteRecord) -> io::Result<()> {
        self.write_record_no_quoting(record.iter())
    }

    #[inline(always)]
    fn should_quote(&self, cell: &[u8]) -> bool {
        if cell.len() < 8 {
            cell.iter()
                .copied()
                .any(|b| b == self.quote || b == self.delimiter || b == b'\n')
        } else {
            memchr3(self.quote, self.delimiter, b'\n', cell).is_some()
        }
    }

    fn write_quoted_cell(&mut self, cell: &[u8]) -> io::Result<()> {
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

    pub fn write_record<I, T>(&mut self, record: I) -> io::Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        let mut first = true;
        let mut written: usize = 0;

        for cell in record.into_iter() {
            if first {
                first = false;
            } else {
                self.buffer.write_all(&[self.delimiter])?;
            }

            let cell = cell.as_ref();

            if self.should_quote(cell) {
                self.write_quoted_cell(cell)?;
            } else {
                self.buffer.write_all(cell)?;
            }

            written += 1;
        }

        self.check_field_count(written)?;

        self.buffer.write_all(b"\n")?;

        Ok(())
    }

    #[inline(always)]
    pub fn write_byte_record(&mut self, record: &ByteRecord) -> io::Result<()> {
        self.write_record(record.iter())
    }

    #[inline]
    pub fn into_inner(self) -> Result<W, IntoInnerError<BufWriter<W>>> {
        self.buffer.into_inner()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    use crate::brec;

    #[test]
    fn test_write_byte_record() -> io::Result<()> {
        let output = Cursor::new(Vec::<u8>::new());
        let mut writer = Writer::with_capacity(output, 32, b',', b'"');

        writer.write_byte_record_no_quoting(&brec!["name", "surname", "age"])?;
        writer.write_byte_record(&brec!["john,", "landis", "45"])?;
        writer.write_byte_record(&brec!["lucy", "get\ngot", "\"te,\"st\""])?;

        assert_eq!(
            std::str::from_utf8(writer.into_inner()?.get_ref()).unwrap(),
            "name,surname,age\n\"john,\",landis,45\nlucy,\"get\ngot\",\"\"\"te,\"\"st\"\"\"\n",
        );

        Ok(())
    }
}
