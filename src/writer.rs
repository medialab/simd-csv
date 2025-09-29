use std::io::{self, BufWriter, IntoInnerError, Write};

use memchr::{memchr, memchr3};

use crate::records::ByteRecord;

pub struct Writer<W: Write> {
    delimiter: u8,
    quote: u8,
    buffer: BufWriter<W>,
}

impl<W: Write> Writer<W> {
    pub fn new(writer: W, delimiter: u8, quote: u8) -> Self {
        Self {
            buffer: BufWriter::new(writer),
            quote,
            delimiter,
        }
    }

    pub fn with_capacity(writer: W, capacity: usize, delimiter: u8, quote: u8) -> Self {
        Self {
            buffer: BufWriter::with_capacity(capacity, writer),
            quote,
            delimiter,
        }
    }

    #[inline(always)]
    pub fn flush(&mut self) -> io::Result<()> {
        self.buffer.flush()
    }

    pub fn write_record_no_quoting<I, T>(&mut self, record: I) -> io::Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        let mut first = true;

        for cell in record.into_iter() {
            if first {
                first = false;
            } else {
                self.buffer.write_all(&[self.delimiter])?;
            }

            self.buffer.write_all(cell.as_ref())?;
        }

        self.buffer.write_all(b"\n")?;

        Ok(())
    }

    #[inline(always)]
    pub fn write_byte_record_no_quoting(&mut self, record: &ByteRecord) -> io::Result<()> {
        self.write_record_no_quoting(record.iter())
    }

    #[inline]
    fn should_quote(&self, cell: &[u8]) -> (bool, Option<usize>) {
        match memchr3(self.quote, self.delimiter, b'\n', cell) {
            Some(offset) => {
                let byte = cell[offset];

                if byte == self.quote {
                    (true, Some(offset))
                } else {
                    (true, None)
                }
            }
            None => (false, None),
        }
    }

    fn write_quoted_cell(
        &mut self,
        cell: &[u8],
        first_quote_offset: Option<usize>,
    ) -> io::Result<()> {
        self.buffer.write_all(&[self.quote])?;

        let mut i: usize = 0;

        if let Some(offset) = first_quote_offset {
            i = offset + 1;
            self.buffer.write_all(&cell[..i])?;
            self.buffer.write_all(&[self.quote])?;
        }

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

        self.buffer.write_all(&[self.quote])?;

        Ok(())
    }

    pub fn write_record<I, T>(&mut self, record: I) -> io::Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        let mut first = true;

        for cell in record.into_iter() {
            if first {
                first = false;
            } else {
                self.buffer.write_all(&[self.delimiter])?;
            }

            let cell = cell.as_ref();

            let (should_quote, first_quote_offset) = self.should_quote(cell);

            if should_quote {
                self.write_quoted_cell(cell, first_quote_offset)?;
            } else {
                self.buffer.write_all(cell)?;
            }
        }

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
