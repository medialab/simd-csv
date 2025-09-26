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

    pub fn flush(&mut self) -> io::Result<()> {
        self.buffer.flush()
    }

    pub fn write_byte_record_no_quoting(&mut self, record: &ByteRecord) -> io::Result<()> {
        let last_i = record.len().saturating_sub(1);

        for (i, cell) in record.iter().enumerate() {
            self.buffer.write_all(cell)?;

            if i != last_i {
                self.buffer.write_all(&[self.delimiter])?;
            }
        }

        self.buffer.write_all(b"\n")?;

        Ok(())
    }

    fn should_quote(&self, cell: &[u8]) -> bool {
        memchr3(self.quote, self.delimiter, b'\n', cell).is_some()
    }

    fn write_quoted_cell(&mut self, cell: &[u8]) -> io::Result<()> {
        self.buffer.write_all(&[self.quote])?;

        let mut i: usize = 0;

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

    pub fn write_byte_record(&mut self, record: &ByteRecord) -> io::Result<()> {
        let last_i = record.len().saturating_sub(1);

        for (i, cell) in record.iter().enumerate() {
            if self.should_quote(cell) {
                self.write_quoted_cell(cell)?;
            } else {
                self.buffer.write_all(cell)?;
            }

            if i != last_i {
                self.buffer.write_all(&[self.delimiter])?;
            }
        }

        self.buffer.write_all(b"\n")?;

        Ok(())
    }

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

        // TODO: flexibility
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
