use std::io::{self, BufWriter, IntoInnerError, Write};

use crate::records::ByteRecord;
use crate::searcher::Searcher;

pub struct Writer<W: Write> {
    delimiter: u8,
    quote: u8,
    buffer: BufWriter<W>,
    quote_bounds: Vec<usize>,
    searcher: Searcher,
}

impl<W: Write> Writer<W> {
    pub fn new(writer: W, delimiter: u8, quote: u8) -> Self {
        Self {
            buffer: BufWriter::new(writer),
            quote,
            delimiter,
            quote_bounds: vec![0],
            searcher: Searcher::new(delimiter, quote, b'\n'),
        }
    }

    pub fn with_capacity(writer: W, capacity: usize, delimiter: u8, quote: u8) -> Self {
        Self {
            buffer: BufWriter::with_capacity(capacity, writer),
            quote,
            delimiter,
            quote_bounds: vec![0],
            searcher: Searcher::new(delimiter, quote, b'\n'),
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

    fn assess_quoting(&mut self, cell: &[u8]) -> bool {
        let mut must_quote = false;

        self.quote_bounds.truncate(1);

        for offset in self.searcher.search(cell) {
            let byte = cell[offset];

            if byte == self.quote {
                self.quote_bounds.push(offset);
            }

            must_quote = true;
        }

        if self.quote_bounds.len() > 1 {
            self.quote_bounds.push(cell.len());
        }

        must_quote
    }

    fn write_quoted_cell(&mut self, cell: &[u8]) -> io::Result<()> {
        self.buffer.write_all(&[self.quote])?;

        if self.quote_bounds.len() < 2 {
            self.buffer.write_all(cell)?;
        } else {
            let windows = self.quote_bounds.windows(2);
            let last_i = windows.len().saturating_sub(1);

            for (i, w) in windows.enumerate() {
                self.buffer.write_all(&cell[w[0]..w[1]])?;

                if i != last_i {
                    self.buffer.write_all(&[self.quote])?;
                }
            }
        }

        self.buffer.write_all(&[self.quote])?;

        Ok(())
    }

    pub fn write_byte_record(&mut self, record: &ByteRecord) -> io::Result<()> {
        let last_i = record.len().saturating_sub(1);

        for (i, cell) in record.iter().enumerate() {
            let must_quote = self.assess_quoting(cell);

            if !must_quote {
                self.buffer.write_all(cell)?;
            } else {
                self.write_quoted_cell(cell)?;
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
