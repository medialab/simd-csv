use std::io::{self, BufWriter, IntoInnerError, Write};

use crate::records::ByteRecord;

/// A module exporting a [`BinaryWriter`] & [`BinaryReader`] useful to serialize
/// and deserialize [`ByteRecord`] faster, all while avoiding quoting & parsing
/// overhead.
///
/// The binary format is currently the following:
/// [bounds_len: u32][data_len: u32][bound_0_start: u32][bound_0_end: u32]...[data: bytes]

/// A writer variant able to write [`ByteRecord`] using a faster, binary
/// serialization format.
pub struct BinaryWriter<W: Write> {
    buf_writer: BufWriter<W>,
}

impl<W: Write> BinaryWriter<W> {
    pub fn from_writer(writer: W) -> Self {
        Self {
            buf_writer: BufWriter::with_capacity(8192, writer),
        }
    }

    /// Flush the underlying [`BufWriter`].
    #[inline(always)]
    pub fn flush(&mut self) -> io::Result<()> {
        self.buf_writer.flush()
    }

    #[inline]
    pub fn write_byte_record(&mut self, record: &ByteRecord) -> io::Result<()> {
        let (bounds, data) = record.as_parts();

        self.buf_writer
            .write_all(&(bounds.len() as u32).to_le_bytes())?;
        self.buf_writer
            .write_all(&(data.len() as u32).to_le_bytes())?;

        for (start, end) in bounds {
            self.buf_writer.write_all(&(*start as u32).to_le_bytes())?;
            self.buf_writer.write_all(&(*end as u32).to_le_bytes())?;
        }

        self.buf_writer.write_all(data)?;

        Ok(())
    }

    /// Attempt to unwrap the underlying [`BufWriter`] by flusing it and
    /// returning the original writer.
    #[inline]
    pub fn into_inner(self) -> Result<W, IntoInnerError<BufWriter<W>>> {
        self.buf_writer.into_inner()
    }
}

pub struct BinaryReader {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writer() {
        let record = brec!["john", "landis"];
        let buffer: Vec<u8> = vec![];
        let mut writer = BinaryWriter::from_writer(buffer);
        writer.write_byte_record(&record).unwrap();

        let buffer = writer.into_inner().unwrap();

        assert_eq!(
            buffer,
            [
                2, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 106, 111,
                104, 110, 108, 97, 110, 100, 105, 115,
            ]
        )
    }
}
