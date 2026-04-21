use std::io::{self, BufWriter, IntoInnerError, Read, Write};

use crate::records::ByteRecord;

/// A module exporting a [`BinaryWriter`] & [`BinaryReader`] useful to serialize
/// and deserialize [`ByteRecord`] faster, all while avoiding quoting & parsing
/// overhead.
///
/// The binary format is currently the following:
/// [bounds_len: u32][data_len: u32][bound_0_start: u32][bound_0_end: u32]...[data: bytes]
///
/// This should not be used yet as it is ironically slower than CSV parsing...

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

pub struct BinaryReader<R> {
    inner: R,
    buffer: Vec<u8>,
}

impl<R: Read> BinaryReader<R> {
    pub fn from_reader(reader: R) -> Self {
        Self {
            inner: reader,
            buffer: Vec::new(),
        }
    }

    pub fn read_byte_record(&mut self, record: &mut ByteRecord) -> io::Result<bool> {
        record.clear();

        let mut counts_buffer = [0u8; 8];

        match self.inner.read_exact(&mut counts_buffer) {
            Ok(()) => (),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                // NOTE: we don't leave the record in an invalid state!
                record.clear();
                return Ok(false);
            }
            Err(err) => {
                // NOTE: we don't leave the record in an invalid state!
                record.clear();
                return Err(err);
            }
        }

        let [b1, b2, b3, b4, d1, d2, d3, d4] = counts_buffer;

        let bounds_len = u32::from_le_bytes([b1, b2, b3, b4]) as usize;
        let data_len = u32::from_le_bytes([d1, d2, d3, d4]) as usize;

        record.bounds.reserve(bounds_len);
        self.buffer.reserve(bounds_len * 8);
        record.data.reserve(data_len);

        unsafe {
            self.buffer.set_len(bounds_len * 8);
        }

        match self.inner.read_exact(&mut self.buffer) {
            Ok(()) => (),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                // NOTE: we don't leave the record in an invalid state!
                record.clear();
                return Ok(false);
            }
            Err(err) => {
                // NOTE: we don't leave the record in an invalid state!
                record.clear();
                return Err(err);
            }
        }

        // TODO: we probably need to validate the bounds, to avoid issues
        // with malformed streams!, we can easily display fine-grained errors here
        for i in 0..bounds_len {
            let mut s = i * 8;

            let start = u32::from_le_bytes(self.buffer[s..s + 4].try_into().unwrap());

            s += 4;

            let end = u32::from_le_bytes(self.buffer[s..s + 4].try_into().unwrap());

            record.bounds.push((start as usize, end as usize));
        }

        unsafe {
            record.data.set_len(data_len);
        }

        match self.inner.read_exact(&mut record.data) {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                // NOTE: we don't leave the record in an invalid state!
                record.clear();
                Ok(false)
            }
            Err(err) => {
                // NOTE: we don't leave the record in an invalid state!
                record.clear();
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error;

    #[test]
    fn test_writer() -> error::Result<()> {
        let record = brec!["john", "landis"];
        let buffer: Vec<u8> = vec![];
        let mut writer = BinaryWriter::from_writer(buffer);
        writer.write_byte_record(&record)?;

        let buffer = writer.into_inner().unwrap();

        assert_eq!(
            buffer,
            [
                2, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 106, 111,
                104, 110, 108, 97, 110, 100, 105, 115,
            ]
        );

        Ok(())
    }

    #[test]
    fn test_reader() -> error::Result<()> {
        let records = vec![brec!["john", "landis"], brec!["beatriz", "babka"]];
        let buffer: Vec<u8> = vec![];
        let mut writer = BinaryWriter::from_writer(buffer);

        for record in &records {
            writer.write_byte_record(record)?;
        }

        let buffer = writer.into_inner().unwrap();

        let mut reader = BinaryReader::from_reader(&buffer[..]);

        let mut out_records = vec![];
        let mut record = ByteRecord::new();

        while reader.read_byte_record(&mut record)? {
            out_records.push(record.clone());
        }

        assert_eq!(&out_records, &records);

        Ok(())
    }
}
