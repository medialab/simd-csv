use std::io::{self, BufReader, BufWriter, IntoInnerError, Read, Write};

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

pub struct BinaryReader<R> {
    buf_reader: BufReader<R>,
}

impl<R: Read> BinaryReader<R> {
    pub fn from_reader(reader: R) -> Self {
        Self {
            buf_reader: BufReader::with_capacity(8192, reader),
        }
    }

    fn try_read_exact(&mut self, record: &mut ByteRecord, buf: &mut [u8]) -> io::Result<bool> {
        match self.buf_reader.read_exact(buf) {
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

    pub fn read_byte_record(&mut self, record: &mut ByteRecord) -> io::Result<bool> {
        record.clear();

        let mut u32_buffer = [0u8; 4];

        if !self.try_read_exact(record, &mut u32_buffer)? {
            return Ok(false);
        }

        let bounds_len = u32::from_le_bytes(u32_buffer) as usize;
        record.bounds.reserve(bounds_len);

        if !self.try_read_exact(record, &mut u32_buffer)? {
            return Ok(false);
        }

        let data_len = u32::from_le_bytes(u32_buffer) as usize;
        record.data.reserve(data_len);

        // TODO: we probably need to validate the bounds, to avoid issues
        // with malformed streams!
        for _ in 0..bounds_len {
            if !self.try_read_exact(record, &mut u32_buffer)? {
                return Ok(false);
            }
            let start = u32::from_le_bytes(u32_buffer);

            if !self.try_read_exact(record, &mut u32_buffer)? {
                return Ok(false);
            }
            let end = u32::from_le_bytes(u32_buffer);

            record.bounds.push((start as usize, end as usize));
        }

        unsafe {
            record.data.set_len(data_len);
        }

        match self.buf_reader.read_exact(&mut record.data[..data_len]) {
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
