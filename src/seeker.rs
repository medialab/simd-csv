use std::io::{Read, Seek, SeekFrom};

use crate::error;
use crate::records::ByteRecord;
use crate::zero_copy_reader::ZeroCopyReaderBuilder;

#[derive(Debug)]
pub struct SeekerSample {
    headers: ByteRecord,
    record_count: u64,
    max_record_size: u64,
    median_record_size: u64,
    first_record_start_pos: u64,
    fields_mean_sizes: Vec<f64>,
    file_len: u64,
    has_reached_eof: bool,
}

impl SeekerSample {
    pub fn from_reader<R: Read + Seek>(
        mut reader: R,
        csv_reader_builder: &ZeroCopyReaderBuilder,
        sample_size: u64,
    ) -> error::Result<Option<Self>> {
        // NOTE: the given reader might have already read.
        // This is for instance the case for CSV-adjacent formats boasting
        // metadata in a header before tabular records even start.
        let initial_pos = reader.stream_position()?;
        let mut csv_reader = csv_reader_builder.from_reader(&mut reader);
        let headers = csv_reader.byte_headers()?.clone();
        let first_record_start_pos = initial_pos + csv_reader.position();

        let mut i: u64 = 0;
        let mut record_sizes: Vec<u64> = Vec::new();
        let mut fields_sizes: Vec<Vec<usize>> = Vec::with_capacity(sample_size as usize);

        while i < sample_size {
            if let Some(record) = csv_reader.read_byte_record()? {
                let record_size = record.as_slice().len() as u64 + 1;

                record_sizes.push(record_size);
                fields_sizes.push(record.iter().map(|cell| cell.len()).collect());

                i += 1;
            } else {
                break;
            }
        }

        // Not enough data to produce decent sample
        if i == 0 {
            return Ok(None);
        }

        let has_reached_eof = csv_reader.read_byte_record()?.is_none();
        let file_len = reader.seek(SeekFrom::End(0))?;
        let fields_mean_sizes = (0..headers.len())
            .map(|i| {
                fields_sizes.iter().map(|sizes| sizes[i]).sum::<usize>() as f64
                    / headers.len() as f64
            })
            .collect();

        record_sizes.sort();

        Ok(Some(Self {
            headers,
            record_count: i,
            max_record_size: *record_sizes.last().unwrap(),
            median_record_size: record_sizes[record_sizes.len() / 2],
            first_record_start_pos,
            fields_mean_sizes,
            has_reached_eof,
            file_len,
        }))
    }
}

pub struct SeekerBuilder {
    delimiter: u8,
    quote: u8,
    has_headers: bool,
    buffer_capacity: Option<usize>,
    sample_size: u64,
}

impl Default for SeekerBuilder {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            buffer_capacity: None,
            has_headers: true,
            sample_size: 128,
        }
    }
}

impl SeekerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut reader = Self::default();
        reader.buffer_capacity(capacity);
        reader
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
        self.buffer_capacity = Some(capacity);
        self
    }

    pub fn sample_size(&mut self, size: u64) -> &mut Self {
        self.sample_size = size;
        self
    }

    pub fn has_headers(&mut self, yes: bool) -> &mut Self {
        self.has_headers = yes;
        self
    }

    pub fn from_reader<R: Read + Seek>(&self, mut reader: R) -> error::Result<Option<Seeker<R>>> {
        let mut builder = ZeroCopyReaderBuilder::new();

        if let Some(capacity) = self.buffer_capacity {
            builder.buffer_capacity(capacity);
        }

        builder
            .delimiter(self.delimiter)
            .quote(self.quote)
            .has_headers(self.has_headers);

        match SeekerSample::from_reader(&mut reader, &builder, self.sample_size) {
            Ok(Some(sample)) => Ok(Some(Seeker {
                inner: reader,
                sample,
                _builder: builder,
            })),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

pub struct Seeker<R> {
    inner: R,
    sample: SeekerSample,
    _builder: ZeroCopyReaderBuilder,
}

impl<R> Seeker<R> {
    pub fn sample(&self) -> &SeekerSample {
        &self.sample
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}
