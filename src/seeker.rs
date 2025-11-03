use std::io::{Cursor, Read, Seek, SeekFrom};

use crate::error::{self, Error, ErrorKind};
use crate::records::ByteRecord;
use crate::utils::ReverseReader;
use crate::zero_copy_reader::{ZeroCopyReader, ZeroCopyReaderBuilder};

#[derive(Debug)]
struct SeekerSample {
    headers: ByteRecord,
    record_count: u64,
    max_record_size: u64,
    median_record_size: u64,
    first_record_pos: u64,
    fields_mean_sizes: Vec<f64>,
    file_len: u64,
    has_reached_eof: bool,
}

impl SeekerSample {
    fn from_reader<R: Read + Seek>(
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

        let first_record_pos = if csv_reader.has_headers() {
            initial_pos + csv_reader.position()
        } else {
            initial_pos
        };

        let mut i: u64 = 0;
        let mut record_sizes: Vec<u64> = Vec::new();
        let mut fields_sizes: Vec<Vec<usize>> = Vec::with_capacity(sample_size as usize);

        while i < sample_size {
            if let Some(record) = csv_reader.read_byte_record()? {
                // The "+ 1" is taking \n into account for better accuracy
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
                    / fields_sizes.len() as f64
            })
            .collect();

        record_sizes.sort();

        Ok(Some(Self {
            headers,
            record_count: i,
            max_record_size: *record_sizes.last().unwrap(),
            median_record_size: record_sizes[record_sizes.len() / 2],
            first_record_pos,
            fields_mean_sizes,
            has_reached_eof,
            file_len,
        }))
    }
}

fn cosine(profile: &[f64], other: impl Iterator<Item = usize>) -> f64 {
    let mut self_norm = 0.0;
    let mut other_norm = 0.0;
    let mut intersection = 0.0;

    for (a, b) in profile.iter().copied().zip(other.map(|i| i as f64)) {
        self_norm += a * a;
        other_norm += b * b;
        intersection += a * b;
    }

    intersection / (self_norm * other_norm).sqrt()
}

pub struct SeekerBuilder {
    delimiter: u8,
    quote: u8,
    has_headers: bool,
    buffer_capacity: Option<usize>,
    sample_size: u64,
    lookahead_factor: u64,
}

impl Default for SeekerBuilder {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            buffer_capacity: None,
            has_headers: true,
            sample_size: 128,
            lookahead_factor: 32,
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

    pub fn lookahead_factor(&mut self, factor: u64) -> &mut Self {
        self.lookahead_factor = factor;
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
            Ok(Some(sample)) => {
                builder.has_headers(false).flexible(true);

                Ok(Some(Seeker {
                    inner: reader,
                    lookahead_factor: self.lookahead_factor,
                    scratch: Vec::with_capacity(
                        (self.lookahead_factor * sample.max_record_size) as usize,
                    ),
                    sample,
                    builder,
                }))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

fn lookahead<R: Read>(
    reader: &mut ZeroCopyReader<R>,
    expected_field_count: usize,
) -> error::Result<Option<(u64, ByteRecord)>> {
    let mut i: usize = 0;
    let mut next_record: Option<(u64, ByteRecord)> = None;
    let mut field_counts: Vec<usize> = Vec::new();
    let mut pos: u64 = 0;

    while let Some(record) = reader.read_byte_record()? {
        if i > 0 {
            field_counts.push(record.len());

            if i == 1 {
                next_record = Some((pos, record.to_byte_record()));
            }
        }

        pos = reader.position();
        i += 1;
    }

    // NOTE: if we have less than 2 records beyond the first one, it will be hard to
    // make a correct decision
    // NOTE: last record might be unaligned since we artificially clamp the read buffer
    if field_counts.len() < 2
        || field_counts[..field_counts.len() - 1]
            .iter()
            .any(|l| *l != expected_field_count)
    {
        Ok(None)
    } else {
        Ok(next_record)
    }
}

pub struct Seeker<R> {
    inner: R,
    sample: SeekerSample,
    lookahead_factor: u64,
    scratch: Vec<u8>,
    builder: ZeroCopyReaderBuilder,
}

impl<R: Read + Seek> Seeker<R> {
    pub fn first_record_pos(&self) -> u64 {
        self.sample.first_record_pos
    }

    pub fn file_len(&self) -> u64 {
        self.sample.file_len
    }

    #[inline]
    pub fn exact_count(&self) -> Option<u64> {
        self.sample
            .has_reached_eof
            .then_some(self.sample.record_count)
    }

    #[inline]
    pub fn approx_count(&self) -> u64 {
        let sample = &self.sample;

        if sample.has_reached_eof {
            sample.record_count
        } else {
            ((sample.file_len - sample.first_record_pos) as f64 / sample.median_record_size as f64)
                .ceil() as u64
        }
    }

    pub fn seek(&mut self, from_pos: u64) -> error::Result<Option<(u64, ByteRecord)>> {
        if from_pos < self.first_record_pos() || from_pos >= self.sample.file_len {
            return Err(Error::new(ErrorKind::OutOfBounds {
                pos: from_pos,
                start: self.first_record_pos(),
                end: self.sample.file_len,
            }));
        }

        self.inner.seek(SeekFrom::Start(from_pos))?;

        // NOTE: first record does not need to be more complex
        if from_pos == self.first_record_pos() {
            let first_record = self
                .builder
                .from_reader(&mut self.inner)
                .read_byte_record()?
                .unwrap()
                .to_byte_record();

            return Ok(Some((self.first_record_pos(), first_record)));
        }

        self.scratch.clear();
        (&mut self.inner)
            .take(self.lookahead_factor * self.sample.max_record_size)
            .read_to_end(&mut self.scratch)?;

        let mut unquoted_reader = self.builder.from_reader(self.scratch.as_slice());
        let mut quoted_reader = self
            .builder
            .from_reader(Cursor::new(b"\"").chain(self.scratch.as_slice()));

        let expected_field_count = self.sample.headers.len();

        let unquoted = lookahead(&mut unquoted_reader, expected_field_count)?;
        let quoted = lookahead(&mut quoted_reader, expected_field_count)?;

        match (unquoted, quoted) {
            (None, None) => Ok(None),
            (Some((pos, record)), None) => Ok(Some((from_pos + pos, record))),
            (None, Some((pos, record))) => Ok(Some((from_pos + pos - 1, record))),
            (Some((unquoted_pos, unquoted_record)), Some((mut quoted_pos, quoted_record))) => {
                // Sometimes we might fall within a cell whose contents suspiciously yield
                // the same record structure. In this case we rely on cosine similarity over
                // record profiles to make sure we select the correct offset.
                quoted_pos -= 1;

                // A tie in offset pos means we are unquoted
                if unquoted_pos == quoted_pos {
                    Ok(Some((from_pos + unquoted_pos, unquoted_record)))
                } else {
                    let unquoted_cosine = cosine(
                        &self.sample.fields_mean_sizes,
                        unquoted_record.iter().map(|cell| cell.len()),
                    );
                    let quoted_cosine = cosine(
                        &self.sample.fields_mean_sizes,
                        quoted_record.iter().map(|cell| cell.len()),
                    );

                    if unquoted_cosine > quoted_cosine {
                        Ok(Some((from_pos + unquoted_pos, unquoted_record)))
                    } else {
                        Ok(Some((from_pos + quoted_pos, quoted_record)))
                    }
                }
            }
        }
    }

    pub fn segments(&mut self, count: usize) -> error::Result<Vec<(u64, u64)>> {
        let sample = &self.sample;
        let file_len = sample.file_len;

        // File is way too short
        if self.sample.record_count < count as u64 {
            return Ok(vec![(self.first_record_pos(), file_len)]);
        }

        let adjusted_file_len = file_len - self.first_record_pos();

        // Adjusting chunks
        let count = count
            .min(
                (file_len / (sample.max_record_size * self.lookahead_factor)).saturating_sub(1)
                    as usize,
            )
            .max(1);

        let mut offsets = vec![self.first_record_pos()];

        for i in 1..count {
            let file_offset = ((i as f64 / count as f64) * adjusted_file_len as f64).floor() as u64
                + self.first_record_pos();

            if let Some((record_offset, _)) = self.seek(file_offset)? {
                offsets.push(record_offset);
            } else {
                break;
            }
        }

        offsets.push(file_len);

        Ok(offsets.windows(2).map(|w| (w[0], w[1])).collect())
    }

    pub fn byte_headers(&self) -> &ByteRecord {
        &self.sample.headers
    }

    pub fn first_byte_record(&mut self) -> error::Result<Option<ByteRecord>> {
        match self.seek(self.first_record_pos()) {
            Ok(Some((_, record))) => Ok(Some(record)),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn last_byte_record(&mut self) -> error::Result<Option<ByteRecord>> {
        let reverse_reader = ReverseReader::new(
            &mut self.inner,
            self.sample.file_len,
            self.sample.first_record_pos,
        );

        let mut reverse_csv_reader = self.builder.from_reader(reverse_reader);

        reverse_csv_reader
            .read_byte_record()
            .map(|record_opt| record_opt.map(|record| record.to_byte_record_in_reverse()))
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}
