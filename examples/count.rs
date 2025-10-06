use std::fs::File;

use clap::{Parser, ValueEnum};
use memmap2::Mmap;

#[derive(Debug, ValueEnum, Clone)]
enum CountingMode {
    Baseline,
    Simd,
    Split,
    Mmap,
    ZeroCopy,
    Copy,
    MmapCopy,
    Lines,
}

#[derive(Parser, Debug)]
struct Args {
    /// Counting mode to benchmark
    #[arg(value_enum)]
    mode: CountingMode,

    /// Path to target CSV file
    path: String,

    /// Check alignment (i.e. whether all rows have same number of columns)
    #[arg(short, long)]
    check_alignment: bool,
}

impl Args {
    fn delimiter(&self) -> u8 {
        if self.path.ends_with(".tsv") {
            b'\t'
        } else {
            b','
        }
    }

    fn simd_splitter(&self) -> csv::Result<simd_csv::Splitter<File>> {
        Ok(
            simd_csv::SplitterBuilder::with_capacity(BUFFERED_READER_DEFAULT_CAPACITY)
                .delimiter(self.delimiter())
                .from_reader(File::open(&self.path)?),
        )
    }

    fn simd_zero_copy_reader(&self) -> csv::Result<simd_csv::ZeroCopyReader<File>> {
        Ok(
            simd_csv::ZeroCopyReaderBuilder::with_capacity(BUFFERED_READER_DEFAULT_CAPACITY)
                .delimiter(self.delimiter())
                .from_reader(File::open(&self.path)?),
        )
    }

    fn simd_buffered_reader(&self) -> csv::Result<simd_csv::Reader<File>> {
        Ok(
            simd_csv::ReaderBuilder::with_capacity(BUFFERED_READER_DEFAULT_CAPACITY)
                .delimiter(self.delimiter())
                .from_reader(File::open(&self.path)?),
        )
    }
}

const BUFFERED_READER_DEFAULT_CAPACITY: usize = 1024 * (1 << 10);

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.mode {
        CountingMode::Baseline => {
            let mut reader_builder = csv::ReaderBuilder::new();
            reader_builder
                .has_headers(false)
                .delimiter(args.delimiter());
            let mut reader = reader_builder.from_path(&args.path)?;

            let mut count: u64 = 0;
            let mut record = csv::ByteRecord::new();

            while reader.read_byte_record(&mut record)? {
                count += 1;
            }

            println!("{}", count);
        }
        CountingMode::Simd => {
            let mut splitter = args.simd_splitter()?;

            println!("{}", splitter.count_records()?);
        }
        CountingMode::Split => {
            let mut splitter = args.simd_splitter()?;

            let mut count: u64 = 0;

            while let Some(_) = splitter.split_record()? {
                count += 1;
            }

            println!("{}", count);
        }
        CountingMode::Mmap => {
            let file = File::open(&args.path)?;

            let map = unsafe { Mmap::map(&file).unwrap() };

            let mut reader = simd_csv::TotalReaderBuilder::new()
                .delimiter(args.delimiter())
                .from_bytes(&map);

            println!("{}", reader.count_records());
        }
        CountingMode::ZeroCopy => {
            let mut reader = args.simd_zero_copy_reader()?;

            let mut count: u64 = 0;
            let mut alignment: Option<usize> = None;

            while let Some(record) = reader.read_byte_record()? {
                if args.check_alignment {
                    match alignment {
                        None => {
                            alignment = Some(record.len());
                        }
                        Some(expected) => {
                            if record.len() != expected {
                                eprintln!("Alignement error!");
                                std::process::exit(1);
                            }
                        }
                    }
                }

                count += 1;
            }

            println!("{}", count);
        }
        CountingMode::Copy => {
            let mut reader = args.simd_buffered_reader()?;
            let mut record = simd_csv::ByteRecord::new();

            let mut count: u64 = 0;

            while reader.read_byte_record(&mut record)? {
                count += 1;
            }

            println!("{}", count);
        }
        CountingMode::MmapCopy => {
            let file = File::open(&args.path)?;

            let map = unsafe { Mmap::map(&file).unwrap() };

            let mut reader = simd_csv::TotalReaderBuilder::new()
                .delimiter(args.delimiter())
                .from_bytes(&map);
            let mut record = simd_csv::ByteRecord::new();

            let mut count: u64 = 0;

            while reader.read_byte_record(&mut record)? {
                count += 1;
            }

            println!("{}", count);
        }
        CountingMode::Lines => {
            let file = File::open(&args.path)?;

            let mut reader = simd_csv::LineReader::new(file);

            println!("{}", reader.count_lines()?);
        }
    }

    Ok(())
}
