use std::fs::File;

use clap::{Parser, ValueEnum};
use csv::{ByteRecord, ReaderBuilder};
use memmap2::Mmap;
use simd_csv::{BufferedReader, TotalReader};

#[derive(Debug, ValueEnum, Clone)]
enum CountingMode {
    Baseline,
    Simd,
    Split,
    Mmap,
    ZeroCopy,
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

    fn simd_buffered_reader(&self) -> csv::Result<BufferedReader<File>> {
        Ok(BufferedReader::with_capacity(
            File::open(&self.path)?,
            BUFFERED_READER_DEFAULT_CAPACITY,
            self.delimiter(),
            b'"',
        ))
    }
}

const BUFFERED_READER_DEFAULT_CAPACITY: usize = 1024 * (1 << 10);

fn main() -> csv::Result<()> {
    let args = Args::parse();

    match args.mode {
        CountingMode::Baseline => {
            let mut reader_builder = ReaderBuilder::new();
            reader_builder
                .has_headers(false)
                .delimiter(args.delimiter());
            let mut reader = reader_builder.from_path(&args.path)?;

            let mut count: u64 = 0;
            let mut record = ByteRecord::new();

            while reader.read_byte_record(&mut record)? {
                count += 1;
            }

            println!("{}", count);
        }
        CountingMode::Simd => {
            let mut reader = args.simd_buffered_reader()?;

            println!("{}", reader.count_records()?);
        }
        CountingMode::Split => {
            let mut reader = args.simd_buffered_reader()?;

            let mut count: u64 = 0;

            while let Some(_) = reader.split_record()? {
                count += 1;
            }

            println!("{}", count);
        }
        CountingMode::Mmap => {
            let file = File::open(&args.path)?;

            let map = unsafe { Mmap::map(&file).unwrap() };

            let mut reader = TotalReader::new(args.delimiter(), b'"');

            println!("{}", reader.count_records(&map));
        }
        CountingMode::ZeroCopy => {
            let mut reader = args.simd_buffered_reader()?;

            let mut count: u64 = 0;
            let mut alignment: Option<usize> = None;

            while let Some(record) = reader.read_zero_copy_record()? {
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
    }

    Ok(())
}
