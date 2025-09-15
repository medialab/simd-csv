use std::fs::File;

use clap::Parser;
use csv::{ByteRecord, ReaderBuilder};
use simd_csv::BufferedReader;

#[derive(Parser, Debug)]
struct Args {
    /// Path to target CSV file
    path: String,

    /// Whether to enable SIMD acceleration
    #[arg(long)]
    simd: bool,

    // Whether to split the record using quasi-zero-copy methods
    #[arg(long)]
    split: bool,
}

fn main() -> csv::Result<()> {
    let args = Args::parse();

    if !args.simd && args.split {
        panic!("--split only works with --simd!");
    }

    let delimiter = if args.path.ends_with(".tsv") {
        b'\t'
    } else {
        b','
    };

    if !args.simd {
        let mut reader_builder = ReaderBuilder::new();
        reader_builder.has_headers(false).delimiter(delimiter);
        let mut reader = reader_builder.from_path(&args.path)?;

        let mut count: u64 = 0;
        let mut record = ByteRecord::new();

        while reader.read_byte_record(&mut record)? {
            count += 1;
        }

        println!("{}", count);
    } else {
        let mut reader = BufferedReader::with_capacity(
            File::open(&args.path)?,
            1024 * (1 << 10),
            delimiter,
            b'"',
        );

        if args.split {
            let mut count: u64 = 0;

            while let Some(_) = reader.split_record()? {
                count += 1;
            }

            println!("{}", count);
        } else {
            println!("{}", reader.count_records()?);
        }
    }

    Ok(())
}
