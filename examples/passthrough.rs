use std::fs::File;

use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    /// Path to target CSV file
    path: String,
}

impl Args {
    fn delimiter(&self) -> u8 {
        if self.path.ends_with(".tsv") {
            b'\t'
        } else {
            b','
        }
    }
}

const DEFAULT_CAPACITY: usize = 1024 * (1 << 10);

fn main() -> csv::Result<()> {
    let args = Args::parse();
    let delimiter = args.delimiter();
    let file = File::open(&args.path)?;

    let mut reader =
        simd_csv::BufferedReader::with_capacity(file, DEFAULT_CAPACITY, delimiter, b'"');

    let mut record = simd_csv::ByteRecord::new();

    let mut writer =
        simd_csv::Writer::with_capacity(std::io::stdout(), DEFAULT_CAPACITY, delimiter, b'"');

    while reader.read_byte_record(&mut record)? {
        writer.write_byte_record(&record)?;
    }

    writer.flush()?;

    Ok(())
}
