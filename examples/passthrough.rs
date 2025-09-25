use std::fs::File;

use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    /// Path to target CSV file
    path: String,

    /// Whether to use SIMD acceleration
    #[arg(long)]
    simd: bool,

    #[arg(long)]
    only_read: bool,
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

    if args.simd {
        let mut reader =
            simd_csv::BufferedReader::with_capacity(file, DEFAULT_CAPACITY, delimiter, b'"');

        let mut record = simd_csv::ByteRecord::new();

        let mut writer =
            simd_csv::Writer::with_capacity(std::io::stdout(), DEFAULT_CAPACITY, delimiter, b'"');

        while reader.read_byte_record(&mut record)? {
            if !args.only_read {
                writer.write_byte_record(&record)?;
            }
        }

        writer.flush()?;
    } else {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(delimiter)
            .from_reader(file);

        let mut record = csv::ByteRecord::new();

        let mut writer = csv::WriterBuilder::new()
            .delimiter(delimiter)
            .from_writer(std::io::stdout());

        while reader.read_byte_record(&mut record)? {
            if !args.only_read {
                writer.write_byte_record(&record)?;
            }
        }

        writer.flush()?;
    }

    Ok(())
}
