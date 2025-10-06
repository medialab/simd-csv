use std::fs::File;

use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    /// Path to target CSV file
    path: String,

    /// Whether to use the novel simd parser
    #[arg(long)]
    simd: bool,
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

const BUFFERED_READER_DEFAULT_CAPACITY: usize = 1024 * (1 << 10);

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let delimiter = args.delimiter();
    let file = File::open(&args.path)?;
    let mut writer = csv::WriterBuilder::new().from_writer(std::io::stdout());

    if args.simd {
        let mut reader = simd_csv::ReaderBuilder::with_capacity(BUFFERED_READER_DEFAULT_CAPACITY)
            .delimiter(args.delimiter())
            .from_reader(File::open(&args.path)?);
        let mut record = simd_csv::ByteRecord::new();

        while reader.read_byte_record(&mut record)? {
            writer.write_record(record.iter())?;
        }
    } else {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(false)
            .from_reader(file);
        let mut record = csv::ByteRecord::new();

        while reader.read_byte_record(&mut record)? {
            writer.write_record(record.iter())?;
        }
    }

    Ok(())
}
