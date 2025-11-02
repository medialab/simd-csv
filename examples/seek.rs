use std::fs::File;

use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    /// Path to target CSV file
    path: String,

    /// Print sample
    #[arg(long)]
    sample: bool,

    /// Seek with offset
    #[arg(long)]
    offset: Option<u64>,
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

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let delimiter = args.delimiter();
    let file = File::open(&args.path)?;

    let mut seeker = simd_csv::SeekerBuilder::new()
        .delimiter(delimiter)
        .from_reader(file)?
        .unwrap();

    if args.sample {
        dbg!(seeker.sample());
    } else if let Some(offset) = args.offset {
        dbg!(seeker.seek(offset)?);
    }

    Ok(())
}
