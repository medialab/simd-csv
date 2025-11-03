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

    /// Last record
    #[arg(long)]
    last: bool,

    /// Approx count
    #[arg(long)]
    approx_count: bool,

    /// No headers?
    #[arg(short, long)]
    no_headers: bool,
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
        .has_headers(!args.no_headers)
        .from_reader(file)?
        .unwrap();

    if args.sample {
        dbg!(seeker.sample());
    } else if let Some(offset) = args.offset {
        dbg!(seeker.seek(offset)?);
    } else if args.approx_count {
        println!("{}", seeker.approx_count());
    } else if args.last {
        println!("{:?}", seeker.last_byte_record()?);
    } else {
        unimplemented!()
    }

    Ok(())
}
