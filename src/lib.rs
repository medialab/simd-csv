/*!
The `simd-csv` crate provides specialized readers & writers of CSV data able
to leverage [SIMD](https://en.wikipedia.org/wiki/Single_instruction,_multiple_data) instructions.

It has been designed to fit the [xan](https://github.com/medialab/xan) command line tool's
requirements, but can be used by anyone to speed up CSV parsing.

Is is less flexible and user-friendly than the [`csv`](https://docs.rs/csv/) crate,
so one should make sure the performance gain is worth it before going further.

This crate is not a port of [simdjson](https://arxiv.org/abs/1902.08318) branchless logic
applied to CSV parsing. It uses a somewhat novel approach instead, mixing traditional state
machine logic with [`memchr`](https://docs.rs/memchr/latest/memchr/)-like SIMD-accelerated
string searching. See the [design notes](#design-notes) for more details.

# Examples

*Reading a CSV file while amortizing allocations*

```
use std::fs::File;
use simd_csv::{Reader, ByteRecord};

let mut reader = Reader::from_reader(File::open("data.csv")?);
let mut record = ByteRecord::new();

while reader.read_byte_record(&mut record)? {
    for cell in record.iter() {
        dbg!(cell);
    }
}
```

*Using a builder to configure your reader*

```
use std::fs::File;
use simd_csv::ReaderBuilder;

let mut reader = ReaderBuilder::new()
    .delimiter(b'\t')
    .buffer_capacity(16 * (1 << 10))
    .from_reader(File::open("data.csv")?);
```

*Using the zero-copy reader*

```
use std::fs::File;
use simd_csv::ZeroCopyReader;

let mut reader = ZeroCopyReader::from_reader(File::new("data.csv")?);

while let Some(record) = reader.read_byte_record()? {
    // Only unescaping third column:
    dbg!(record.unescape(2));
}
```

*Counting records as fast as possible using the splitter*

```
use std::fs::File;
use simd_csv::Splitter;

let mut splitter = Splitter::from_reader(File::new("data.csv")?);

println!("{}", splitter.count_records()?);
```

# Readers

From least to most performant. Also from most integrated to most barebone.

- [`Reader`], [`ReaderBuilder`]: a streaming copy reader, unescaping quoted data on the fly.
  This is the closest thing you will find to the [`csv`](https://docs.rs/csv/) crate `Reader`.
- [`ZeroCopyReader`], [`ZeroCopyReaderBuilder`]: a streaming zero-copy reader that only find cell
  delimiters and does not unescape quoted data.
- [`Splitter`], [`SplitterBuilder`]: a streaming zero-copy splitter that will only
  find record delimitations, but not cell delimiters at all.
- [`LineReader`]: a streaming zero-copy line splitter that does not handle quoting at all.

You can also find more exotic readers like:

- [`TotalReader`], [`TotalReaderBuilder`]: a reader optimized to work with uses-cases when
  CSV data is fully loaded into memory or with memory maps.
- [`Seeker`], [`SeekerBuilder`]: a reader able to find record start positions in a seekable CSV stream.
  This can be very useful for parallelization, or more creative uses like performing binary
  search in a sorted file.
- [`ReverseReader`], [`ReaderBuilder`]: a reader able to read a seekable CSV stream in reverse, in amortized linear time.

# Writers

- [`Writer`], [`WriterBuilder`]: a typical CSV writer.

# Supported targets

- On `x86_64` targets, `sse2` instructions are used. `avx2` instructions
  will also be used if their availability is detected at runtime.
- On `aarch64` targets, `neon` instructions are used.
- On `wasm` targets, `simd128` instructions are used.
- Everywhere else, the library will fallback to [SWAR](https://en.wikipedia.org/wiki/SWAR)
  techniques or scalar implementations.

Using `RUSTFLAGS='-C target-cpu=native'` should not be required when compiling
this crate because it either uses SIMD instructions tied to your `target_arch`
already and because it will rely on runtime detection to find better SIMD
instructions (typically `avx2`).

# Design notes

## Regarding performance

This crate's CSV parser has been cautiously designed to offer "reasonable" performance
by combinining traditional state machine logic with SIMD-accelerated string searching.

I say "reasonable" because you cannot expect to parse 16/32 times faster than a state-of-the-art
scalar implementation like the [`csv`](https://docs.rs/csv/) crate. What's more, the
throughput of the SIMD-accelerated parser remains very data-dependent. Sometimes
you will go up to ~8 times faster, sometimes you will only go as fast as scalar code.
(Remember also that CSV parsing is often an IO-bound task, even more so than with other
data formats expected to fit into memory like JSON etc.)

As a rule of thumb, the larger your records and cells, the greater the
performance boost vs. a scalar byte-by-byte implementation will be. This also means that
for worst cases, this crate's parser will just be on par with scalar code. I
have made everything in my power to ensure this SIMD parser is never slower (I think
one of the reasons why SIMD CSV parsers are not yet very prevalent is that they
tend to suffer real-life cases where scalar code outperform them).

Also, note that this crate is geared towards parsing **streams** of CSV data
only quoted when needed (e.g. not written with a `QUOTE_ALWAYS` policy).

## Regarding simdjson techniques

I have tried very hard to apply [simdjson](https://arxiv.org/abs/1902.08318) tricks
to make this crate's parser as branchless as possible but I couldn't make it as
fast as the state-machine/SIMD string searching hybrid.

`PCLMULQDQ` & shuffling tricks in this context only add more complexity and overhead
to the SIMD sections of the code, all while making it less "democratic" since you need
specific SIMD instructions that are not available everywhere, if you don't want to
fallback to scalar instructions.

Said differently, those techniques seem overkill in practice for CSV parsing.
But it is also possible I am not competent enough to make them work properly and
I won't hesitate to move towards them if proven wrong.

## Hybrid design

This crate's CSV parser follows a hybrid approach where we maintain a traditional
state machine, but search for structural characters in the byte stream using
SIMD string searching techniques like the ones implemented in the excellent
[`memchr`](https://docs.rs/memchr/latest/memchr/) crate:

The idea is to compare 16/32 bytes of data at once with splats of structural
characters like `\n`, `"` or `,` in order to extract a "move mask" that will
be handled as a bit string so we can find whether and where some character
was found using typical bit-twiddling.

This ultimately means that branching happens on each structural characters rather
than on each byte, which is very good. But this is also the reason why CSV data
with a very high density of structural characters will not get parsed much faster
than with the equivalent scalar code.

## Two-speed SIMD branches

This crate's CSV parser actually uses two different modes of SIMD string searching:

1. when reading unquoted CSV data, the parser uses an amortized variant of the
   [`memchr`](https://docs.rs/memchr/latest/memchr/) routines where move masks
   containing more than a single match are kept and consumed progressively on subsequent calls,
   instead of restarting a search from the character just next to an earlier match, as the
   `memchr_iter` routine does.
2. when reading quoted CSV data, the parser uses the optmized & unrolled functions
   of the [`memchr`](https://docs.rs/memchr/latest/memchr/) crate directly to find the next
   quote as fast as possible.

This might seem weird but this seems to be the best tradeoff for performance. Counter-intuitively,
using larger SIMD registers like `avx2` for 1. actually hurts the overall performance.
Similarly, using the amortized routine to scan quoted data is actually slower than
using the unrolled functions of [`memchr`](https://docs.rs/memchr/latest/memchr/).

This actually makes sense if you consider that the longer a field is, the more
probable it is to contain a character requiring the field to be quoted. What's more
the density of quotes to be found in a quoted field is usually lower that structural
characters in an unquoted CSV stream. So if you use larger SIMD registers in the
unquoted stream you will end up 1. throttling the SIMD part of the code too much
because of the inner branching (when hitting a delimiter or a newline) and 2. you
will often discard too much work when hitting a record end or a quoted field.

## Copy amortization

Copying tiny amounts of data often is quite detrimental to the overall performance.
As such, and to make sure the copying [`Reader`] remains as fast as possible,
I decided to change the design of the [`ByteRecord`] to save fields as fully-fledged
ranges over the underlying byte slice instead of only delimiting them implicitly
by the offsets separating them as it is done in the [`csv`](https://docs.rs/csv/) crate.

This means I am able to copy large swathes of unquoted data at once instead of
copying fields one by one. This also means I keep delimiter characters and sometimes
inconsequential double quotes in the underlying byte slice (but don't worry, the
user will never actually see them), so that copies remain as vectorized as possible.

# Caveats

## "Nonsensical" CSV data

To remain as fast as possible, "nonsensical" CSV data is handled by this
crate differently than it might traditionally be done.

For instance, this crate's CSV parser has no concept of "beginning of field",
which means opening quotes in the middle of a field might corrupt the output.
(I would say this is immoral to do so in the first place but traditional parsers
tend to deal with this case more graciously).

For instance, given the following CSV data:

```txt
name,surname\njoh"n,landis\nbéatrice,babka
```

Cautious parsers would produce the following result:

| name     | surname |
| -------- | ------- |
| joh"n    | landis  |
| béatrice | babka   |

While this crate's parser would produce the following unaligned result:

| name                         | surname |
| ---------------------------- | ------- |
| joh"n,landis\nbéatrice,babka | \<eof\> |

Keep also in mind that fields opening and closing quotes multiple
times might lose some characters here & there (especially whitespace) because
the parser's state machine is not geared towards this at all.

Rest assured that morally valid & sensical CSV data will still be parsed
correctly ;)

## Regarding line terminators

To avoid needless branching and SIMD overhead, this crate's CSV parser
expect line terminators to be either CRLF or single LF, but not single CR.

Also, to avoid state machine overhead related to CRLF at buffer boundaries
when streaming and to make sure we skip empty lines of the file (we
don't parse them as empty records),
one edge case has been deemed an acceptable loss: leading CR characters
will be trimmed from the beginning of records.

For instance, given the following CSV data:

```txt
name,surname\n\rjohn,landis\r\nbéatrice,babka
```

A morally correct parser recognizing CRLF or LF line terminators should return:

| name     | surname |
| -------- | ------- |
| \rjohn   | landis  |
| béatrice | babka   |

While the hereby crate returns:

| name     | surname |
| -------- | ------- |
| john     | landis  |
| béatrice | babka   |

*/
#[allow(unused_macros)]
macro_rules! brec {
    () => {{
        $crate::records::ByteRecord::new()
    }};

    ($($x: expr),*) => {{
        let mut r = $crate::records::ByteRecord::new();

        $(
            r.push_field($x.as_bytes());
        )*

        r
    }};
}

mod buffer;
mod core;
mod debug;
mod error;
mod ext;
mod line_reader;
mod reader;
mod records;
mod searcher;
mod seeker;
mod splitter;
mod total_reader;
mod utils;
mod writer;
mod zero_copy_reader;

pub use error::{Error, ErrorKind, Result};
pub use line_reader::LineReader;
pub use reader::{Reader, ReaderBuilder, ReverseReader};
pub use records::{ByteRecord, ZeroCopyByteRecord};
pub use searcher::Searcher;
pub use seeker::{Seeker, SeekerBuilder};
pub use splitter::{Splitter, SplitterBuilder};
pub use total_reader::{TotalReader, TotalReaderBuilder};
pub use utils::unescape;
pub use writer::{Writer, WriterBuilder};
pub use zero_copy_reader::{ZeroCopyReader, ZeroCopyReaderBuilder};
