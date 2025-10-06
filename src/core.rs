use memchr::{memchr, memchr2};

use crate::records::ByteRecordBuilder;
use crate::searcher::Searcher;
use crate::utils::trim_trailing_cr;

#[derive(Debug, Clone, Copy)]
pub enum ReadResult {
    InputEmpty,
    Cr,
    Lf,
    Record,
    End,
}

#[derive(Debug, Clone, Copy)]
enum ReadState {
    Unquoted,
    Quoted,
    Quote,
}

// NOTE: funnily enough, knowing the delimiter is not required to split the records,
// but since we expose a single unified `struct` here, it is simpler to include it.
pub(crate) struct CoreReader {
    delimiter: u8,
    quote: u8,
    state: ReadState,
    record_was_read: bool,
    searcher: Searcher,
}

impl CoreReader {
    pub(crate) fn new(delimiter: u8, quote: u8) -> Self {
        Self {
            delimiter,
            quote,
            state: ReadState::Unquoted,
            // Must be true at the beginning to avoid counting one record for empty input
            record_was_read: true,
            searcher: Searcher::new(delimiter, b'\n', quote),
        }
    }

    pub(crate) fn split_record(&mut self, input: &[u8]) -> (ReadResult, usize) {
        use ReadState::*;

        if input.is_empty() {
            if !self.record_was_read {
                self.record_was_read = true;
                return (ReadResult::Record, 0);
            }

            return (ReadResult::End, 0);
        }

        if self.record_was_read {
            if input[0] == b'\n' {
                return (ReadResult::Lf, 1);
            } else if input[0] == b'\r' {
                return (ReadResult::Cr, 1);
            }
        }

        self.record_was_read = false;

        let mut pos: usize = 0;

        while pos < input.len() {
            match self.state {
                Unquoted => {
                    // Fast path for quoted field start
                    if input[pos] == self.quote {
                        self.state = Quoted;
                        pos += 1;
                        continue;
                    }

                    // Here we are moving to next quote or end of line
                    if let Some(offset) = memchr2(b'\n', self.quote, &input[pos..]) {
                        pos += offset;

                        let byte = input[pos];

                        pos += 1;

                        if byte == b'\n' {
                            self.record_was_read = true;
                            return (ReadResult::Record, pos);
                        }

                        // Here, `byte` is guaranteed to be a quote
                        self.state = Quoted;
                    } else {
                        break;
                    }
                }
                Quoted => {
                    // Here we moving to next quote
                    if let Some(offset) = memchr(self.quote, &input[pos..]) {
                        pos += offset + 1;
                        self.state = Quote;
                    } else {
                        break;
                    }
                }
                Quote => {
                    let byte = input[pos];

                    pos += 1;

                    if byte == self.quote {
                        self.state = Quoted;
                    } else if byte == b'\n' {
                        self.record_was_read = true;
                        self.state = Unquoted;
                        return (ReadResult::Record, pos);
                    } else {
                        self.state = Unquoted;
                    }
                }
            }
        }

        (ReadResult::InputEmpty, input.len())
    }

    pub(crate) fn split_record_and_find_separators(
        &mut self,
        input: &[u8],
        seps_offset: usize,
        seps: &mut Vec<usize>,
    ) -> (ReadResult, usize) {
        use ReadState::*;

        if input.is_empty() {
            if !self.record_was_read {
                self.record_was_read = true;
                return (ReadResult::Record, 0);
            }

            return (ReadResult::End, 0);
        }

        if self.record_was_read {
            if input[0] == b'\n' {
                return (ReadResult::Lf, 1);
            } else if input[0] == b'\r' {
                return (ReadResult::Cr, 1);
            }
        }

        self.record_was_read = false;

        let mut pos: usize = 0;

        while pos < input.len() {
            match self.state {
                Unquoted => {
                    // Fast path for quoted field start
                    if input[pos] == self.quote {
                        self.state = Quoted;
                        pos += 1;
                        continue;
                    }

                    // Here we are moving to next quote or end of line
                    let mut last_offset: usize = 0;

                    for offset in self.searcher.search(&input[pos..]) {
                        last_offset = offset + 1;

                        let byte = input[pos + offset];

                        if byte == self.delimiter {
                            seps.push(seps_offset + pos + offset);
                            continue;
                        }

                        if byte == b'\n' {
                            self.record_was_read = true;
                            return (ReadResult::Record, pos + last_offset);
                        }

                        // Here, `byte` is guaranteed to be a quote
                        self.state = Quoted;
                        break;
                    }

                    if last_offset > 0 {
                        pos += last_offset;
                    } else {
                        break;
                    }
                }
                Quoted => {
                    // Here we moving to next quote
                    if let Some(offset) = memchr(self.quote, &input[pos..]) {
                        pos += offset + 1;
                        self.state = Quote;
                    } else {
                        break;
                    }
                }
                Quote => {
                    let byte = input[pos];

                    pos += 1;

                    if byte == self.quote {
                        self.state = Quoted;
                    } else if byte == self.delimiter {
                        seps.push(seps_offset + pos - 1);
                        self.state = Unquoted;
                    } else if byte == b'\n' {
                        self.record_was_read = true;
                        self.state = Unquoted;
                        return (ReadResult::Record, pos);
                    } else {
                        self.state = Unquoted;
                    }
                }
            }
        }

        (ReadResult::InputEmpty, input.len())
    }

    #[allow(dead_code)]
    pub(crate) fn split_record_and_find_separators_alt(
        &mut self,
        input: &[u8],
        seps_offset: usize,
        seps: &mut Vec<usize>,
    ) -> (ReadResult, usize) {
        use ReadState::*;

        if input.is_empty() {
            if !self.record_was_read {
                self.record_was_read = true;
                return (ReadResult::Record, 0);
            }

            return (ReadResult::End, 0);
        }

        if self.record_was_read {
            if input[0] == b'\n' {
                return (ReadResult::Lf, 1);
            } else if input[0] == b'\r' {
                return (ReadResult::Cr, 1);
            }
        }

        self.record_was_read = false;

        for offset in self.searcher.search(input) {
            let byte = input[offset];

            match self.state {
                Unquoted => {
                    if byte == self.delimiter {
                        seps.push(seps_offset + offset);
                    } else if byte == self.quote {
                        self.state = Quoted;
                    } else {
                        self.record_was_read = true;
                        return (ReadResult::Record, offset);
                    }
                }
                Quoted => {
                    if byte == self.quote {
                        self.state = Quote;
                    }
                }
                Quote => {
                    if byte == self.quote {
                        self.state = Quoted;
                    } else if byte == self.delimiter {
                        seps.push(seps_offset + offset);
                        self.state = Unquoted;
                    } else {
                        self.record_was_read = true;
                        self.state = Unquoted;
                        return (ReadResult::Record, offset);
                    }
                }
            }
        }

        (ReadResult::InputEmpty, input.len())
    }

    pub(crate) fn read_record(
        &mut self,
        input: &[u8],
        record_builder: &mut ByteRecordBuilder,
    ) -> (ReadResult, usize) {
        use ReadState::*;

        if input.is_empty() {
            if !self.record_was_read {
                self.record_was_read = true;

                // NOTE: this is required to handle streams not ending with a newline
                record_builder.finalize_field();
                return (ReadResult::Record, 0);
            }

            return (ReadResult::End, 0);
        }

        if self.record_was_read {
            if input[0] == b'\n' {
                return (ReadResult::Lf, 1);
            } else if input[0] == b'\r' {
                return (ReadResult::Cr, 1);
            }
        }

        self.record_was_read = false;

        let mut pos: usize = 0;

        while pos < input.len() {
            match self.state {
                Unquoted => {
                    // Fast path for quoted field start
                    if input[pos] == self.quote {
                        self.state = Quoted;
                        pos += 1;
                        continue;
                    }

                    // Here we are moving to next quote or end of line
                    let mut last_offset: usize = 0;

                    for offset in self.searcher.search(&input[pos..]) {
                        last_offset = offset + 1;

                        let byte = input[pos + offset];

                        // NOTE: we don't copy here yet to avoid slowing down
                        // because of multiple tiny copies.
                        if byte == self.delimiter {
                            record_builder.finalize_field_preemptively(offset);
                            continue;
                        }

                        if byte == b'\n' {
                            record_builder
                                .extend_from_slice(trim_trailing_cr(&input[pos..pos + offset]));
                            record_builder.finalize_field();
                            self.record_was_read = true;
                            return (ReadResult::Record, pos + last_offset);
                        }

                        // Here, `byte` is guaranteed to be a quote
                        self.state = Quoted;
                        record_builder.bump();
                        break;
                    }

                    if last_offset > 0 {
                        record_builder.extend_from_slice(&input[pos..pos + last_offset]);
                        pos += last_offset
                    } else {
                        break;
                    }
                }
                Quoted => {
                    // Here we moving to next quote
                    if let Some(offset) = memchr(self.quote, &input[pos..]) {
                        record_builder.extend_from_slice(&input[pos..pos + offset]);
                        pos += offset + 1;
                        self.state = Quote;
                    } else {
                        break;
                    }
                }
                Quote => {
                    let byte = input[pos];

                    pos += 1;

                    if byte == self.quote {
                        self.state = Quoted;
                        record_builder.push_byte(byte);
                    } else if byte == self.delimiter {
                        record_builder.finalize_field();
                        self.state = Unquoted;
                    } else if byte == b'\n' {
                        self.record_was_read = true;
                        self.state = Unquoted;
                        record_builder.finalize_field();
                        return (ReadResult::Record, pos);
                    } else {
                        self.state = Unquoted;
                    }
                }
            }
        }

        record_builder.extend_from_slice(&input[pos..]);

        (ReadResult::InputEmpty, input.len())
    }

    // NOTE: this version of the method wraps the state machine logic within the
    // SIMD iteration logic. Ironically it seems slower than the multiple-speed
    // stop-and-go implementation above.
    // Be advised that this code does not handle final \r correctly yet.
    // fn read_record(&mut self, input: &[u8], record: &mut ByteRecord) -> (ReadResult, usize) {
    //     use ReadState::*;

    //     if input.is_empty() {
    //         if !self.record_was_read {
    //             self.record_was_read = true;
    //             record.finalize_field();
    //             return (ReadResult::Record, 0);
    //         }

    //         return (ReadResult::End, 0);
    //     }

    //     if self.record_was_read {
    //         if input[0] == b'\n' {
    //             return (ReadResult::Lf, 1);
    //         } else if input[0] == b'\r' {
    //             return (ReadResult::Cr, 1);
    //         }
    //     }

    //     self.record_was_read = false;

    //     let mut last_offset: Option<usize> = None;
    //     let mut start: usize;

    //     for offset in self.searcher.search(input) {
    //         let byte = input[offset];

    //         if let Quote = self.state {
    //             if byte == self.quote {
    //                 let was_previously_a_quote = match last_offset {
    //                     None => true,
    //                     Some(o) => o == offset - 1,
    //                 };

    //                 if was_previously_a_quote {
    //                     self.state = Quoted;
    //                     continue;
    //                 } else {
    //                     self.state = Unquoted;
    //                 }
    //             } else {
    //                 self.state = Unquoted;
    //             }
    //         }

    //         start = last_offset.map(|o| o + 1).unwrap_or(0);
    //         last_offset = Some(offset);

    //         match self.state {
    //             Unquoted => {
    //                 record.extend_from_slice(&input[start..offset]);

    //                 last_offset = Some(offset);

    //                 if byte == self.delimiter {
    //                     record.finalize_field();
    //                     continue;
    //                 }

    //                 if byte == b'\n' {
    //                     record.finalize_field();
    //                     self.record_was_read = true;
    //                     return (ReadResult::Record, offset + 1);
    //                 }

    //                 // Here, `byte` is guaranteed to be a quote
    //                 self.state = Quoted;
    //             }
    //             Quoted => {
    //                 record.extend_from_slice(&input[start..offset]);

    //                 if byte != self.quote {
    //                     record.push_byte(byte);
    //                     continue;
    //                 }

    //                 self.state = Quote;
    //             }
    //             _ => unreachable!(),
    //         }
    //     }

    //     start = last_offset.map(|o| o + 1).unwrap_or(0);
    //     record.extend_from_slice(&input[start..]);

    //     (ReadResult::InputEmpty, input.len())
    // }
}
