use memchr::{memchr, memchr2};

use crate::records::ByteRecordBuilder;
use crate::searcher::Searcher;

#[derive(Debug, Clone, Copy)]
pub enum ReadResult {
    InputEmpty,
    Skip,
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
    pub(crate) delimiter: u8,
    pub(crate) quote: u8,
    pub(crate) comment: Option<u8>,
    state: ReadState,
    record_was_read: bool,
    in_comment: bool,
    searcher: Searcher,
}

impl CoreReader {
    pub(crate) fn new(delimiter: u8, quote: u8, comment: Option<u8>) -> Self {
        Self {
            delimiter,
            quote,
            comment,
            state: ReadState::Unquoted,
            // Must be true at the beginning to avoid counting one record for empty input
            record_was_read: true,
            in_comment: false,
            searcher: Searcher::new(delimiter, b'\n', quote),
        }
    }

    pub(crate) fn split_record(&mut self, input: &[u8]) -> (ReadResult, usize) {
        use ReadState::*;

        let input_len = input.len();

        if input_len == 0 {
            if !self.record_was_read {
                self.record_was_read = true;
                return (ReadResult::Record, 0);
            }

            return (ReadResult::End, 0);
        }

        if self.record_was_read && (input[0] == b'\n' || input[0] == b'\r') {
            return (ReadResult::Skip, 1);
        }

        self.record_was_read = false;

        let mut pos: usize = 0;

        while pos < input_len {
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
                    } else if byte == b'\r' && pos + 1 < input_len && input[pos + 1] == b'\n' {
                        self.record_was_read = true;
                        self.state = Unquoted;
                        return (ReadResult::Record, pos + 1);
                    } else {
                        self.state = Unquoted;
                    }
                }
            }
        }

        (ReadResult::InputEmpty, input_len)
    }

    pub(crate) fn split_record_and_find_separators(
        &mut self,
        input: &[u8],
        seps_offset: usize,
        seps: &mut Vec<usize>,
    ) -> (ReadResult, usize) {
        use ReadState::*;

        let input_len = input.len();

        if input_len == 0 {
            if !self.record_was_read {
                self.record_was_read = true;
                return (ReadResult::Record, 0);
            }

            return (ReadResult::End, 0);
        }

        if self.record_was_read && (input[0] == b'\n' || input[0] == b'\r') {
            return (ReadResult::Skip, 1);
        }

        self.record_was_read = false;

        let mut pos: usize = 0;

        while pos < input_len {
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
                    } else if byte == b'\r' && pos + 1 < input_len && input[pos + 1] == b'\n' {
                        self.record_was_read = true;
                        self.state = Unquoted;
                        return (ReadResult::Record, pos + 1);
                    } else {
                        self.state = Unquoted;
                    }
                }
            }
        }

        (ReadResult::InputEmpty, input_len)
    }

    // pub(crate) fn split_record_and_find_separators_alt(
    //     &mut self,
    //     input: &[u8],
    //     seps_offset: usize,
    //     seps: &mut Vec<usize>,
    // ) -> (ReadResult, usize) {
    //     use ReadState::*;

    //     if input.is_empty() {
    //         if !self.record_was_read {
    //             self.record_was_read = true;
    //             return (ReadResult::Record, 0);
    //         }

    //         return (ReadResult::End, 0);
    //     }

    //     if self.record_was_read {
    //         if input[0] == b'\n' {
    //             return (ReadResult::Skip, 1);
    //         } else if input[0] == b'\r' {
    //             return (ReadResult::Skip, 1);
    //         }
    //     }

    //     self.record_was_read = false;

    //     for offset in self.searcher.search(input) {
    //         let byte = input[offset];

    //         match self.state {
    //             Unquoted => {
    //                 if byte == self.delimiter {
    //                     seps.push(seps_offset + offset);
    //                 } else if byte == self.quote {
    //                     self.state = Quoted;
    //                 } else {
    //                     self.record_was_read = true;
    //                     return (ReadResult::Record, offset);
    //                 }
    //             }
    //             Quoted => {
    //                 if byte == self.quote {
    //                     self.state = Quote;
    //                 }
    //             }
    //             Quote => {
    //                 if byte == self.quote {
    //                     self.state = Quoted;
    //                 } else if byte == self.delimiter {
    //                     seps.push(seps_offset + offset);
    //                     self.state = Unquoted;
    //                 } else {
    //                     self.record_was_read = true;
    //                     self.state = Unquoted;
    //                     return (ReadResult::Record, offset);
    //                 }
    //             }
    //         }
    //     }

    //     (ReadResult::InputEmpty, input.len())
    // }

    pub(crate) fn read_record(
        &mut self,
        input: &[u8],
        record_builder: &mut ByteRecordBuilder,
    ) -> (ReadResult, usize) {
        use ReadState::*;

        let input_len = input.len();

        if input_len == 0 {
            if !self.record_was_read && !self.in_comment {
                self.record_was_read = true;

                // NOTE: this is required to handle streams not ending with a newline
                record_builder.finalize_record();
                return (ReadResult::Record, 0);
            }

            return (ReadResult::End, 0);
        }

        if self.record_was_read {
            let first_byte = input[0];

            if first_byte == b'\n' || first_byte == b'\r' {
                self.in_comment = false;
                return (ReadResult::Skip, 1);
            }

            // Comments
            if let Some(comment) = self.comment {
                if self.in_comment || first_byte == comment {
                    let offset = if let Some(o) = memchr(b'\n', &input[1..]) {
                        self.in_comment = false;
                        o + 1
                    } else {
                        self.in_comment = true;
                        input_len
                    };

                    return (ReadResult::Skip, offset);
                }
            }
        }

        self.record_was_read = false;

        let mut pos: usize = 0;

        while pos < input_len {
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
                            record_builder.extend_from_slice(&input[pos..pos + offset]);
                            record_builder.finalize_record();
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

                    if byte == self.quote {
                        self.state = Quoted;
                        record_builder.push_byte(byte);
                        pos += 1;
                    } else if byte == self.delimiter {
                        record_builder.finalize_field();
                        pos += 1;
                        self.state = Unquoted;
                    } else if byte == b'\n' {
                        self.record_was_read = true;
                        self.state = Unquoted;
                        record_builder.finalize_field();
                        return (ReadResult::Record, pos + 1);
                    } else if byte == b'\r' && pos + 2 < input_len && input[pos + 2] == b'\n' {
                        self.record_was_read = true;
                        self.state = Unquoted;
                        record_builder.finalize_field();
                        return (ReadResult::Record, pos + 2);
                    } else {
                        self.state = Unquoted;
                    }
                }
            }
        }

        record_builder.extend_from_slice(&input[pos..]);

        (ReadResult::InputEmpty, input_len)
    }
}
