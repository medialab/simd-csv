use memchr::{memchr, memchr2};

use crate::records::ByteRecordBuilder;
use crate::searcher::Searcher;

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
    FieldStart,
    Unquoted,
    Quoted,
    Quote,
}

// NOTE: funnily enough, knowing the delimiter is not required to split the records,
// but since we expose a single unified `struct` here, it is simpler to include it.
// NOTE: I tried to remove the `record_was_read` and to keep this information
// as a new `ReadState` variant, but it resulted in slightly worse codegen.
#[derive(Debug)]
pub(crate) struct CoreReader {
    pub(crate) delimiter: u8,
    pub(crate) quote: u8,
    state: ReadState,
    record_was_read: bool,
    searcher: Searcher,
}

impl CoreReader {
    pub(crate) fn new(delimiter: u8, quote: u8) -> Self {
        Self {
            delimiter,
            quote,
            state: ReadState::FieldStart,
            // Must be true at the beginning to avoid counting one record for empty input
            record_was_read: true,
            searcher: Searcher::new(delimiter, b'\n'),
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

        if self.record_was_read {
            if input[0] == b'\n' {
                return (ReadResult::Lf, 1);
            } else if input[0] == b'\r' {
                return (ReadResult::Cr, 1);
            }
        }

        self.record_was_read = false;

        let mut pos: usize = 0;

        while pos < input_len {
            match self.state {
                FieldStart => {
                    if input[pos] == self.quote {
                        self.state = Quoted;
                        pos += 1;
                    } else {
                        self.state = Unquoted;
                    }
                }
                Unquoted => {
                    // Here we are moving to next quote or end of line
                    if let Some(offset) = memchr2(b'\n', self.quote, &input[pos..]) {
                        pos += offset;

                        let byte = input[pos];

                        pos += 1;

                        if byte == b'\n' {
                            self.record_was_read = true;
                            self.state = FieldStart;
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
                        self.state = FieldStart;
                        return (ReadResult::Record, pos);
                    } else if byte == b'\r' && pos + 1 < input_len && input[pos + 1] == b'\n' {
                        self.record_was_read = true;
                        self.state = FieldStart;
                        return (ReadResult::Record, pos + 1);
                    } else {
                        self.state = FieldStart;
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

        if self.record_was_read {
            if input[0] == b'\n' {
                return (ReadResult::Lf, 1);
            } else if input[0] == b'\r' {
                return (ReadResult::Cr, 1);
            }
        }

        self.record_was_read = false;

        let mut pos: usize = 0;

        while pos < input_len {
            match self.state {
                FieldStart => {
                    if input[pos] == self.quote {
                        self.state = Quoted;
                        pos += 1;
                    } else {
                        self.state = Unquoted;
                    }
                }
                Unquoted => {
                    // Here we are moving to next delimiter or end of line

                    let mut last_offset: usize = 0;

                    for offset in self.searcher.search(&input[pos..]) {
                        last_offset = offset + 1;

                        let byte = input[pos + offset];

                        if byte == self.delimiter {
                            seps.push(seps_offset + pos + offset);

                            let next_pos = pos + last_offset;

                            if next_pos < input_len && input[next_pos] == self.quote {
                                self.state = Quoted;
                                last_offset += 1;
                                break;
                            }

                            if next_pos == input_len {
                                self.state = FieldStart;
                                break;
                            }

                            continue;
                        }

                        self.record_was_read = true;
                        self.state = FieldStart;
                        return (ReadResult::Record, pos + last_offset);
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
                        self.state = FieldStart;
                    } else if byte == b'\n' {
                        self.record_was_read = true;
                        self.state = FieldStart;
                        return (ReadResult::Record, pos);
                    } else if byte == b'\r' && pos + 1 < input_len && input[pos + 1] == b'\n' {
                        self.record_was_read = true;
                        self.state = FieldStart;
                        return (ReadResult::Record, pos + 1);
                    } else {
                        self.state = Unquoted;
                    }
                }
            }
        }

        (ReadResult::InputEmpty, input_len)
    }

    // NOTE: this variant uses less branching and processes the comma mask separately,
    // before considering other structural chars like quotes and newlines. It is
    // supposedly faster, but in practice, for the copying variant, it is detrimental
    // or moot. Next step would be to test a two-step process involving processing a
    // large chunk of bytes, branchlessly, to find structural chars, then applying
    // the branching state-machine over found chars.

    // pub(crate) fn split_record_and_find_separators(
    //     &mut self,
    //     input: &[u8],
    //     seps_offset: usize,
    //     seps: &mut Vec<usize>,
    // ) -> (ReadResult, usize) {
    //     use ReadState::*;

    //     let input_len = input.len();

    //     if input_len == 0 {
    //         if !self.record_was_read {
    //             self.record_was_read = true;
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

    //     let mut pos: usize = 0;

    //     while pos < input_len {
    //         match self.state {
    //             Unquoted => {
    //                 // Fast path for quoted field start
    //                 if input[pos] == self.quote {
    //                     self.state = Quoted;
    //                     pos += 1;
    //                     continue;
    //                 }

    //                 // Here we are moving to next quote or end of line
    //                 while pos < input_len {
    //                     let (read, (delimiter_mask, structural_mask)) =
    //                         self.searcher.process_chunk(&input[pos..]);

    //                     for offset in delimiter_mask.into_iter() {
    //                         seps.push(seps_offset + pos + offset);
    //                     }

    //                     if let Some(offset) = structural_mask.into_first_offset() {
    //                         let byte = input[pos + offset];

    //                         if byte == b'\n' {
    //                             self.record_was_read = true;
    //                             return (ReadResult::Record, pos + offset + 1);
    //                         }

    //                         // Here, `byte` is guaranteed to be a quote
    //                         self.state = Quoted;
    //                         pos += offset + 1;
    //                         break;
    //                     } else {
    //                         pos += read;
    //                     }
    //                 }
    //             }
    //             Quoted => {
    //                 // Here we moving to next quote
    //                 if let Some(offset) = memchr(self.quote, &input[pos..]) {
    //                     pos += offset + 1;
    //                     self.state = Quote;
    //                 } else {
    //                     break;
    //                 }
    //             }
    //             Quote => {
    //                 let byte = input[pos];

    //                 pos += 1;

    //                 if byte == self.quote {
    //                     self.state = Quoted;
    //                 } else if byte == self.delimiter {
    //                     seps.push(seps_offset + pos - 1);
    //                     self.state = Unquoted;
    //                 } else if byte == b'\n' {
    //                     self.record_was_read = true;
    //                     self.state = Unquoted;
    //                     return (ReadResult::Record, pos);
    //                 } else if byte == b'\r' && pos + 1 < input_len && input[pos + 1] == b'\n' {
    //                     self.record_was_read = true;
    //                     self.state = Unquoted;
    //                     return (ReadResult::Record, pos + 1);
    //                 } else {
    //                     self.state = Unquoted;
    //                 }
    //             }
    //         }
    //     }

    //     (ReadResult::InputEmpty, input_len)
    // }

    // NOTE: this variant used to perform a single iterated search to do all the
    // work, without changing speed, and wrapping the state machine inside the
    // search breakpoints, i.e. when hitting a structural char.

    // pub(crate) fn split_record_and_find_separators(
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
    //             return (ReadResult::Lf, 1);
    //         } else if input[0] == b'\r' {
    //             return (ReadResult::Cr, 1);
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
            if !self.record_was_read {
                self.record_was_read = true;

                // NOTE: this is required to handle streams not ending with a newline
                record_builder.finalize_record();
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

        while pos < input_len {
            match self.state {
                FieldStart => {
                    if input[pos] == self.quote {
                        self.state = Quoted;
                        pos += 1;
                    } else {
                        self.state = Unquoted;
                    }
                }
                Unquoted => {
                    // Here we are moving to next delimiter or end of line

                    let mut scan_pos = pos;
                    let mut mask: u64 = 0;
                    let mut mask_start = 0;
                    let mut scalar_tail = false;

                    loop {
                        let event_pos;

                        if mask != 0 {
                            let offset = mask.trailing_zeros() as usize;
                            mask &= mask - 1;
                            event_pos = mask_start + offset;
                        } else if scalar_tail {
                            if let Some(offset) = memchr2(self.delimiter, b'\n', &input[scan_pos..])
                            {
                                event_pos = scan_pos + offset;
                                scan_pos = event_pos + 1;
                            } else {
                                record_builder.extend_from_slice(&input[pos..]);
                                pos = input_len;
                                break;
                            }
                        } else {
                            let block = self.searcher.refill_mask(&input[scan_pos..]);
                            scan_pos += block.scanned;

                            if block.mask != 0 {
                                mask = block.mask;
                                mask_start = scan_pos - block.width;
                                continue;
                            }

                            scalar_tail = true;
                            continue;
                        }

                        let byte = input[event_pos];

                        // NOTE: we don't copy here yet to avoid slowing down
                        // because of multiple tiny copies.
                        if byte == self.delimiter {
                            record_builder.finalize_field_preemptively(event_pos - pos);

                            let next_pos = event_pos + 1;

                            if next_pos < input_len && input[next_pos] == self.quote {
                                self.state = Quoted;
                                record_builder.bump();
                                record_builder.extend_from_slice(&input[pos..next_pos + 1]);
                                pos = next_pos + 1;
                                break;
                            }

                            if next_pos == input_len {
                                self.state = FieldStart;
                                record_builder.extend_from_slice(&input[pos..next_pos]);
                                pos = next_pos;
                                break;
                            }

                            continue;
                        }

                        record_builder.extend_from_slice(&input[pos..event_pos]);
                        record_builder.finalize_record();
                        self.record_was_read = true;
                        self.state = FieldStart;
                        return (ReadResult::Record, event_pos + 1);
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
                        self.state = FieldStart;
                    } else if byte == b'\n' {
                        self.record_was_read = true;
                        self.state = FieldStart;
                        record_builder.finalize_field();
                        return (ReadResult::Record, pos + 1);
                    } else if byte == b'\r' && pos + 1 < input_len && input[pos + 1] == b'\n' {
                        self.record_was_read = true;
                        self.state = FieldStart;
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
