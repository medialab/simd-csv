/// This module contains the parsing logic of a tiny column selection DSL.
///
/// It comes directly from [`xan`](https://github.com/medialab/xan/) and was
/// originally formulated and implemented by @BurntSushi for
/// [`xsv`](https://github.com/burntsushi/xsv).
///
/// `xan` changed multiple things from the original `xsv` implementation:
///     - indexation is now zero-based
///     - range char was changed to `:` instead of `-`
///     - added negative indexing
///     - added wildcard selection
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;

use super::selection::Selection;

/// A parsed selector that can be applied on CSV headers to create a
/// [`crate::Selection`].
#[derive(Clone)]
pub struct Selector {
    selectors: Vec<CompositeSelector>,
    invert: bool,
}

impl FromStr for Selector {
    type Err = String;

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        let invert = if !s.is_empty() && s.as_bytes()[0] == b'!' {
            s = &s[1..];
            true
        } else {
            false
        };
        Ok(Self {
            selectors: SelectorParser::new(s).parse()?,
            invert,
        })
    }
}

impl Selector {
    pub fn is_empty(&self) -> bool {
        self.selectors.is_empty()
    }

    pub fn invert(&mut self) {
        self.invert = !self.invert;
    }

    pub fn select<'a, H>(&self, first_record: H, use_names: bool) -> Result<Selection, String>
    where
        H: IntoIterator<Item = &'a [u8]>,
    {
        let first_record = first_record.into_iter().collect::<Vec<_>>();

        if self.selectors.is_empty() {
            return Ok(Selection::new(
                if self.invert {
                    // Inverting everything means we get nothing.
                    vec![]
                } else {
                    (0..first_record.len()).collect()
                },
                first_record.len(),
            ));
        }

        let mut map = vec![];
        for sel in &self.selectors {
            let idxs = sel.indices(&first_record, use_names);
            map.extend(idxs?.into_iter());
        }
        if self.invert {
            let mut new_map = vec![];
            for i in 0..first_record.len() {
                if !map.contains(&i) {
                    new_map.push(i);
                }
            }
            return Ok(Selection::new(new_map, first_record.len()));
        }
        Ok(Selection::new(map, first_record.len()))
    }

    pub fn select_one<'a, H>(&self, first_record: H, use_names: bool) -> Result<usize, String>
    where
        H: IntoIterator<Item = &'a [u8]>,
    {
        let selection = self.select(first_record, use_names)?;

        if selection.len() != 1 {
            return Err("target selection is not a single column".to_string());
        }

        Ok(selection[0])
    }

    pub fn retain_known<'a, H>(&mut self, headers: H) -> Vec<usize>
    where
        H: IntoIterator<Item = &'a [u8]>,
    {
        let headers = headers.into_iter().collect::<Vec<_>>();

        let mut dropped: Vec<usize> = Vec::new();

        for (i, selector) in self.selectors.iter().enumerate() {
            match selector {
                CompositeSelector::One(sel) if sel.index(&headers, true).is_err() => {
                    dropped.push(i);
                }
                CompositeSelector::Range(start, end)
                    if start.index(&headers, true).is_err()
                        && end.index(&headers, true).is_err() =>
                {
                    dropped.push(i);
                }
                _ => continue,
            };
        }

        let mut i: usize = 0;

        self.selectors.retain(|_| {
            let drop = !dropped.contains(&i);

            i += 1;

            drop
        });

        dropped
    }
}

impl fmt::Debug for Selector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.selectors.is_empty() {
            write!(f, "<All>")
        } else {
            let strs: Vec<_> = self
                .selectors
                .iter()
                .map(|sel| format!("{:?}", sel))
                .collect();
            write!(f, "{}", strs.join(", "))
        }
    }
}

impl TryFrom<String> for Selector {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl Default for Selector {
    fn default() -> Self {
        "".parse().unwrap()
    }
}

struct SelectorParser {
    chars: Vec<char>,
    pos: usize,
}

impl SelectorParser {
    fn new(s: &str) -> SelectorParser {
        SelectorParser {
            chars: s.chars().collect(),
            pos: 0,
        }
    }

    fn parse(&mut self) -> Result<Vec<CompositeSelector>, String> {
        let mut sels = vec![];
        loop {
            if self.cur().is_none() {
                break;
            }

            let f1: OneSelector = if self.cur() == Some(':') {
                OneSelector::Start
            } else {
                self.parse_one()?
            };

            let f2: Option<OneSelector> = if self.cur() == Some(':') {
                self.bump();

                let sel = if self.is_end_of_selector() {
                    OneSelector::End
                } else {
                    self.parse_one()?
                };

                Some(sel)
            } else {
                None
            };

            if !self.is_end_of_selector() {
                return Err(format!(
                    "Expected end of field but got '{}' instead.",
                    self.cur().unwrap()
                ));
            }

            sels.push(match f2 {
                Some(end) => CompositeSelector::Range(f1, end),
                None => CompositeSelector::One(f1),
            });

            self.bump();
        }

        for sel in sels.iter_mut() {
            sel.refine()?;
        }

        Ok(sels)
    }

    fn parse_one(&mut self) -> Result<OneSelector, String> {
        let mut was_quoted = false;
        let name = if self.cur() == Some('"') {
            was_quoted = true;
            self.bump();
            self.parse_quoted_name()?
        } else {
            self.parse_name()?
        };
        Ok(if self.cur() == Some('[') {
            let idx = self.parse_index()?;
            OneSelector::IndexedName(name, Some(idx), was_quoted)
        } else {
            match name.parse() {
                Err(_) => OneSelector::IndexedName(name, None, was_quoted),
                Ok(idx) => OneSelector::Index(idx),
            }
        })
    }

    fn parse_name(&mut self) -> Result<String, String> {
        let mut name = String::new();
        loop {
            if self.is_end_of_field() || self.cur() == Some('[') {
                break;
            }
            name.push(self.cur().unwrap());
            self.bump();
        }
        Ok(name)
    }

    fn parse_quoted_name(&mut self) -> Result<String, String> {
        let mut name = String::new();
        loop {
            match self.cur() {
                None => {
                    return Err("Unclosed quote, missing closing \".".to_owned());
                }
                Some('"') => {
                    self.bump();
                    if self.cur() == Some('"') {
                        self.bump();
                        name.push('"');
                        name.push('"');
                        continue;
                    }
                    break;
                }
                Some(c) => {
                    name.push(c);
                    self.bump();
                }
            }
        }
        Ok(name)
    }

    fn parse_index(&mut self) -> Result<isize, String> {
        assert_eq!(self.cur().unwrap(), '[');
        self.bump();

        let mut idx = String::new();
        loop {
            match self.cur() {
                None => {
                    return Err("Unclosed index bracket, missing closing ].".to_owned());
                }
                Some(']') => {
                    self.bump();
                    break;
                }
                Some(c) => {
                    idx.push(c);
                    self.bump();
                }
            }
        }

        idx.parse()
            .map_err(|err| format!("Could not convert '{}' to an integer: {}", idx, err))
    }

    fn cur(&self) -> Option<char> {
        self.chars.get(self.pos).cloned()
    }

    fn is_end_of_field(&self) -> bool {
        match self.cur() {
            None => true,
            Some(c) => c == ',' || c == ':',
        }
    }

    fn is_end_of_selector(&self) -> bool {
        match self.cur() {
            None => true,
            Some(c) => c == ',',
        }
    }

    fn bump(&mut self) {
        if self.pos < self.chars.len() {
            self.pos += 1;
        }
    }
}

#[derive(Clone)]
enum CompositeSelector {
    One(OneSelector),
    Range(OneSelector, OneSelector),
    GlobPrefix(String, Option<isize>),
    GlobSuffix(String, Option<isize>),
    GlobInner(String, String, Option<isize>),
    All(Option<isize>),
}

impl CompositeSelector {
    fn refine(&mut self) -> Result<(), String> {
        match self {
            Self::One(OneSelector::IndexedName(name, pos_opt, was_quoted)) => {
                if *was_quoted {
                    return Ok(());
                }

                let star_count = name.chars().filter(|c| *c == '*').count();

                match star_count {
                    0 => Ok(()),
                    1 => {
                        if name == "*" {
                            *self = Self::All(*pos_opt);
                        } else if name.starts_with('*') {
                            *self = Self::GlobSuffix(
                                name.trim_start_matches('*').to_string(),
                                *pos_opt,
                            );
                        } else if name.ends_with('*') {
                            *self =
                                Self::GlobPrefix(name.trim_end_matches('*').to_string(), *pos_opt);
                        } else {
                            let pos = name
                                .char_indices()
                                .find_map(|(i, c)| if c == '*' { Some(i) } else { None })
                                .unwrap();

                            *self = Self::GlobInner(
                                name[..pos].to_string(),
                                name[pos + 1..].to_string(),
                                *pos_opt,
                            );
                        }

                        Ok(())
                    }
                    _ => Err(format!("'{}' contains more than one \"*\" wildcard", name)),
                }
            }
            Self::Range(start, end) => {
                if let OneSelector::IndexedName(name, _, false) = start {
                    if name.contains("*") {
                        return Err(
                            "start of range cannot contain \"*\" wildcard unquoted".to_string()
                        );
                    }
                }

                if let OneSelector::IndexedName(name, _, false) = end {
                    if name.contains("*") {
                        return Err(
                            "end of range cannot contain \"*\" wildcard unquoted".to_string()
                        );
                    }
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone)]
enum OneSelector {
    Start,
    End,
    Index(isize),
    IndexedName(String, Option<isize>, bool),
}

impl CompositeSelector {
    fn indices(&self, first_record: &[&[u8]], use_names: bool) -> Result<Vec<usize>, String> {
        struct Map<'s> {
            inner: BTreeMap<&'s [u8], Vec<usize>>,
        }

        impl<'s> Map<'s> {
            fn new(first_record: &'s [&[u8]]) -> Self {
                let mut map = BTreeMap::new();

                for (i, name) in first_record.iter().enumerate() {
                    let list: &mut Vec<usize> = map.entry(*name).or_default();
                    list.push(i);
                }

                Self { inner: map }
            }

            fn for_each<P, C>(&self, pos: isize, predicate: P, mut callback: C)
            where
                P: Fn(&[u8]) -> bool,
                C: FnMut(usize),
            {
                for (name, indices) in self.inner.iter() {
                    if !predicate(name) {
                        continue;
                    }

                    let pos = if pos < 0 {
                        indices.len() as isize + pos
                    } else {
                        pos
                    };

                    if pos < 0 {
                        continue;
                    }

                    if let Some(i) = indices.get(pos as usize) {
                        callback(*i);
                    }
                }
            }
        }

        match *self {
            CompositeSelector::All(pos_opt) => {
                if let Some(pos) = pos_opt {
                    if !use_names {
                        return Err(format!(
                            "Cannot use '*[{}]' in selection \
                                        with --no-headers set.",
                            pos
                        ));
                    }

                    let mut inds = vec![];
                    let map = Map::new(first_record);

                    map.for_each(pos, |_| true, |i| inds.push(i));

                    if inds.is_empty() {
                        return Err(format!("'*[{}]' selected nothing.", pos));
                    }

                    Ok(inds)
                } else {
                    Ok((0..first_record.len()).collect())
                }
            }
            CompositeSelector::One(ref sel) => sel.index(first_record, use_names).map(|i| vec![i]),
            CompositeSelector::Range(ref sel1, ref sel2) => {
                let i1 = sel1.index(first_record, use_names)?;
                let i2 = sel2.index(first_record, use_names)?;
                Ok(match i1.cmp(&i2) {
                    Ordering::Equal => vec![i1],
                    Ordering::Less => (i1..(i2 + 1)).collect(),
                    Ordering::Greater => {
                        let mut inds = vec![];
                        let mut i = i1 + 1;
                        while i > i2 {
                            i -= 1;
                            inds.push(i);
                        }
                        inds
                    }
                })
            }
            CompositeSelector::GlobPrefix(ref prefix, pos_opt) => {
                if let Some(pos) = pos_opt {
                    if !use_names {
                        return Err(format!(
                            "Cannot use prefix ('{}*[{}]') in selection \
                                        with --no-headers set.",
                            prefix, pos
                        ));
                    }

                    let mut inds = vec![];
                    let map = Map::new(first_record);

                    map.for_each(
                        pos,
                        |name| name.starts_with(prefix.as_bytes()),
                        |i| inds.push(i),
                    );

                    if inds.is_empty() {
                        return Err(format!("Prefix '{}*[{}]' selected nothing.", prefix, pos));
                    }

                    Ok(inds)
                } else {
                    if !use_names {
                        return Err(format!(
                            "Cannot use prefix ('{}*') in selection \
                                        with --no-headers set.",
                            prefix
                        ));
                    }

                    let inds: Vec<usize> = first_record
                        .iter()
                        .enumerate()
                        .filter_map(|(i, h)| {
                            if h.starts_with(prefix.as_bytes()) {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect();

                    if inds.is_empty() {
                        return Err(format!("Prefix '{}*' selected nothing.", prefix));
                    }

                    Ok(inds)
                }
            }
            CompositeSelector::GlobSuffix(ref suffix, pos_opt) => {
                if let Some(pos) = pos_opt {
                    if !use_names {
                        return Err(format!(
                            "Cannot use suffix ('*{}[{}]') in selection \
                                        with --no-headers set.",
                            suffix, pos
                        ));
                    }

                    let mut inds = vec![];
                    let map = Map::new(first_record);

                    map.for_each(
                        pos,
                        |name| name.ends_with(suffix.as_bytes()),
                        |i| inds.push(i),
                    );

                    if inds.is_empty() {
                        return Err(format!("Suffix '*{}[{}]' selected nothing.", suffix, pos));
                    }

                    Ok(inds)
                } else {
                    if !use_names {
                        return Err(format!(
                            "Cannot use suffix ('*{}') in selection \
                                        with --no-headers set.",
                            suffix
                        ));
                    }

                    let inds: Vec<usize> = first_record
                        .iter()
                        .enumerate()
                        .filter_map(|(i, h)| {
                            if h.ends_with(suffix.as_bytes()) {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect();

                    if inds.is_empty() {
                        return Err(format!("Suffix '*{}' selected nothing.", suffix));
                    }

                    Ok(inds)
                }
            }
            CompositeSelector::GlobInner(ref prefix, ref suffix, pos_opt) => {
                if let Some(pos) = pos_opt {
                    if !use_names {
                        return Err(format!(
                            "Cannot use inner wildcard ('{}*{}[{}]') in selection \
                                        with --no-headers set.",
                            prefix, suffix, pos
                        ));
                    }

                    let mut inds = vec![];
                    let map = Map::new(first_record);

                    map.for_each(
                        pos,
                        |name| {
                            name.starts_with(prefix.as_bytes()) && name.ends_with(suffix.as_bytes())
                        },
                        |i| inds.push(i),
                    );

                    if inds.is_empty() {
                        return Err(format!(
                            "Inner wildcard '{}*{}[{}]' selected nothing.",
                            prefix, suffix, pos
                        ));
                    }

                    Ok(inds)
                } else {
                    if !use_names {
                        return Err(format!(
                            "Cannot use inner wildcard ('{}*{}') in selection \
                                        with --no-headers set.",
                            prefix, suffix
                        ));
                    }

                    let inds: Vec<usize> = first_record
                        .iter()
                        .enumerate()
                        .filter_map(|(i, h)| {
                            if h.starts_with(prefix.as_bytes()) && h.ends_with(suffix.as_bytes()) {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect();

                    if inds.is_empty() {
                        return Err(format!(
                            "Inner wildcard '{}*{}' selected nothing.",
                            prefix, suffix
                        ));
                    }

                    Ok(inds)
                }
            }
        }
    }
}

impl OneSelector {
    fn index(&self, first_record: &[&[u8]], use_names: bool) -> Result<usize, String> {
        match *self {
            OneSelector::Start => Ok(0),
            OneSelector::End => Ok(if first_record.is_empty() {
                0
            } else {
                first_record.len() - 1
            }),
            OneSelector::Index(i) => {
                if i < 0 {
                    if i.unsigned_abs() > first_record.len() {
                        Err(format!(
                            "Column index {} is out of \
                                 bounds. Index must be between -1 \
                                 and -{}.",
                            i,
                            first_record.len()
                        ))
                    } else {
                        Ok(first_record.len() - i.unsigned_abs())
                    }
                } else {
                    let i = i as usize;
                    if i >= first_record.len() {
                        Err(format!(
                            "Column index {} is out of \
                                 bounds. Index must be between 0 \
                                 and {}.",
                            i,
                            first_record.len()
                        ))
                    } else {
                        Ok(i)
                    }
                }
            }
            OneSelector::IndexedName(ref s, sidx, _) => {
                let sidx = sidx.unwrap_or(0);

                if !use_names {
                    return Err(format!(
                        "Cannot use names ('{}') in selection \
                                        with --no-headers set.",
                        s
                    ));
                }
                let mut num_found = 0;

                if sidx < 0 {
                    for (i, field) in first_record.iter().enumerate().rev() {
                        if field == &s.as_bytes() {
                            if num_found == sidx.abs() - 1 {
                                return Ok(i);
                            }
                            num_found += 1;
                        }
                    }
                } else {
                    for (i, field) in first_record.iter().enumerate() {
                        if field == &s.as_bytes() {
                            if num_found == sidx {
                                return Ok(i);
                            }
                            num_found += 1;
                        }
                    }
                }

                if num_found == 0 {
                    Err(format!(
                        "'{}' does not exist \
                                 as a named header in the given CSV \
                                 data.",
                        s
                    ))
                } else if sidx < 0 {
                    Err(format!(
                        "index '{}' for '{}' is \
                                     out of bounds. Must be between -{} and -1.",
                        sidx, s, num_found
                    ))
                } else {
                    Err(format!(
                        "index '{}' for name '{}' is \
                                 out of bounds. Must be between 0 and {}.",
                        sidx,
                        s,
                        num_found - 1
                    ))
                }
            }
        }
    }
}

impl fmt::Debug for CompositeSelector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CompositeSelector::All(pos_opt) => {
                if let Some(pos) = pos_opt {
                    write!(f, "All[{}]", pos)
                } else {
                    write!(f, "All")
                }
            }
            CompositeSelector::One(ref sel) => sel.fmt(f),
            CompositeSelector::Range(ref s, ref e) => write!(f, "Range({:?}, {:?})", s, e),
            CompositeSelector::GlobPrefix(ref prefix, pos_opt) => write!(
                f,
                "Prefix({:?}){}",
                prefix,
                if let Some(pos) = pos_opt {
                    format!("[{}]", pos)
                } else {
                    "".to_string()
                }
            ),
            CompositeSelector::GlobSuffix(ref suffix, pos_opt) => write!(
                f,
                "Suffix({:?}){}",
                suffix,
                if let Some(pos) = pos_opt {
                    format!("[{}]", pos)
                } else {
                    "".to_string()
                }
            ),
            Self::GlobInner(ref prefix, ref suffix, pos_opt) => {
                write!(
                    f,
                    "Inner({:?}, {:?}){}",
                    prefix,
                    suffix,
                    if let Some(pos) = pos_opt {
                        format!("[{}]", pos)
                    } else {
                        "".to_string()
                    }
                )
            }
        }
    }
}

impl fmt::Debug for OneSelector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OneSelector::Start => write!(f, "Start"),
            OneSelector::End => write!(f, "End"),
            OneSelector::Index(idx) => write!(f, "Index({})", idx),
            OneSelector::IndexedName(ref s, idx, _) => match idx {
                None => write!(f, "IndexedName({})", s),
                Some(i) => write!(f, "IndexedName({}[{}])", s, i),
            },
        }
    }
}
