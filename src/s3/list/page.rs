//! One page of a listing, cut from keys offered in key order.
//!
//! Everything that decides what a page holds (prefix, delimiter grouping, the resume point,
//! max-keys, truncation) lives here and nowhere else. Key sources only save work: anything they
//! leave out must be something this builder would have dropped anyway, so every source yields
//! the same pages and the rules can be tested without a backend.

use crate::s3::naming;

/// A validated ListObjectsV2 request, as far as paging is concerned.
#[derive(Debug, Clone, Default)]
pub(super) struct Query {
    pub prefix: String,
    pub delimiter: Option<String>,
    /// Exclusive: the page starts strictly after this name.
    pub start_after: Option<String>,
    pub max_keys: usize,
}

impl Query {
    /// The CommonPrefix that `path` folds into: the prefix and everything after it up to and
    /// including the first occurrence of the delimiter. `path` is a key, or a directory ending
    /// in `/`; every key below such a directory folds into the same CommonPrefix as the
    /// directory, whatever the delimiter. The occurrence found lies inside the directory's own
    /// path, so a key below it cannot hold an earlier one. Likewise every key that starts with
    /// a CommonPrefix folds into it, so a group is one contiguous run of keys.
    pub fn common_prefix<'p>(&self, path: &'p str) -> Option<&'p str> {
        let delimiter = self.delimiter.as_deref()?;
        let rest = path.strip_prefix(self.prefix.as_str())?;
        let end = self.prefix.len() + rest.find(delimiter)? + delimiter.len();
        Some(&path[..end])
    }

    pub fn is_after_start(&self, name: &str) -> bool {
        self.start_after
            .as_deref()
            .is_none_or(|marker| name > marker)
    }

    /// Whether a key below `dir` (a directory path ending in `/`) can reach the page. Every such
    /// key starts with `dir`, so it can match the prefix only if one of the two contains the
    /// other, and it can follow the marker only if `dir` does or `dir` is a prefix of the marker.
    pub fn may_list_below(&self, dir: &str) -> bool {
        let prefix = self.prefix.as_str();
        let matches_prefix = dir.starts_with(prefix) || prefix.starts_with(dir);
        let before_start = self
            .start_after
            .as_deref()
            .is_some_and(|marker| dir < marker && !marker.starts_with(dir));
        matches_prefix && !before_start
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Item<T> {
    /// `entry` is whatever the source knew about the object when it listed it.
    Object {
        key: String,
        entry: T,
    },
    CommonPrefix(String),
}

impl<T> Item<T> {
    /// Where the item sorts. A CommonPrefix sorts as itself, so a marker equal to it skips the
    /// whole group even though every key in the group sorts after it.
    pub fn name(&self) -> &str {
        match self {
            Self::Object { key, .. } => key,
            Self::CommonPrefix(prefix) => prefix,
        }
    }
}

#[derive(Debug)]
pub(super) struct Page<T> {
    pub items: Vec<Item<T>>,
    /// Set only when the listing goes on: the name of the last item, after which the next
    /// page starts.
    pub next_marker: Option<String>,
}

pub(super) struct PageBuilder<'q, T> {
    query: &'q Query,
    items: Vec<Item<T>>,
}

impl<'q, T> PageBuilder<'q, T> {
    pub fn new(query: &'q Query) -> Self {
        Self {
            query,
            items: Vec::new(),
        }
    }

    pub fn query(&self) -> &'q Query {
        self.query
    }

    /// Sources stop feeding once this holds. The item after the last one on the page is what
    /// tells the page it is truncated, so it is collected too. max-keys 0 asks for nothing and,
    /// as on AWS, is never truncated.
    pub fn is_full(&self) -> bool {
        self.query.max_keys == 0 || self.items.len() > self.query.max_keys
    }

    /// Whether the last item taken is `name`. Keys of one group are contiguous in key order, so
    /// once its CommonPrefix is taken, nothing else from the group can add to the page.
    pub fn took_last(&self, name: &str) -> bool {
        self.items.last().is_some_and(|last| last.name() == name)
    }

    /// Offers the next key; keys must come in key order. A key that is outside the prefix or
    /// not after the marker is ignored, and one that folds into the CommonPrefix just taken is
    /// absorbed by it. So is a name s3dal refuses as a key, which another tool may have put on
    /// the backend: it could not be read through s3dal, and one with a control character would
    /// fail the whole page.
    pub fn offer(&mut self, key: &str, entry: T) {
        if self.is_full()
            || !key.starts_with(self.query.prefix.as_str())
            || naming::validate_key(key).is_err()
        {
            return;
        }
        let item = match self.query.common_prefix(key) {
            Some(prefix) if self.took_last(prefix) => return,
            Some(prefix) => Item::CommonPrefix(prefix.to_owned()),
            None => Item::Object {
                key: key.to_owned(),
                entry,
            },
        };
        if self.query.is_after_start(item.name()) {
            self.items.push(item);
        }
    }

    pub fn finish(mut self) -> Page<T> {
        let is_truncated = self.items.len() > self.query.max_keys;
        self.items.truncate(self.query.max_keys);
        let next_marker = is_truncated
            .then(|| self.items.last().map(|item| item.name().to_owned()))
            .flatten();
        Page {
            items: self.items,
            next_marker,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(prefix: &str, delimiter: bool, start_after: Option<&str>, max_keys: usize) -> Query {
        Query {
            prefix: prefix.to_owned(),
            delimiter: delimiter.then(|| "/".to_owned()),
            start_after: start_after.map(str::to_owned),
            max_keys,
        }
    }

    /// Objects as their key, CommonPrefixes as `CP:<prefix>`, in page order.
    fn run(keys: &[&str], q: &Query) -> (Vec<String>, Option<String>) {
        let mut builder = PageBuilder::new(q);
        for key in keys {
            builder.offer(key, ());
        }
        let page = builder.finish();
        let names = page
            .items
            .iter()
            .map(|item| match item {
                Item::Object { key, .. } => key.clone(),
                Item::CommonPrefix(prefix) => format!("CP:{prefix}"),
            })
            .collect();
        (names, page.next_marker)
    }

    const KEYS: &[&str] = &["a-", "a.txt", "a/b", "a/c/d", "a0", "b", "b/x"];

    #[test]
    fn names_s3dal_refuses_as_keys_are_not_listed() {
        // Another tool may have stored them on the backend. One with a control character would
        // make s3s fail the whole page, and one ending in a space could not be read through
        // s3dal: OpenDAL would read the key without it.
        let keys = ["a", "ctl\u{1}char", "ctl\u{1}dir/x", "ws ", "x"];
        let (names, _) = run(&keys, &query("", true, None, 1000));
        assert_eq!(names, ["a", "x"]);
    }

    #[test]
    fn keys_outside_the_prefix_are_ignored() {
        // A prefix is a plain string match, so it can end in the middle of a name.
        let (names, _) = run(KEYS, &query("a", false, None, 1000));
        assert_eq!(names, ["a-", "a.txt", "a/b", "a/c/d", "a0"]);
        let (names, _) = run(KEYS, &query("a/c", false, None, 1000));
        assert_eq!(names, ["a/c/d"]);
    }

    #[test]
    fn delimiter_folds_each_group_into_one_common_prefix_in_key_order() {
        // `aws s3 ls` and every file browser build their tree from these; a repeated or
        // misplaced prefix shows up as a duplicate or out-of-order folder.
        let (names, _) = run(KEYS, &query("", true, None, 1000));
        assert_eq!(names, ["a-", "a.txt", "CP:a/", "a0", "b", "CP:b/"]);
        let (names, _) = run(KEYS, &query("a/", true, None, 1000));
        assert_eq!(names, ["a/b", "CP:a/c/"]);
    }

    #[test]
    fn any_delimiter_folds_up_to_its_first_occurrence_after_the_prefix() {
        // S3 groups by any string: flat namespaces group by `-` or `.`, date-partitioned ones by
        // several characters, and ceph s3-tests list with `a`, `%`, ` ` and `.`. Only what
        // follows the prefix counts, so with prefix `ba` the `a` of `bazar` that groups is the
        // second one.
        let q = |prefix: &str, delimiter: &str, start_after: Option<&str>| Query {
            delimiter: Some(delimiter.to_owned()),
            ..query(prefix, false, start_after, 1000)
        };
        let keys = ["bar", "baz", "bazar", "cab", "foo"];
        assert_eq!(run(&keys, &q("", "a", None)).0, ["CP:ba", "CP:ca", "foo"]);
        assert_eq!(run(&keys, &q("ba", "a", None)).0, ["bar", "baz", "CP:baza"]);
        assert_eq!(
            run(&keys, &q("", "az", None)).0,
            ["bar", "CP:baz", "cab", "foo"]
        );
        // A marker at or inside a group skips the group, as with `/`.
        for marker in ["ba", "bar"] {
            assert_eq!(run(&keys, &q("", "a", Some(marker))).0, ["CP:ca", "foo"]);
        }
        // A delimiter holding `/` need not end at a directory boundary.
        let paths = ["x/a", "x/b/c", "x/bz", "x/c"];
        assert_eq!(run(&paths, &q("", "/b", None)).0, ["x/a", "CP:x/b", "x/c"]);
        // After a prefix that ends with the delimiter, the key equal to the prefix is a key.
        let dashed = ["a-", "a-b", "a-b-c", "a-c-"];
        assert_eq!(
            run(&dashed, &q("a-", "-", None)).0,
            ["a-", "a-b", "CP:a-b-", "CP:a-c-"]
        );
    }

    #[test]
    fn a_page_holds_max_keys_items_and_the_next_one_starts_after_its_last() {
        let q = query("", true, None, 2);
        let (names, marker) = run(KEYS, &q);
        assert_eq!(names, ["a-", "a.txt"]);
        assert_eq!(marker.as_deref(), Some("a.txt"));

        let q = query("", true, marker.as_deref(), 2);
        let (names, marker) = run(KEYS, &q);
        assert_eq!(names, ["CP:a/", "a0"]);
        assert_eq!(marker.as_deref(), Some("a0"));
    }

    #[test]
    fn a_page_that_ends_exactly_at_the_last_item_is_not_truncated() {
        // A spurious token would make clients request an empty extra page, or loop on it.
        let (names, marker) = run(KEYS, &query("", true, None, 6));
        assert_eq!(names.len(), 6);
        assert_eq!(marker, None);
    }

    #[test]
    fn a_common_prefix_marker_skips_its_whole_group() {
        // The token after a page ending in `a/` must not bring back `a/` on the next page,
        // although every key under `a/` sorts after it.
        let (names, _) = run(KEYS, &query("", true, Some("a/"), 1000));
        assert_eq!(names, ["a0", "b", "CP:b/"]);
    }

    #[test]
    fn a_marker_inside_a_group_skips_the_group_but_not_later_keys() {
        // The group sorts at its prefix, which lies before the marker.
        let (names, _) = run(KEYS, &query("", true, Some("a/b"), 1000));
        assert_eq!(names, ["a0", "b", "CP:b/"]);
        // Without a delimiter the same marker resumes inside the directory.
        let (names, _) = run(KEYS, &query("", false, Some("a/b"), 1000));
        assert_eq!(names, ["a/c/d", "a0", "b", "b/x"]);
    }

    #[test]
    fn the_marker_itself_is_excluded() {
        let (names, _) = run(KEYS, &query("", false, Some("a0"), 1000));
        assert_eq!(names, ["b", "b/x"]);
    }

    #[test]
    fn max_keys_zero_wants_nothing_and_is_never_truncated() {
        // AWS answers max-keys=0 with an empty, untruncated page; a token here would send
        // paginators into an endless loop of empty pages.
        let q = query("", false, None, 0);
        let builder = PageBuilder::<()>::new(&q);
        assert!(builder.is_full(), "sources must not even start");
        assert_eq!(run(KEYS, &q), (vec![], None));
    }

    #[test]
    fn the_builder_wants_exactly_one_item_past_the_page() {
        // That one item proves truncation; anything more is wasted backend work.
        let q = query("", false, None, 2);
        let mut builder = PageBuilder::new(&q);
        for key in ["a", "b"] {
            builder.offer(key, ());
            assert!(!builder.is_full());
        }
        builder.offer("c", ());
        assert!(builder.is_full());
    }

    /// Every string over `alphabet` up to `max_len` characters, the empty one included.
    fn strings(alphabet: &[&str], max_len: usize) -> Vec<String> {
        let mut all = vec![String::new()];
        let mut last = vec![String::new()];
        for _ in 0..max_len {
            last = last
                .iter()
                .flat_map(|s| alphabet.iter().map(move |c| format!("{s}{c}")))
                .collect();
            all.extend(last.iter().cloned());
        }
        all
    }

    #[test]
    fn pruning_and_folding_a_directory_never_change_the_page() {
        // The walk skips or folds whole directories on the strength of these predicates, and
        // the ordered path seeks past every key that starts with a CommonPrefix; a wrong answer
        // silently drops keys from listings. Checked exhaustively around the bytes that matter
        // for `/` ordering, `-` (0x2D) `.` (0x2E) `/` (0x2F) `0` (0x30), with every delimiter
        // of up to two of them.
        let alphabet = ["a", "-", ".", "/", "0"];
        let short = strings(&alphabet, 2);
        let suffixes: Vec<&String> = short.iter().filter(|s| !s.is_empty()).collect();
        let mut markers: Vec<Option<&str>> = short.iter().map(|s| Some(s.as_str())).collect();
        markers.push(None);

        let dirs: Vec<String> = short.iter().map(|s| format!("{s}/")).collect();
        for prefix in &short {
            // Pruning does not depend on the delimiter: a key that is not listable is not
            // listable folded either, since its CommonPrefix sorts at or before it.
            for dir in &dirs {
                for &marker in &markers {
                    let q = query(prefix, true, marker, 1000);
                    if q.may_list_below(dir) {
                        continue;
                    }
                    for suffix in &suffixes {
                        let key = format!("{dir}{suffix}");
                        let listable = key.starts_with(prefix.as_str()) && q.is_after_start(&key);
                        assert!(!listable, "{key:?} lost: dir {dir:?}, {q:?}");
                    }
                }
            }
            for delimiter in &suffixes {
                let q = Query {
                    delimiter: Some((*delimiter).clone()),
                    ..query(prefix, false, None, 1000)
                };
                for path in dirs.iter().chain(suffixes.iter().copied()) {
                    let Some(group) = q.common_prefix(path) else {
                        continue;
                    };
                    for suffix in &suffixes {
                        // Below a folded directory, and after a CommonPrefix.
                        for key in [format!("{path}{suffix}"), format!("{group}{suffix}")] {
                            assert_eq!(q.common_prefix(&key), Some(group), "{key:?}, {q:?}");
                        }
                    }
                }
            }
        }
    }
}
