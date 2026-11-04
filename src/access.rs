//! What a key pair may reach. A pair names the buckets, or folders of them, it is limited to,
//! and whether it may write there; a pair that names neither reaches every bucket and the
//! settings, as every pair did before limits existed. This is an allowlist, not IAM: there are
//! no policies, deny rules or conditions, so what a pair reaches can be read off its two keys.

use std::fmt;

use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Access {
    #[default]
    ReadWrite,
    ReadOnly,
}

impl Access {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadWrite => "read-write",
            Self::ReadOnly => "read-only",
        }
    }
}

/// A bucket, or a folder in one: `photos`, or `photos/2024/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub bucket: String,
    /// Empty for the whole bucket; otherwise ends in `/`.
    pub prefix: String,
}

impl Scope {
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        let (bucket, rest) = s.split_once('/').unwrap_or((s, ""));
        if bucket.is_empty() {
            return Err(format!("{s:?}: expected a bucket, or bucket/folder/"));
        }
        // Always a folder, so `photos/2024` does not also reach `photos/2024-old/`.
        let prefix = if rest.is_empty() || rest.ends_with('/') {
            rest.to_owned()
        } else {
            format!("{rest}/")
        };
        Ok(Self {
            bucket: bucket.to_owned(),
            prefix,
        })
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.prefix.is_empty() {
            f.write_str(&self.bucket)
        } else {
            write!(f, "{}/{}", self.bucket, self.prefix)
        }
    }
}

/// The limits of a key pair that has any. Such a pair never manages buckets or the settings:
/// those reach past any one bucket or folder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limit {
    /// `None` for every bucket, as a read-only pair without `allow` has.
    scopes: Option<Vec<Scope>>,
    write: bool,
}

impl Limit {
    /// `None` for a pair without limits.
    pub fn of(allow: Option<&[String]>, access: Access) -> Result<Option<Self>, String> {
        if allow.is_none() && access == Access::ReadWrite {
            return Ok(None);
        }
        let scopes = match allow {
            None => None,
            // Reaching nothing is never what was meant; more likely a list left half-edited.
            Some([]) => return Err("`allow` is empty; leave it out to allow every bucket".into()),
            Some(list) => Some(
                list.iter()
                    .map(|s| Scope::parse(s))
                    .collect::<Result<_, _>>()?,
            ),
        };
        Ok(Some(Self {
            scopes,
            write: access == Access::ReadWrite,
        }))
    }

    pub fn may_write(&self) -> bool {
        self.write
    }

    pub fn scopes(&self) -> Option<&[Scope]> {
        self.scopes.as_deref()
    }

    fn any(&self, f: impl Fn(&Scope) -> bool) -> bool {
        self.scopes.as_ref().is_none_or(|s| s.iter().any(f))
    }

    /// Whether the bucket is listed and its name may be seen, for a scope anywhere in it.
    pub fn reaches_bucket(&self, bucket: &str) -> bool {
        self.any(|s| s.bucket == bucket)
    }

    pub fn reaches_key(&self, bucket: &str, key: &str) -> bool {
        self.any(|s| s.bucket == bucket && key.starts_with(&s.prefix))
    }

    /// A listing shows keys under its prefix, and the names of folders there, so the whole
    /// prefix must lie inside a scope: listing `photos/` for a pair limited to `photos/2024/`
    /// would show the other years' folders.
    pub fn reaches_listing(&self, bucket: &str, prefix: &str) -> bool {
        self.any(|s| s.bucket == bucket && prefix.starts_with(&s.prefix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(allow: &[&str], access: Access) -> Limit {
        let allow: Vec<String> = allow.iter().map(|s| (*s).to_owned()).collect();
        Limit::of(Some(&allow), access).unwrap().unwrap()
    }

    #[test]
    fn a_pair_without_limits_has_none() {
        // Pairs configured before limits existed must keep reaching everything.
        assert_eq!(Limit::of(None, Access::ReadWrite), Ok(None));
        let read_only = Limit::of(None, Access::ReadOnly).unwrap().unwrap();
        assert!(read_only.reaches_key("any", "thing") && !read_only.may_write());
    }

    #[test]
    fn a_folder_scope_reaches_its_keys_only() {
        let l = limit(&["photos/2024"], Access::ReadWrite);
        assert!(l.reaches_key("photos", "2024/a.jpg"));
        assert!(l.reaches_key("photos", "2024/"));
        // Not the sibling that merely shares the name's start, nor another bucket.
        assert!(!l.reaches_key("photos", "2024-old/a.jpg"));
        assert!(!l.reaches_key("photos", "2023/a.jpg"));
        assert!(!l.reaches_key("videos", "2024/a.jpg"));
        assert!(l.reaches_bucket("photos") && !l.reaches_bucket("videos"));
    }

    #[test]
    fn listings_must_stay_inside_a_scope() {
        // A listing of the parent would reveal the names of folders the pair cannot open.
        let l = limit(&["photos/2024/", "logs"], Access::ReadOnly);
        assert!(l.reaches_listing("photos", "2024/"));
        assert!(l.reaches_listing("photos", "2024/may/"));
        assert!(!l.reaches_listing("photos", ""));
        assert!(!l.reaches_listing("photos", "20"));
        assert!(l.reaches_listing("logs", ""));
    }

    #[test]
    fn an_empty_allow_list_is_refused() {
        assert!(Limit::of(Some(&[]), Access::ReadWrite).is_err());
        assert!(Limit::of(Some(&["/x".to_owned()]), Access::ReadWrite).is_err());
    }
}
