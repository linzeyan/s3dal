//! Cold tiers (cold-tier.md): backends besides the primary that hold only the bytes of cold
//! objects. The primary keeps the whole namespace; a cold object is an empty stub there whose
//! metadata names its tier object, `.s3dal/tier/<bucket>/<E(key)>!!/<id>` in the tier. This module
//! builds and claims the tiers at startup; what requests do with them lives in `s3::tier`.

use anyhow::{Context, bail};
use opendal::options::WriteOptions;
use opendal::{ErrorKind, Operator};
use serde::{Deserialize, Serialize};

use crate::backend::{self, Profile, RESERVED_ROOT};
use crate::config::{TierConfig, UNREAD_BY_CLIS};

/// One configured tier, probed and claimed for this primary.
#[derive(Clone)]
pub struct Tier {
    pub(crate) name: String,
    pub(crate) storage_class: String,
    pub(crate) op: Operator,
    pub(crate) profile: Profile,
}

impl std::fmt::Debug for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tier")
            .field("name", &self.name)
            .field("storage_class", &self.storage_class)
            .field("scheme", &self.op.info().scheme())
            .field("profile", &self.profile)
            .finish()
    }
}

impl Tier {
    /// Probes `op` and claims it for the primary `primary`, as startup does for a configured
    /// tier. `name` and `storage_class` must be valid (see `config::check_tiers`); an embedder
    /// or a test that builds its own operator passes them here.
    pub async fn open(
        name: &str,
        storage_class: &str,
        op: Operator,
        primary: &Operator,
        primary_profile: &Profile,
    ) -> anyhow::Result<Self> {
        let profile = backend::probe(&op)
            .await
            .with_context(|| format!("tier `{name}`"))?;
        let id = primary_id(primary, primary_profile).await?;
        claim(name, &op, &profile, &id).await?;
        Ok(Self {
            name: name.to_owned(),
            storage_class: storage_class.to_owned(),
            op,
            profile,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn storage_class(&self) -> &str {
        &self.storage_class
    }

    pub fn profile(&self) -> &Profile {
        &self.profile
    }

    /// The OpenDAL service the tier's objects are stored in.
    pub fn scheme(&self) -> String {
        self.op.info().scheme().to_string()
    }
}

/// Builds, probes and claims every configured tier, after the primary's probe and before any
/// request is served. A tier that cannot be reached fails startup, as the primary does: cold
/// reads would otherwise fail one by one, and its objects' deletes leave orphans.
pub async fn connect(
    primary: &Operator,
    primary_profile: &Profile,
    configs: &[TierConfig],
) -> anyhow::Result<Vec<Tier>> {
    let mut tiers = Vec::with_capacity(configs.len());
    for config in configs {
        let op = backend::build_operator(&config.backend, &config.backend_opts)
            .with_context(|| format!("tier `{}`", config.name))?;
        let tier = Tier::open(
            &config.name,
            &config.storage_class,
            op,
            primary,
            primary_profile,
        )
        .await?;
        tracing::info!(
            tier = tier.name,
            storage_class = tier.storage_class,
            backend = config.backend,
            profile = ?tier.profile,
            "tier profile"
        );
        if UNREAD_BY_CLIS.contains(&tier.storage_class.as_str()) {
            tracing::warn!(
                tier = tier.name,
                storage_class = tier.storage_class,
                "the aws CLI skips objects of this class on downloads unless given \
                 --force-glacier-transfer; GLACIER_IR or STANDARD_IA describe a tier that is read \
                 through"
            );
        }
        tiers.push(tier);
    }
    Ok(tiers)
}

/// The primary's id, made once by whichever instance starts first and read by every other.
const ID_FILE: &str = "id";
/// Which primary a tier belongs to.
const OWNER_FILE: &str = "tier/owner.json";

#[derive(Serialize, Deserialize)]
struct Owner {
    primary: String,
}

/// The id of the primary, `.s3dal/id`, made if it has none yet.
async fn primary_id(op: &Operator, profile: &Profile) -> anyhow::Result<String> {
    let made = uuid::Uuid::new_v4().simple().to_string();
    let path = format!("{RESERVED_ROOT}{ID_FILE}");
    let id = read_or_create(op, profile, &path, made.into_bytes())
        .await
        .context("cannot read or make the primary backend's id")?;
    String::from_utf8(id)
        .ok()
        .map(|id| id.trim().to_owned())
        .filter(|id| !id.is_empty())
        .with_context(|| format!("the primary backend's {path} is not an id"))
}

/// Records the tier as `primary`'s, or checks that it already is. Two deployments pointed at
/// one tier root would each take the other's tier objects for orphans.
async fn claim(name: &str, op: &Operator, profile: &Profile, primary: &str) -> anyhow::Result<()> {
    let path = format!("{RESERVED_ROOT}{OWNER_FILE}");
    let mine = serde_json::to_vec(&Owner {
        primary: primary.to_owned(),
    })?;
    let found = read_or_create(op, profile, &path, mine)
        .await
        .with_context(|| format!("tier `{name}`: cannot read or write {path}"))?;
    let owner: Owner = serde_json::from_slice(&found)
        .with_context(|| format!("tier `{name}`: {path} is not a claim s3dal wrote"))?;
    if owner.primary != primary {
        bail!(
            "tier `{name}` belongs to another s3dal (id {}); remove {path} there if that s3dal \
             is gone",
            owner.primary
        );
    }
    Ok(())
}

/// The content of `path`, written as `value` if absent. Where the backend checks conditions the
/// write is a create, so of two instances starting at once one writes and the other reads what
/// it wrote; on fs only one process serves a root.
async fn read_or_create(
    op: &Operator,
    profile: &Profile,
    path: &str,
    value: Vec<u8>,
) -> opendal::Result<Vec<u8>> {
    match op.read(path).await {
        Ok(found) => return Ok(found.to_vec()),
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    let opts = WriteOptions {
        if_not_exists: profile.conditional_writes,
        ..Default::default()
    };
    match op.write_options(path, value.clone(), opts).await {
        Ok(_) => Ok(value),
        Err(err) if err.kind() == ErrorKind::ConditionNotMatch => Ok(op.read(path).await?.to_vec()),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fs(root: &std::path::Path) -> (Operator, Profile) {
        let root = root.to_string_lossy().into_owned();
        let op = backend::build_operator("fs", &[("root".to_owned(), root)]).unwrap();
        let profile = backend::probe(&op).await.unwrap();
        (op, profile)
    }

    fn config(name: &str, root: &std::path::Path) -> TierConfig {
        TierConfig {
            name: name.into(),
            storage_class: "STANDARD_IA".into(),
            backend: "fs".into(),
            backend_opts: vec![("root".into(), root.to_string_lossy().into_owned())],
        }
    }

    #[tokio::test]
    async fn a_tier_belongs_to_the_first_primary_that_claims_it() {
        // Two deployments sharing a tier root would each sweep the other's objects as orphans;
        // the second must not start. The first starts again, as often as it likes.
        let (one, two, shared) = (
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        );
        let (one, one_profile) = fs(one.path()).await;
        let (two, two_profile) = fs(two.path()).await;
        let tiers = [config("hdd", shared.path())];
        connect(&one, &one_profile, &tiers).await.unwrap();
        connect(&one, &one_profile, &tiers).await.unwrap();
        let err = connect(&two, &two_profile, &tiers).await.unwrap_err();
        let err = format!("{err:#}");
        assert!(err.contains("belongs to another s3dal"), "{err}");
        assert!(err.contains("owner.json"), "{err}");
        // The primary's id is made once and kept.
        let id = one.read(".s3dal/id").await.unwrap().to_vec();
        connect(&one, &one_profile, &tiers).await.unwrap();
        assert_eq!(one.read(".s3dal/id").await.unwrap().to_vec(), id);
    }

    #[tokio::test]
    async fn a_tier_that_cannot_be_reached_stops_startup() {
        // Started without it, every cold read would fail, one by one, and deletes would leave
        // its objects behind for good (cold-tier.md, open question 3).
        let (primary, tier_parent) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (primary, profile) = fs(primary.path()).await;
        // A root under a regular file can be neither made nor written.
        let blocked = tier_parent.path().join("file");
        std::fs::write(&blocked, b"x").unwrap();
        let err = connect(&primary, &profile, &[config("hdd", &blocked.join("root"))])
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("tier `hdd`"), "{err:#}");
    }
}
