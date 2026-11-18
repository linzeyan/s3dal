//! Expose any Apache OpenDAL backend as an S3-compatible endpoint.
//!
//! [`S3Dal`] implements [`s3s::S3`], so it can be served by the bundled binary or embedded in
//! another service through [`s3s::service::S3ServiceBuilder`].

pub mod access;
pub mod backend;
mod checksum;
#[cfg(feature = "client")]
pub mod client;
pub mod config;
pub mod console;
mod error;
pub mod events;
pub mod jobs;
pub mod live;
pub mod logging;
pub mod meta;
pub mod metrics;
pub mod notify;
mod s3;
pub mod server;
pub mod tier;

pub use backend::Profile;
pub use config::Settings;
pub use s3::{BucketUsage, Expired, Quota, S3Dal};
pub use tier::Tier;
