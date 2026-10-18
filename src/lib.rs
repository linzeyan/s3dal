//! Expose any Apache OpenDAL backend as an S3-compatible endpoint.
//!
//! [`S3Dal`] implements [`s3s::S3`], so it can be served by the bundled binary or embedded in
//! another service through [`s3s::service::S3ServiceBuilder`].

pub mod backend;
mod checksum;
pub mod config;
mod error;
pub mod meta;
mod s3;
pub mod server;

pub use backend::Profile;
pub use config::Settings;
pub use s3::S3Dal;
