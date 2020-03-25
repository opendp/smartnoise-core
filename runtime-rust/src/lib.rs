/// The Whitenoise rust runtime is an execution engine for evaluating differentially private analyses.

extern crate whitenoise_validator;

pub use whitenoise_validator::proto;
use whitenoise_validator::errors::*;

// trait which holds `display_chain`
use error_chain::ChainedError;
pub mod base;
pub mod utilities;
pub mod components;
pub mod ffi;

extern crate libc;

/// Evaluate an analysis and release the differentially private results.
pub fn release(
    request: &proto::RequestRelease
) -> Result<proto::Release> {
    base::execute_graph(
        request.analysis.as_ref().ok_or::<Error>("analysis must be defined".into())?,
        request.release.as_ref().ok_or::<Error>("release must be defined".into())?)
}
