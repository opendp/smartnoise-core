//! The Whitenoise rust runtime is an execution engine for evaluating differentially private analyses.
//! 
//! The runtime contains implementations of basic data transformations and aggregations, 
//! statistics, and privatizing mechanisms. These functions are combined in the 
//! Whitenoise validator to create more complex differentially private analyses.
//!
//! - [Top-level documentation](https://opendifferentialprivacy.github.io/whitenoise-core/)

extern crate whitenoise_validator;

pub use whitenoise_validator::proto;
use whitenoise_validator::errors::*;

pub mod utilities;
pub mod components;
pub mod ffi;
pub mod base;

use std::collections::HashMap;
use std::vec::Vec;

use whitenoise_validator::base::{Value};
use whitenoise_validator::utilities::serial::{parse_release, serialize_release};
use crate::base::evaluate_analysis;

pub type NodeArguments<'a> = HashMap<String, &'a Value>;

/// Evaluate an analysis and release the differentially private results.
pub fn release(
    request: proto::RequestRelease
) -> Result<(proto::Release, Vec<proto::Error>)> {
    let proto::RequestRelease {
        analysis, release, stack_trace: _, filter_level
    } = request;

    let (release, warnings) = evaluate_analysis(
        analysis.ok_or_else(|| Error::from("analysis must be defined"))?,
        parse_release(release.ok_or_else(|| Error::from("release must be defined"))?),
        proto::FilterLevel::from_i32(filter_level)
            .ok_or_else(|| Error::from(format!("unrecognized filter level {:?}", filter_level)))?)?;

    Ok((serialize_release(release), warnings))
}
