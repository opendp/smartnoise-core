//! Work-in-progress shorthand interface for building differentially private analyses.
//! Bundled with the rust validator.
//!
//! The Analysis struct has impl's for each component variant, that returns a builder object.
//! Mandatory arguments are supplied in analysis impl, but optional arguments and evaluated values may be set via the builder.
//! Once the component is ready to add to the analysis, call enter() on the builder to get a node id of the component.
//!
//! # Example
//! ```
//! use whitenoise_validator::bindings::Analysis;
//! use ndarray::arr1;
//! let mut analysis = Analysis::new();
//! let lit_2 = analysis.literal().value(2.0.into()).enter();
//! let lit_3 = analysis.literal().value(3.0.into()).enter();
//! let _lit_5 = analysis.add(lit_2, lit_3).enter();
//!
//! let col_a = analysis.literal()
//!     .value(arr1(&[1., 2., 3.]).into_dyn().into())
//!     .enter();
//! analysis.mean(col_a).enter();
//!
//! analysis.count(col_a).enter();
//! println!("graph {:?}", analysis.components);
//! println!("release {:?}", analysis.release);
//! ```

use crate::proto;
use crate::base::Release;
use std::collections::HashMap;


#[derive(Debug)]
pub struct Analysis {
    pub components: HashMap<u32, proto::Component>,
    pub component_count: u32,
    pub submission_count: u32,
    pub release: Release,
}

impl Analysis {
    pub fn new() -> Self {
        Analysis {
            components: HashMap::new(),
            component_count: 0,
            submission_count: 0,
            release: Release::new(),
        }
    }
}

include!(concat!(env!("OUT_DIR"), "/bindings_analysis.rs"));

pub mod builders {
    include!(concat!(env!("OUT_DIR"), "/bindings_builders.rs"));
}

#[cfg(test)]
mod test_bindings {
    use crate::errors::*;
    use crate::bindings::Analysis;
    use crate::bindings::*;
    use crate::bindings::builders;
    use ndarray::arr1;

    fn build_analysis() -> Result<()> {
        let mut analysis = Analysis::new();

        let lit_2 = analysis.literal().value(2.0.into()).enter();
        let lit_3 = analysis.literal().value(3.0.into()).enter();
        let _lit_5 = analysis.add(lit_2, lit_3).enter();

        let col_a = analysis.literal()
            .value(arr1(&[1., 2., 3.]).into_dyn().into())
            .enter();
        analysis.mean(col_a).enter();

        analysis.count(col_a).enter();
        println!("graph {:?}", analysis.components);
        println!("release {:?}", analysis.release);
        Ok(())
    }

    #[test]
    fn test_analysis() {
        build_analysis().unwrap();
    }
}


