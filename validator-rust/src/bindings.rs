//! The Whitenoise rust bindings provide a friendly way to build differentially private analyses specifically for users of the Rust language

use crate::proto;
use crate::base::{Release, Value, ReleaseNode};
use std::collections::HashMap;

#[derive(Debug)]
pub struct Analysis {
    pub components: HashMap<u32, proto::Component>,
    pub component_count: u32,
    pub submission_count: u32,
    pub dataset_count: u32,
    pub release: Release,
}

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

impl Analysis {
    pub fn new() -> Self {
        Analysis {
            components: HashMap::new(),
            component_count: 0,
            submission_count: 0,
            dataset_count: 0,
            release: Release::new(),
        }
    }
}

#[cfg(test)]
mod test_bindings {
    use crate::errors::*;
    use crate::bindings::Analysis;
    use ndarray::arr0;
    use crate::base::Value;

    fn build_analysis() -> Result<()> {
        let mut analysis = Analysis::new();

        let lit_2 = analysis.literal().value(2.0.into()).enter();
        let lit_3 = analysis.literal().value(3.0.into()).enter();
        let lit_6 = analysis.add(lit_2, lit_3).enter();

        println!("graph {:?}", analysis.components);
        println!("release {:?}", analysis.release);
        Ok(())
    }

    #[test]
    fn test_analysis() {
        build_analysis().unwrap();
    }
}


