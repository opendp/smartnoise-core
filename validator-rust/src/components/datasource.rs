use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};

use std::collections::HashMap;

use crate::base::{ArrayND, Value, Vector2DJagged, Nature, Vector1DNull, NatureContinuous, NatureCategorical, Properties, NodeProperties};


use crate::{proto, base};

use crate::components::{Component};


impl Component for proto::DataSource {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _public_arguments: &HashMap<String, Value>,
        _properties: &base::NodeProperties,
    ) -> Result<Properties> {
        Ok(Properties {
            nullity: true,
            releasable: false,
            nature: None,
            c_stability: vec![1.],
            num_columns: Some(1),
            num_records: vec![None]
        })
    }

    fn is_valid(
        &self,
        _properties: &base::NodeProperties,
    ) -> Result<()> {
        Ok(())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}
