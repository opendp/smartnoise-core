use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};

use std::collections::HashMap;


use crate::{proto, base};

use crate::components::Component;
use crate::utilities::serial::{parse_value};
use crate::base::{Value, Properties, NodeProperties};
use crate::utilities::inference::{infer_num_columns, infer_property};

impl Component for proto::Constant {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _public_arguments: &HashMap<String, Value>,
        _properties: &base::NodeProperties,
    ) -> Result<Properties> {
        match self.value.clone() {
            Some(value) => infer_property(&parse_value(&value)?),
            None => Err("release value for constant is missing".into())
        }
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}
