use crate::errors::*;

use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Named};
use crate::base::{Hashmap, Value, ValueProperties, HashmapProperties, ArrayProperties, DataType};
use ndarray::prelude::*;

impl Component for proto::Materialize {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &HashMap<String, Value>,
        _properties: &base::NodeProperties,
        node_id: u32
    ) -> Result<ValueProperties> {

        let column_names = self.get_names(public_arguments, &HashMap::new(), None)?;

        Ok(HashmapProperties {
            num_records: None,
            disjoint: false,
            properties: Hashmap::<ValueProperties>::Str(column_names.into_iter()
                .map(|name| (name, ValueProperties::Array(ArrayProperties {
                    num_records: None,
                    num_columns: Some(1),
                    nullity: true,
                    releasable: self.public,
                    c_stability: vec![1.],
                    aggregator: None,
                    nature: None,
                    data_type: DataType::Str,
                    dataset_id: Some(node_id as i64),
                    // this is a library-wide assumption - that datasets initially have more than zero rows
                    is_not_empty: true,
                    dimensionality: Some(1)
                }))).collect()),
            variant: proto::hashmap_properties::Variant::Dataframe
        }.into())
    }
}

impl Named for proto::Materialize {
    fn get_names(
        &self,
        public_arguments: &HashMap<String, Value>,
        _argument_variables: &HashMap<String, Vec<String>>,
        _release: Option<&Value>
    ) -> Result<Vec<String>> {

        let column_names = public_arguments.get("column_names")
            .and_then(|column_names| column_names.array().ok()?.string().ok()).cloned();
        let num_columns = public_arguments.get("num_columns")
            .and_then(|num_columns| num_columns.array().ok()?.first_i64().ok());

        // standardize to vec of column names
        Ok(match (column_names, num_columns) {
            (Some(column_names), None) => column_names.into_dimensionality::<Ix1>()?.to_vec(),
            (None, Some(num_columns)) => (0..num_columns).map(|idx| idx.to_string()).collect(),
            _ => return Err("either column_names or num_columns must be specified".into())
        })
    }
}
