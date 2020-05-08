use crate::errors::*;

use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Named};
use crate::base::{Indexmap, Value, ValueProperties, IndexmapProperties, ArrayProperties};
use ndarray::prelude::*;
use crate::utilities::prepend;

impl Component for proto::Rename {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        _node_id: u32
    ) -> Result<ValueProperties> {

        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?;

        let column_names = self.get_names(public_arguments, &HashMap::new(), None)?;

        if column_names.len() != data_property.c_stability.len() {
            return Err("Column names must be the same length as the number of columns.".into())
        }

        Ok(ValueProperties::Indexmap(IndexmapProperties {
            num_records: None,
            disjoint: false,
            variant: proto::indexmap_properties::Variant::Dataframe,
            properties: Indexmap::<ValueProperties>::Str(column_names.into_iter().enumerate()
                .map(|(idx, name)| (name, ValueProperties::Array(ArrayProperties {
                    num_records: data_property.num_records,
                    num_columns: Some(1),
                    nullity: data_property.nullity,
                    releasable: data_property.releasable,
                    c_stability: vec![data_property.c_stability[idx]],
                    aggregator: None,
                    nature: None,
                    data_type: (&data_property.data_type).clone(),
                    dataset_id: data_property.dataset_id,
                    is_not_empty: data_property.is_not_empty,
                    dimensionality: Some(1),
                }))).collect()),
        }))
    }
}

impl Named for proto::Rename {
    fn get_names(
        &self,
        public_arguments: &HashMap<String, Value>,
        _argument_variables: &HashMap<String, Vec<String>>,
        _release: Option<&Value>
    ) -> Result<Vec<String>> {
        Ok(public_arguments.get("column_names")
            .ok_or_else(|| Error::from("column_names must be supplied"))?
            .array()?.string()?.clone().into_dimensionality::<Ix1>()?.to_vec())
    }
}
