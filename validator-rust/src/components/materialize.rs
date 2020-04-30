use crate::errors::*;

use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Named};
use crate::base::{Hashmap, Value, ValueProperties, HashmapProperties, ArrayProperties, DataType};
use crate::utilities::serial::{parse_i64_null, parse_value};
use crate::utilities::inference::infer_property;
use ndarray::prelude::*;

impl Component for proto::Materialize {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &HashMap<String, Value>,
        _properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {

        let column_names = self.get_names(public_arguments, &HashMap::new(), &None)?;

        let data_source = self.data_source.clone()
            .ok_or_else(|| Error::from("data source must be supplied"))?;

        match data_source.value.as_ref()
            .ok_or_else(|| Error::from("data_source variant must be defined"))? {
            proto::data_source::Value::Literal(value) => {
                let array = match value.data.as_ref().ok_or_else(|| Error::from("Value variant must not empty"))? {
                    proto::value::Data::Array(value) => value,
                    _ => return Err("Value variant must be an Array".into())
                };

                let data_type = match array
                    .flattened.as_ref().ok_or_else(|| Error::from("Array must not be empty"))?
                    .data.as_ref().ok_or_else(|| Error::from("Array must not be empty"))? {
                    proto::array1d::Data::Bool(_) => DataType::Bool,
                    proto::array1d::Data::String(_) => DataType::Str,
                    proto::array1d::Data::I64(_) => DataType::I64,
                    proto::array1d::Data::F64(_) => DataType::F64,
                };

                if public_arguments.get("column_names").is_some() {
                    match self.public {
                        false => Ok(ValueProperties::Hashmap(HashmapProperties {
                            num_records: None,
                            disjoint: false,
                            properties: Hashmap::<ValueProperties>::Str(column_names.iter().map(|name| (name.clone(), ValueProperties::Array(ArrayProperties {
                                num_records: None,
                                num_columns: Some(1),
                                nullity: true,
                                releasable: self.public,
                                c_stability: vec![1.],
                                aggregator: None,
                                nature: None,
                                data_type: data_type.clone(),
                                dataset_id: self.dataset_id.as_ref().and_then(parse_i64_null),
                                // this is a library-wide assumption - that datasets initially have more than zero rows
                                is_not_empty: true,
                                dimensionality: 1
                            }))).collect()),
                            columnar: true
                        })),
                        true => return Err("column_names on value-materialized public data is not currently supported. Use num_columns instead.".into())
                    }
                } else {
                    match self.public {
                        false => Ok(ValueProperties::Array(ArrayProperties {
                            num_records: None,
                            num_columns: Some(column_names.len() as i64),
                            nullity: true,
                            releasable: false,
                            c_stability: column_names.iter().map(|_| 1.).collect(),
                            aggregator: None,
                            nature: None,
                            data_type,
                            dataset_id: self.dataset_id.as_ref().and_then(parse_i64_null),
                            // this is a library-wide assumption - that datasets initially have more than zero rows
                            is_not_empty: true,
                            dimensionality: array.shape.len() as u32
                        })),
                        true => infer_property(&parse_value(value)?)
                    }
                }
            }
            proto::data_source::Value::FilePath(_) => Ok(HashmapProperties {
                num_records: None,
                disjoint: false,
                properties: Hashmap::<ValueProperties>::Str(column_names.iter()
                    .map(|name| (name.clone(), ValueProperties::Array(ArrayProperties {
                        num_records: None,
                        num_columns: Some(1),
                        nullity: true,
                        releasable: self.public,
                        c_stability: vec![1.],
                        aggregator: None,
                        nature: None,
                        data_type: DataType::Str,
                        dataset_id: self.dataset_id.as_ref().and_then(parse_i64_null),
                        // this is a library-wide assumption - that datasets initially have more than zero rows
                        is_not_empty: true,
                        dimensionality: 1
                    }))).collect()),
                columnar: true,
            }.into()),
            data_source => Err(format!("data source format is not supported: {:?}", data_source).into())
        }
    }
}

impl Named for proto::Materialize {
    fn get_names(
        &self,
        public_arguments: &HashMap<String, Value>,
        _argument_variables: &HashMap<String, Vec<String>>,
        _release: &Option<&Value>
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
