use crate::errors::*;


use std::collections::HashMap;

use crate::base::{ArrayND, Value, Properties, NodeProperties};


use crate::{proto, base};

use crate::components::Component;


use std::ops::Deref;

// TODO: this could use additional checks to prevent out of bounds


impl Component for proto::Index {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_properties = properties.get("data")
            .ok_or::<Error>("data is a required argument for Index".into())?.clone();

        match public_arguments.get("columns")
            .ok_or::<Error>("columns is a required argument for Index".into())?.deref().to_owned().get_arraynd()? {
            ArrayND::Str(column_names) => {
                let column_names = column_names.iter().map(|v| v.clone()).collect::<Vec<String>>();
                // TODO: this data is missing, need to figure out how to get all column names here
                let all_column_names: Vec<String> = vec![];

                // update natures
//                if let Some(mut nature) = data_properties.nature.clone() {
//                    data_properties.nature = match nature {
//                        Nature::Categorical(mut nature) => Some(Nature::Categorical(NatureCategorical {
//                            categories: column_names.iter()
//                                .map(|column_name| nature.categories[all_column_names.iter()
//                                    .position(|x| x == column_name).unwrap()]).collect()
//                        })),
//
//                        Nature::Continuous(mut nature) => Some(Nature::Continuous(NatureContinuous {
//                            min: column_names.iter().map(|column_name| {
//                                match all_column_names.iter().position(|x| x == column_name) {
//                                    Some(position) => nature.min[position],
//                                    None => panic!("column not found")
//                                }
//                            }).collect(),
//                            max: column_names.iter().map(|column_name| {
//                                match all_column_names.iter().position(|x| x == column_name) {
//                                    Some(position) => nature.max[position],
//                                    None => panic!("column not found")
//                                }
//                            }).collect(),
//                        }))
//                    }
//                }

                // update c stabilities
                data_properties.c_stability = column_names.iter().map(|column_name| {
                    match all_column_names.iter().position(|x: &String| x == column_name) {
                        Some(position) => data_properties.c_stability[position],
                        None => 1.
                    }
                }).collect();

                // update number of columns
                data_properties.num_columns = Some(column_names.len() as i64);

                // update number of records in each column
                data_properties.num_records = column_names.iter().map(|column_name| {
                    match all_column_names.iter().position(|x| x == column_name) {
                        Some(position) => data_properties.num_records[position],
                        None => None
                    }
                }).collect();

                return Ok(data_properties);
            },
            ArrayND::I64(column_indices) => {
                let column_indices = column_indices.iter().map(|v| v.clone()).collect::<Vec<i64>>();

                // update natures
//                if let Some(mut nature) = data_properties.nature.clone() {
//                    data_properties.nature = match nature {
//                        Nature::Categorical(mut nature) => Some(Nature::Categorical(NatureCategorical {
//                            categories: column_indices.iter().map(|column_index| nature.categories[column_index]).collect()
//                        })),
//                        Nature::Continuous(mut nature) => Some(Nature::Continuous(NatureContinuous {
//                            min: column_indices.iter().map(|column_index| nature.min[column_index]).collect(),
//                            max: column_indices.iter().map(|column_index| nature.max[column_index]).collect(),
//                        }))
//                    }
//                }

                // update c stabilities
                data_properties.c_stability = column_indices.iter()
                    .map(|column_index| data_properties.c_stability[*column_index as usize]).collect();

                // update number of columns
                data_properties.num_columns = Some(column_indices.len() as i64);

                // update number of records in each column
                data_properties.num_records = column_indices.iter()
                    .map(|column_index| data_properties.num_records[*column_index as usize]).collect();
                return Ok(data_properties);
            }
            _ => return Err("columns must be strings or integers".into())
        }
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}
