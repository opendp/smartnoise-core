use whitenoise_validator::errors::*;

use ndarray::prelude::*;
use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, Hashmap};
use crate::components::Evaluable;
use std::collections::HashMap;
use whitenoise_validator::utilities::serial::parse_value;
use ndarray::Array;
use whitenoise_validator::proto;

impl Evaluable for proto::Materialize {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {

        let column_names = arguments.get("column_names")
            .and_then(|column_names| column_names.array().ok()?.string().ok()).cloned();

        let data_source = self.data_source.clone()
            .ok_or::<Error>("data source must be supplied".into())?;
        
        match data_source.value.as_ref().unwrap() {
            proto::data_source::Value::Literal(value) => parse_value(value),
            proto::data_source::Value::FilePath(path) => {
                let mut response = HashMap::<String, Vec<String>>::new();

                let mut reader = match csv::Reader::from_path(path) {
                    Ok(reader) => reader,
                    Err(_) => return Err("provided file path could not be found".into())
                };
                if let Some(column_names) = column_names {
                    let column_names = match column_names.into_dimensionality::<Ix1>() {
                        Ok(column_names) => column_names,
                        Err(_) => return Err("column names must be one-dimensional".into())
                    };

                    reader.set_headers(csv::StringRecord::from(column_names.to_vec()))
                }

                // parse from csv into response
                reader.deserialize()
                    .map(|result| {
                        // parse each record into the whitenoise internal format
                        let record: HashMap<String, String> = result.unwrap();
                        record.iter().for_each(|(k, v)| response
                            .entry(k.to_owned()).or_insert_with(Vec::new)
                            .push(v.clone()));
                        Ok(())
                    }).collect::<Result<()>>()?;


                // convert hashmap of vecs into arrays
                Ok(Value::Hashmap(Hashmap::Str(response.iter()
                    .map(|(k, v): (&String, &Vec<String>)| (
                        k.clone(), Array::from(v.to_owned()).into_dyn().into()
                    ))
                    .collect::<HashMap<String, Value>>())))
            }
            _ => Err("the selected table reference format is not implemented".into())
        }
    }
}
