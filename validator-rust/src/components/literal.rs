
use crate::errors::*;
use crate::components::{Named, Component};
use std::collections::HashMap;
use crate::utilities::get_ith_column;
use ndarray::ArrayD;
use crate::{proto, base, Warnable};
use crate::base::{Value, Array, ValueProperties, ArrayProperties, DataType};

impl Component for proto::Literal {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: &HashMap<String, Value>,
        _properties: &base::NodeProperties,
        node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        Ok(ValueProperties::Array(ArrayProperties {
            num_records: None,
            num_columns: None,
            nullity: true,
            releasable: false,
            c_stability: vec![],
            aggregator: None,
            nature: None,
            data_type: DataType::Unknown,
            dataset_id: Some(node_id as i64),
            // this is a library-wide assumption - that datasets initially have more than zero rows
            is_not_empty: true,
            dimensionality: None,
        }).into())
    }
}


impl Named for proto::Literal {
    fn get_names(
        &self,
        _public_arguments: &HashMap<String, Value>,
        _argument_variables: &HashMap<String, Vec<String>>,
        release: Option<&Value>
    ) -> Result<Vec<String>> {

        fn array_to_names<T: ToString + Clone + Default>(array: &ArrayD<T>, num_columns: i64) -> Result<Vec<String>> {
            (0..num_columns as usize)
                .map(|index| {
                    let array = get_ith_column(array, &index)?;
                    match array.ndim() {
                        0 => match array.first() {
                            Some(value) => Ok(value.to_string()),
                            None => Err("array may not be empty".into())
                        },
                        1 => Ok("[Literal Column]".into()),
                        _ => Err("array has too great of a dimension".into())
                    }
                })
                .collect::<Result<Vec<String>>>()
        }

        match release {
            Some(release) => match release {
                Value::Jagged(jagged) => Ok((0..jagged.num_columns()).map(|_| "[Literal vector]".to_string()).collect()),
                Value::Indexmap(_) => Err("names for indexmap literals are not supported".into()),  // (or necessary)
                Value::Array(value) => match value {
                    Array::F64(array) => array_to_names(array, value.num_columns()?),
                    Array::I64(array) => array_to_names(array, value.num_columns()?),
                    Array::Str(array) => array_to_names(array, value.num_columns()?),
                    Array::Bool(array) => array_to_names(array, value.num_columns()?),
                },
                Value::Function(_function) => Ok(vec![])
            },
            None => Err("Literals must always be accompanied by a release".into())
        }
    }
}



#[cfg(test)]
pub mod test_literal {
    use crate::base::Value;
    use crate::bindings::Analysis;
    use crate::base::test_data::array1d_f64_10_uniform;

    pub fn analysis_literal(value: Value, public: bool) -> (Analysis, u32) {
        let mut analysis = Analysis::new();
        let literal = analysis.literal()
            .value(value).value_public(public)
            .build();
        (analysis, literal)
    }

    #[test]
    fn test_literal() {
        let (analysis, literal) = analysis_literal(array1d_f64_10_uniform(), true);

        analysis.properties(literal).unwrap();
    }
}