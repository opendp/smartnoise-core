use crate::errors::*;

use crate::{proto, base};

use crate::components::{Expandable};
use crate::base::{ValueProperties, IndexKey, Value};
use crate::utilities::{get_literal};
use crate::utilities::inference::infer_property;
use indexmap::set::IndexSet;
use indexmap::map::IndexMap;


impl Expandable for proto::Map {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: u32,
        mut maximum_id: u32
    ) -> Result<base::ComponentExpansion> {
        let mut expansion = base::ComponentExpansion::default();

        let mapped_component = self.component.as_ref()
            .ok_or_else(|| "component must be defined")?;

        let (props_partitioned, props_singular): (
            IndexMap<IndexKey, ValueProperties>,
            IndexMap<IndexKey, ValueProperties>
        ) = properties.clone().into_iter()
            .partition(|(_, props)| props.partition().is_ok());

        let indexes = props_partitioned.values()
            .map(|v| Ok(v.partition()?.children.keys().collect()))
            .collect::<Result<Vec<Vec<&IndexKey>>>>()?.into_iter().flatten()
            .collect::<IndexSet<&IndexKey>>();

        let arguments = component.arguments();

        // for each partition
        let union_arguments = indexes.into_iter()
            // for each argument
            .map(|partition_idx| Ok((
                partition_idx.clone(),
                props_partitioned.iter()
                    .map(|(name, _)| {
                        maximum_id += 1;
                        let id_index_name = maximum_id;
                        let (patch_node, release) = get_literal(Value::from_index_key(partition_idx.clone())?, component.submission)?;
                        expansion.computation_graph.insert(id_index_name, patch_node);
                        expansion.properties.insert(id_index_name, infer_property(&release.value, None)?);
                        expansion.releases.insert(id_index_name, release);

                        maximum_id += 1;
                        let id_index = maximum_id;
                        let id_data = *arguments.get(name)
                            .ok_or_else(|| Error::from(format!("{:?}: missing from component arguments", name)))?;
                        expansion.computation_graph.insert(id_index, proto::Component {
                            arguments: Some(proto::ArgumentNodeIds::new(indexmap!["data".into() => id_data, "names".into() => id_index_name])),
                            omit: true,
                            submission: component.submission,
                            variant: Some(proto::component::Variant::Index(proto::Index {})),
                        });
                        expansion.traversal.push(id_index);

                        Ok((name.clone(), id_index))
                    })
                    .chain(props_singular.clone().into_iter().map(|(name, _)| {
                        let id_arg = arguments.get(&name).ok_or_else(||
                            format!("{:?}: missing from component arguments", name))?;
                        Ok((name, *id_arg))
                    }))
                    .collect::<Result<IndexMap<IndexKey, u32>>>()?
            )))
            .collect::<Result<IndexMap<IndexKey, IndexMap<IndexKey, u32>>>>()?.into_iter()
            .map(|(partition_idx, arguments)| {

                maximum_id += 1;
                let id_inner_component = maximum_id;
                let mut inner_component = *mapped_component.clone();

                inner_component.arguments = Some(proto::ArgumentNodeIds::new(arguments));
                inner_component.omit = true;

                expansion.computation_graph.insert(id_inner_component, inner_component);
                expansion.traversal.push(id_inner_component);

                Ok((partition_idx, id_inner_component))
            })
            .collect::<Result<IndexMap<IndexKey, u32>>>()?;

        expansion.computation_graph.insert(component_id, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(union_arguments)),
            omit: component.omit,
            submission: component.submission,
            variant: Some(proto::component::Variant::Union(proto::Union {
                flatten: false
            }))
        });
        expansion.traversal.push(component_id);

        Ok(expansion)
    }
}
