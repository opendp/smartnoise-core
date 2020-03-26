use crate::errors::*;


// TODO: open an issue on the rust ndarray package
//  Requiring the Copy trait on stack makes strings un-stackable/un-selectable
//  Pulled from the ndarray library, and tweaked to remove the Copy trait requirement

use ndarray::{RemoveAxis, Axis, Ix, Array, ArrayView};

use itertools::zip;

pub fn slow_stack<A, D>(
    axis: Axis,
    arrays: &[ArrayView<A, D>],
) -> Result<Array<A, D>>
    where
        A: Clone,
        A: Default,
        D: RemoveAxis,
{
    if arrays.is_empty() {
        return Err("shape error when stacking, array is empty".into());
    }
    let mut res_dim = arrays[0].raw_dim();
    if axis.index() >= res_dim.ndim() {
        return Err("shape error when stacking, out of bounds".into());
    }
    let common_dim = res_dim.remove_axis(axis);
    if arrays
        .iter()
        .any(|a| a.raw_dim().remove_axis(axis) != common_dim)
    {
        return Err("shape error when stacking, incompatible shape".into());
    }

    let stacked_dim = arrays.iter().fold(0, |acc, a| acc + a.len_of(axis));
    res_dim[axis.index()] = stacked_dim;

    let size = res_dim.size();
    let v = (0..size).map(|_| A::default()).collect();

    let mut res = match Array::from_shape_vec(res_dim, v) {
        Ok(v) => v, Err(_) => return Err("shape error when stacking, could not create array from shape vec".into())
    };

    {
        let mut assign_view = res.view_mut();
        for array in arrays {
            let len = array.len_of(axis);
            let (mut front, rest) = assign_view.split_at(axis, len);
            front.assign(array);
            assign_view = rest;
        }
    }
    Ok(res)
}

pub fn slow_select<A, D>(data: &Array<A, D>, axis: Axis, indices: &[Ix]) -> Array<A, D>
    where
        A: Default,
        A: Clone,
        D: RemoveAxis,
{
    let mut subs = vec![data.view(); indices.len()];
    for (&i, sub) in zip(indices, &mut subs[..]) {
        sub.collapse_axis(axis, i);
    }
    if subs.is_empty() {
        let mut dim = data.raw_dim();
        dim[axis.index()] = 0;
        Array::from_shape_vec(dim, vec![]).unwrap()
    } else {
        slow_stack(axis, &subs).unwrap()
    }
}