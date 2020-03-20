use whitenoise_validator::errors::*;


// TODO: open an issue on the rust ndarray package
//  Requiring the Copy trait on stack makes this (necessary) part of ndarray unusable
//  Pulled from the ndarray library, and tweaked to remove the Copy trait requirement

use ndarray::{RemoveAxis, Axis, Ix, Array, ArrayView};

use itertools::zip;

pub fn stack<A, D>(
    axis: Axis,
    arrays: &[ArrayView<A, D>],
) -> Result<Array<A, D>>
    where
        A: Clone,
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

    // we can safely use uninitialized values here because they are Copy
    // and we will only ever write to them
    let size = res_dim.size();
    let mut v = Vec::with_capacity(size);
    unsafe {
        v.set_len(size);
    }
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

pub fn select<A, D>(data: &Array<A, D>, axis: Axis, indices: &[Ix]) -> Array<A, D>
    where
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
        unsafe { Array::from_shape_vec_unchecked(dim, vec![]) }
    } else {
        stack(axis, &subs).unwrap()
    }
}