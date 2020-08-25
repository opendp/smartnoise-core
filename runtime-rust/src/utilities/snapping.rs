use ieee754::Ieee754;

use whitenoise_validator::errors::*;

/// Finds the smallest integer m such that 2^m is equal to or greater than x.
///
/// # Arguments
/// * `x` - The number for which we want the next power of two.
///
/// # Returns
/// The found power of two
pub fn get_smallest_greater_or_eq_power_of_two(x: f64) -> i16 {
    let (_sign, exponent, mantissa) = x.decompose();
    exponent + if mantissa == 0 {0} else {1}
}

#[cfg(test)]
pub mod test_get_smallest_greater_or_eq_power_of_two {
    use crate::utilities::snapping::get_smallest_greater_or_eq_power_of_two;

    #[test]
    fn test() {
        (0..1000)
            .map(|i| i as f64 / 100.)
            .for_each(|v| {
                let route_1 = v.log2().floor();
                let route_2 = get_smallest_greater_or_eq_power_of_two(v);
                println!("{:?}, {:?}, {:?}", v, route_1, route_2);
            })
    }
}

/// Finds the closest number to x that is a multiple of Lambda.
/// 
/// # Arguments
/// * `x` - Number to be rounded to closest multiple of Lambda.
/// * `m` - Integer such that Lambda = 2^m.
///
/// # Returns
/// Closest multiple of Lambda to x.
pub fn get_closest_multiple_of_lambda(x: f64, m: i16) -> Result<f64> {
    let (sign, mut exponent, mantissa) = x.decompose();
    exponent -= m;

    let (sign, mut exponent, mantissa) = match exponent {
        // original components already represent an integer (decimal shifted >= 52 places on mantissa)
        exponent if exponent >= 52 => (sign, exponent, mantissa),
        // round int to +- 1
        exponent if exponent == -1 => (sign, 0, 0),
        // round int to 0, and keep it zero after adding m
        exponent if exponent < -1 => (sign, -1023 - m, 0),
        // round to int when decimal is within range of mantissa
        _ => {
            // get elements of mantissa that represent integers (after decimal is shifted by "exponent" places)
            //     shift 1 "exponent" places to the left (no overflow because exponent < 64)
            //     subtract one to set "exponent" bits to one
            //     shift the mask to the left for a 52-bit mask that keeps the top #"exponent" bits
            let integer_mask: u64 = ((1u64 << exponent) - 1) << (52 - exponent);
            let integer_mantissa: u64 = mantissa & integer_mask;

            // check if digit after exponent point is set
            if mantissa & (1u64 << (52 - (exponent + 1))) == 0u64 {
                (sign, exponent, integer_mantissa)
            } else {
                // if integer part of mantissa is all 1s, rounding needs to be reflected in the exponent instead
                if integer_mantissa == integer_mask {
                    (sign, exponent + 1, 0)
                } else {
                    (sign, exponent, integer_mantissa + (1u64 << (52 - exponent)))
                }
            }
        }
    };

    exponent += m;
    Ok(f64::recompose(sign, exponent, mantissa))
}

#[cfg(test)]
mod test_get_closest_multiple_of_lambda {
    use ieee754::Ieee754;

    use crate::utilities::snapping::get_closest_multiple_of_lambda;
    use whitenoise_validator::hashmap;

    #[test]
    fn test_get_closest_multiple_of_lambda_range() {
        (0..100).for_each(|i| {
            let x = 1. - 0.01 * (i as f64);
            println!("{}: {}", x, get_closest_multiple_of_lambda(x, -1).unwrap())
        });
    }

    #[test]
    fn test_get_closest_multiple_of_lambda() {
        let input = vec![-30.01, -2.51, -1.01, -0.76, -0.51, -0.26, 0.0, 0.26, 0.51, 0.76, 1.01, 2.51, 30.01];

        hashmap![
            -2 => vec![-30., -2.5, -1.0, -0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0, 2.5, 30.0],
            -1 => vec![-30., -2.5, -1.0, -1.0, -0.5, -0.5, 0.0, 0.5, 0.5, 1.0, 1.0, 2.5, 30.0],
            0 => vec![-30., -3.0, -1.0, -1.0, -1.0, -0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 3.0, 30.0],
            1 => vec![-30., -2.0, -2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.0, 2.0, 30.0],
            2 => vec![-32., -4.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 4.0, 32.0]
        ].into_iter().for_each(|(m, outputs)| {
            input.iter().copied().zip(outputs.into_iter())
                .for_each(|(input, expected)| {
                    let actual = get_closest_multiple_of_lambda(input, m).unwrap();
                    println!("m: {:?}, input: {:?}, actual: {:?}, expected: {:?}",
                             m, input, actual, expected);
                    assert_eq!(actual, expected)
                })
        });
    }

    #[test]
    fn test_recompose() {
        println!("{:?}", f64::recompose(false, 0, 0));
        println!("{:?}", f64::recompose(false, -1023, 0));
    }
}

/// Gets functional epsilon for Snapping mechanism such that privacy loss does not exceed the user's proposed budget.
/// Described in https://github.com/ctcovington/floating_point/blob/master/snapping_mechanism/notes/snapping_implementation_notes.pdf
/// 
/// # Arguments
/// * `epsilon` - Desired privacy guarantee.
/// * `b` - Upper bound on function value being privatized.
/// * `precision` - Number of bits of precision to which arithmetic inside the mechanism has access.
///
/// # Returns
/// Functional epsilon that will determine amount of noise.
pub fn redefine_epsilon(epsilon: f64, b: f64, precision: u32) -> f64 {
    let eta = 2_f64.powi(-(precision as i32));
    (epsilon - 2.0 * eta) / (1.0 + 12.0 * b * eta)
}

/// Finds accuracy that is achievable given desired epsilon and confidence requirements. Described in
/// https://github.com/ctcovington/floating_point/blob/master/snapping_mechanism/notes/snapping_implementation_notes.pdf
///
/// # Arguments
/// * `alpha` - Desired confidence level.
/// * `epsilon` - Desired privacy guarantee.
/// * `sensitivity` - l1 Sensitivity of function to which mechanism is being applied.
/// * `B` - Upper bound on function value being privatized.
/// * `precision` - Number of bits of precision to which arithmetic inside the mechanism has access.
///
/// # Returns
/// Epsilon use for the Snapping mechanism.
pub fn epsilon_to_accuracy(
    alpha: f64, epsilon: f64, sensitivity: f64, b: f64, precision: u32
) -> f64 {
    (1.0 + (1.0 / alpha).ln())
        / redefine_epsilon(epsilon, b, precision)
        * sensitivity
}

/// Finds epsilon that will achieve desired accuracy and confidence requirements. Described in 
/// https://github.com/ctcovington/floating_point/blob/master/snapping_mechanism/notes/snapping_implementation_notes.pdf
///
/// # Arguments
/// * `accuracy` - Desired accuracy level.
/// * `alpha` - Desired confidence level.
/// * `sensitivity` - l1 Sensitivity of function to which mechanism is being applied.
/// * `B` - Upper bound on function value being privatized.
/// * `precision` - Number of bits of precision to which arithmetic inside the mechanism has access.
///
/// # Returns
/// Epsilon use for the Snapping mechanism.
pub fn accuracy_to_epsilon(
    accuracy: f64, alpha: f64, sensitivity: f64, b: f64, precision: u32
) -> f64 {
    let eta = 2_f64.powi(-(precision as i32));
    (1.0 + 12.0 * b * eta) / accuracy
        * (1.0 + (1.0 / alpha).ln()) * sensitivity
        + 2. * eta
}
