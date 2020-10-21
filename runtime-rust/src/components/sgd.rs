use whitenoise_validator::{proto, Float, Integer};
use crate::utilities::noise::{sample_bit_prob, sample_uniform, sample_gaussian_mpfr};
use ndarray::ArrayD;
use whitenoise_runtime::utilities::noise::sample_uniform;
use std::ops::Deref;
use std::cmp;


fn calculate_gradient(theta: &Vec<Float>, x: &Vec<Float>) -> &Vec<Float> {
    x
}

fn sgd(data: &ArrayD<Float>, data_size: Integer, learning_rate: Float, noise_scale: Float, group_size: Integer,
       gradient_norm_bound: Float, max_iters: Integer) {

    // Initialize theta_0 randomly
    let mut theta: Vec<Float> = Vec::new();
    for i in 0..data_size-1 {
        let min: Float = 0.0;
        let max: Float = 1.0;
        let enforce_constant_time = true;
        theta.push(sample_uniform(min, max, enforce_constant_time).unwrap());
    }

    for t in 0..max_iters {
        // Random Sample with P = L/N
        let mut L: Vec<Float> = Vec::new();
        for _ in 0..group_size-1 {
            // TODO: ??
            // L.push();
        }
        // Compute gradient
        let mut gradients: Vec<Vec<Float>> = Vec::new();
        for _ in 0..L.size()-1 {
            let &grad = calculate_gradient(&theta, &x).to_vec();
            gradients.push(*grad);
        }
        // Clip gradient
        let mut clipped_gradients = gradients.clone();
        let gradients_magnitude = {
            clipped_gradients.clone().iter().map(|x| x^2).sum().collect()
        };
        for j in 0..L.size()-1 {
            clipped_gradients[j] = gradients[j] / cmp::max(1.0,
                                                   gradients_magnitude/gradient_norm_bound)
        }
        // Add noise
        let mut noisy_gradients = Vec::new();
        let noise = sample_gaussian_mpfr(0, noise_scale^2 * gradient_norm_bound^2);
        for _ in 0..L.size()-1 {
            for _ in 0..data_size-1 {
                let mut noisy_grad = (1/group_size) * )
                noisy_gradients.push(
            }
        }
        // Descent
    }
}