use ndarray::{ArrayD, Array};
use ndarray;
use whitenoise_validator::{Integer, Float};
use crate::utilities::noise::sample_uniform_mpfr;


fn generate_centroids(data: &ArrayD<Float>, k: Integer, r_min: Float, r_max: Float) -> Vec<Vec<rug::Float>> {
    let mut centroids: Vec<Vec<rug::Float>> = Vec::new();
    for _ in 0..k {
        centroids.push(Vec::new());
        let i = centroids.len() - 1;
        for _ in data.gencolumns() {
            let sample = sample_uniform_mpfr(r_min, r_max).unwrap();
            centroids[i].push(sample);
        }
    }
    centroids
}

fn find_closest_centroid(data_point: &ArrayD<Float>, centroids: Vec<Vec<rug::Float>>) -> Integer {
    let mut min_distance = Float::INFINITY;
    let mut centroid: Integer = -1;
    for i in 0..centroids.len() {
        let c_array = Array::from(centroids[i].to_owned());
        let current_distance = (c_array - data_point).len() as Float;
        if current_distance < min_distance {
            centroid = i as Integer;
            min_distance = current_distance;
        }
    }
    centroid
}

pub fn kmeans(data: &ArrayD<Float>, k: Integer, r_min: Float, r_max: Float) {
    let _centroids = generate_centroids(data, k, r_min, r_max);
}

#[cfg(test)]
pub mod test_clustering {

    use ndarray::{arr2, arr1};
    use crate::components::clustering::{generate_centroids, find_closest_centroid};
    use whitenoise_validator::Integer;

    #[test]
    pub fn test_generate_centroids() {
        let data = arr2(
            &[[0., 1., 2.],
                  [3., 4., 5.]]).into_dyn();
        let k = 2;
        let centroids = generate_centroids(&data, k, 0., 5.).to_owned();
        let centroid_count = centroids.len() as Integer;

        println!("Centroids: ");
        for cent in centroids.clone() {
            println!("{:?}", cent);
        }
        assert_eq!(centroid_count, k);
    }

    #[test]
    pub fn test_find_nearest_centroid() {
        use rug::Float;

        let mut centroids: Vec<Vec<rug::Float>> = Vec::new();
        centroids.push(Vec::new());
        for _ in 0..3 {
            centroids[0].push(Float::with_val(32, 0.0));
        }
        centroids.push(Vec::new());
        for _ in 0..3 {
            centroids[1].push(Float::with_val(32, 10.0));
        }

        let data = arr1(&[1., 1., 1.]).into_dyn();
        let nearest_centroid = find_closest_centroid(&data, centroids);
        assert_eq!(nearest_centroid, 0);
    }
}