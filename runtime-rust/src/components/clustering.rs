use ndarray::ArrayD;
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

pub fn kmeans(data: &ArrayD<Float>, k: Integer, r_min: Float, r_max: Float) {
    let _centroids = generate_centroids(data, k, r_min, r_max);
}

#[cfg(test)]
pub mod test_clustering {

    use ndarray::arr2;
    use crate::components::clustering::generate_centroids;
    use whitenoise_validator::Integer;

    #[test]
    pub fn test_centroids() {
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
}