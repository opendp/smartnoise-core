use ndarray::{ArrayD, Array};
use ndarray;
use whitenoise_validator::{Integer, Float};
use crate::utilities::noise::sample_uniform_mpfr;

#[derive(Debug)]
struct Cluster {
    center: ArrayD<rug::Float>,
    members: Vec<ArrayD<Float>>
}

impl Cluster {
    pub fn add_member(&mut self, data: &ArrayD<Float>) {
        &self.members.push(data.to_owned().into_dyn());
    }
}


fn generate_clusters(data: &ArrayD<Float>, k: Integer, r_min: Float, r_max: Float) -> Vec<Cluster> {
    // let mut centroids: Vec<Vec<rug::Float>> = Vec::new();
    let mut clusters: Vec<Cluster> = Vec::new();
    for _ in 0..k {
        let mut centroid: Vec<rug::Float> = Vec::new();
        for _ in data.gencolumns() {
            let sample = sample_uniform_mpfr(r_min, r_max).unwrap();
            centroid.push(sample);
        }
        let new_cluster = Cluster{ center: Array::from(centroid).into_dyn(), members: vec![] };
        clusters.push(new_cluster);
    }
    clusters
}

/// Euclidean magnitude of array
///
fn array_magnitude(data: &ArrayD<Float>) -> Float {
    let mut magnitude: Float = 0.0;
    for row in data.genrows() {
        magnitude += row.map(|x| x*x).sum();
    }
    magnitude
}

fn find_closest_centroid(data_point: &ArrayD<Float>, clusters: &Vec<Cluster>) -> Integer {
    let mut min_distance = Float::INFINITY;
    let mut cluster: Integer = -1;
    for i in 0..clusters.len() {
        let current_distance = (&clusters[i].center - data_point).len() as Float;
        if current_distance < min_distance {
            cluster = i as Integer;
            min_distance = current_distance;
        }
    }
    cluster
}


/// Given a vector of Clusters, match each data point with its closest cluster center
///
fn assign_initial_clusters(data: &ArrayD<Float>, clusters: &mut Vec<Cluster>) {
    for d in data.genrows().into_iter() {
        let cluster_id = find_closest_centroid(&d.to_owned().into_dyn(), &clusters) as usize;
        clusters[cluster_id].add_member(&d.to_owned().into_dyn());
    }
}

// fn update_centroids(cluster_map: HashMap<&ArrayD<Float>, Integer>, centroids: Vec<Vec<rug::Float>>) -> Vec<Vec<rug::Float>> {
// }

pub fn kmeans(data: &ArrayD<Float>, k: Integer, r_min: Float, r_max: Float) {
    let _clusters = generate_clusters(data, k, r_min, r_max);
}

#[cfg(test)]
pub mod test_clustering {

    use ndarray::{arr2, arr1};
    use crate::components::clustering::{find_closest_centroid, generate_clusters, assign_initial_clusters, array_magnitude};
    use whitenoise_validator::Integer;
    // use rug::Float;

    #[test]
    pub fn test_array_magnitude() {
        let data = arr1(&[0., 1., 2.]).into_dyn();
        assert_eq!(array_magnitude(&data), 5.0);
    }

    #[test]
    pub fn test_generate_clusters() {
        let data = arr2(
            &[[0., 1., 2.],
                  [3., 4., 5.]]).into_dyn();
        let k = 2;
        let clusters = generate_clusters(&data, k, 0., 5.);
        let cluster_count = clusters.len() as Integer;

        println!("test_generate_clusters");
        println!("Centroids: ");
        for c in clusters {
            println!("{:?}", c.center);
        }
        println!("\n");

        assert_eq!(cluster_count, k);
    }

    #[test]
    pub fn test_find_nearest_centroid() {

        let data = arr2(&[[0., 0., 0.], [1., 1., 1.], [10., 10., 10.]]).into_dyn();
        let k = 3;
        let r_min = 0.0;
        let r_max = 10.0;
        let clusters = generate_clusters(&data, k, r_min, r_max);

        let nearest_centroid = find_closest_centroid(&arr1(&[1., 1., 1.]).into_dyn(), &clusters);

        // TODO: Make this actually test something
        assert_eq!(nearest_centroid, 0);
    }

    #[test]
    pub fn test_assign_initial_clusters() {
        let data = arr2(&[[0., 0., 0.], [1., 1., 1.], [10., 10., 10.]]).into_dyn();
        let k = 3;
        let r_min = 0.0;
        let r_max = 10.0;
        let mut clusters = generate_clusters(&data, k, r_min, r_max);

        assign_initial_clusters(&data, &mut clusters);

        println!("test_assign_initial_clusters");
        for c in &clusters {
            println!("Center: {} \t Member Count: {}", c.center, c.members.len());
            // assert_eq!(c.members.len(), 1);
        }
        println!("\n");
    }

}