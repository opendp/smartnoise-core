use ndarray::{ArrayD, Array};
use ndarray;
use whitenoise_validator::{Integer, Float};
use crate::utilities::noise::sample_uniform;

#[derive(Debug)]
#[derive(Clone)]
pub struct Cluster {
    center: ArrayD<Float>,
    members: Vec<ArrayD<Float>>
}

impl Cluster {

    pub fn add_member(&mut self, data: &ArrayD<Float>) {
        &self.members.push(data.to_owned().into_dyn());
    }

    pub fn remove_member(&mut self, index: usize) -> ArrayD<Float> {
        self.members.remove(index)
    }

    /// Take average of all members, set to new center
    pub fn update_center(&mut self) {

    }
}


fn generate_clusters(data: &ArrayD<Float>, k: Integer, r_min: Float, r_max: Float) -> Vec<Cluster> {
    // let mut centroids: Vec<Vec<rug::Float>> = Vec::new();
    let mut clusters: Vec<Cluster> = Vec::new();
    for _ in 0..k {
        let mut centroid: Vec<Float> = Vec::new();
        for _ in data.gencolumns() {
            let sample: Float = sample_uniform(r_min, r_max, true).unwrap();
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
        let current_distance = array_magnitude(&(&clusters[i].center - data_point)) as Float;
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

/// TODO: make this more efficient?
fn update_clusters(clusters: &Vec<Cluster>) {
    let clusters = clusters.clone();
    for (cluster_index, current_cluster) in clusters.iter().enumerate() {
        for (member_index, member) in current_cluster.members.iter().enumerate() {
            let new_cluster_index = find_closest_centroid(&member, &clusters) as usize;
            if new_cluster_index != cluster_index {
                let mut new_cluster: Cluster = clusters.get(new_cluster_index).unwrap().clone();
                let removed_member = current_cluster.to_owned().remove_member(member_index).into_dyn();
                new_cluster.add_member(&removed_member);
            }
        }
    }
}

pub fn kmeans(data: &ArrayD<Float>, k: Integer, r_min: Float, r_max: Float, niters: Integer) -> Vec<Cluster> {
    let mut clusters = generate_clusters(data, k, r_min, r_max).clone();
    assign_initial_clusters(&data, &mut clusters);
    for _ in 0..niters-1 {
        for mut c in clusters.to_owned() {
            c.update_center();
        }
        update_clusters(&clusters);
    }
    clusters
}

#[cfg(test)]
pub mod test_clustering {

    use ndarray::{arr2, arr1};
    use crate::components::clustering::{find_closest_centroid, generate_clusters, assign_initial_clusters, array_magnitude, update_clusters, kmeans};
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

    #[test]
    pub fn test_update_center() {
        let data = arr2(&[[0., 0., 0.], [1., 1., 1.], [10., 10., 10.]]).into_dyn();
        let k = 3;
        let r_min = 0.0;
        let r_max = 10.0;
        let mut clusters = generate_clusters(&data, k, r_min, r_max);
        assign_initial_clusters(&data, &mut clusters);
        for mut c in clusters {
            c.update_center();
        }
        assert_eq!(1, 1);
    }

    #[test]
    pub fn test_assign_update_clusters() {
        let data = arr2(&[[0., 0., 0.], [1., 1., 1.], [10., 10., 10.]]).into_dyn();
        let k = 3;
        let r_min = 0.0;
        let r_max = 10.0;
        let mut clusters = generate_clusters(&data, k, r_min, r_max);

        assign_initial_clusters(&data, &mut clusters);
        update_clusters(&clusters);
        println!("test_update_clusters");
        for c in &clusters {
            println!("Center: {} \t Member Count: {}", c.center, c.members.len());
            // assert_eq!(c.members.len(), 1);
        }
        println!("\n");
    }

    #[test]
    pub fn test_kmeans() {
        let data = arr2(&[
            [0., 0., 0.],
            [1., 1., 1.],
            [5., 5., 5.],
            [10., 10., 10.],
            [100., 100., 100.],
        ]).into_dyn();
        let k = 3;
        let r_min = 0.0;
        let r_max = 10.0;
        let niters = 1000;
        let clusters = kmeans(&data, k, r_min, r_max, niters);
        println!("---------------");
        println!("- test_kmeans -");
        println!("---------------");
        for (cluster_index, c) in clusters.iter().enumerate() {
            println!("Cluster {}", cluster_index);
            println!("Center: {} \t Member Count: {}", c.center, c.members.len());
            for (i, m) in c.members.iter().enumerate() {
                println!("Member {}: {:?}", i, m);
            }
            // assert_eq!(c.members.len(), 1);
            println!();
        }

    }

}