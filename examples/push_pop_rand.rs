use rand::prelude::*;
use rayon::prelude::*;
use rand_distr::Uniform;
use ordered_float::NotNan;
use milkpq::MilkPQ;

fn main() {
    let now = std::time::Instant::now();
    let rands = thread_rng()
        .sample_iter(Uniform::new_inclusive(0.0, 1.0))
        .take(30000)
        .map(|f: f32| NotNan::new(f).unwrap())
        .collect::<Vec<_>>();
    
    let mpq = rands.clone().into_iter().collect::<MilkPQ<_>>();

    rayon::join(
        || rands.clone().into_par_iter().for_each(|f| mpq.push(f)),
        || rands.clone().into_par_iter().for_each(|f| mpq.push(f)),
    );
    
    rayon::join(
        || while mpq.pop().is_some() {},
        || while mpq.pop().is_some() {},
    );

    rayon::join(
        || rands.into_par_iter().for_each(|f| mpq.push(f)),
        || while mpq.pop().is_some() || mpq.pop().is_some() {},
    );

    let out = mpq.into_sorted_vec();

    println!("{}", out.len());
    println!("{}ms", now.elapsed().as_millis());
}
