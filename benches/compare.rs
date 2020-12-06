use std::collections::BinaryHeap;
use std::any::type_name;
use rayon::prelude::*;
use milkpq::MilkPQ;
use parking_lot::Mutex;
use rand::prelude::*;
use criterion::{
    Criterion, criterion_group, criterion_main, BenchmarkId, BenchmarkGroup,
    measurement::Measurement, BatchSize, PlotConfiguration, AxisScale::Logarithmic,
};

type MPQ = MilkPQ<usize>;

struct SPQ(Mutex<BinaryHeap<usize>>);

impl Clone for SPQ {
    fn clone(&self) -> Self {
        let bheap = self.0.lock();
        SPQ(Mutex::new(bheap.clone()))
    }

    fn clone_from(&mut self, source: &Self) {
        self.0.get_mut().clone_from(&source.0.lock());
    }
}

trait PQueue<T: Send + Sync>: Clone + Send + Sync {
    fn new_rand(n: usize) -> Self;

    fn pop(&self) -> Option<T>;

    fn strong_pop(&self) -> Option<T>;

    fn push(&self, t: T);

    fn once_pop(&self) {
        self.pop();
    }

    fn seq_pop(&self, n: usize) {
        for _ in 0..n {
            self.pop();
        }
    }

    fn par_pop(&self, n: usize) {
        (0..n).into_par_iter()
            .for_each(|_| { self.pop(); });
    }

    fn once_strong_pop(&self) {
        self.strong_pop();
    }
    
    fn seq_strong_pop(&self, n: usize) {
        for _ in 0..n {
            self.strong_pop();
        }
    }
    
    fn par_strong_pop(&self, n: usize) {
        (0..n).into_par_iter()
            .for_each(|_| { self.strong_pop(); });
    }
    
    fn once_push(&self, t: T) {
        self.push(t);
    }
    
    fn seq_push<I: Iterator<Item = T>>(&self, iter: I) {
        for t in iter {
            self.push(t);
        }
    }
    
    fn par_push<I: ParallelIterator<Item = T>>(&self, iter: I) {
        iter.for_each(|t| { self.push(t); });
    }
    
    fn seq_mix<I: Iterator<Item = T>>(&self, iter: I, mut pops: usize) {
        for t in iter {
            self.push(t);
            
            if pops > 0 {
                pops -= 1;
                self.pop();
            }
        }
    }
    
    fn seq_seq_mix<I: Iterator<Item = T> + Send>(&self, iter: I, pops: usize) {
        rayon::join(
            || for t in iter {
                self.push(t);
            },
            || for _ in 0..pops {
                self.pop();
            },
        );
    }
    
    fn seq_par_mix<I: Iterator<Item = T> + Send>(&self, iter: I, pops: usize) {
        rayon::join(
            || for t in iter {
                self.push(t);
            },
            || (0..pops).into_par_iter().for_each(|_| { self.pop(); }),
        );
    }
    
    fn par_seq_mix<I: ParallelIterator<Item = T>>(&self, iter: I, pops: usize) {
        rayon::join(
            || iter.for_each(|t| self.push(t)),
            || for _ in 0..pops {
                self.pop();
            },
        );
    }
    
    fn par_par_mix<I: ParallelIterator<Item = T>>(&self, iter: I, pops: usize) {
        rayon::join(
            || iter.for_each(|t| self.push(t)),
            || (0..pops).into_par_iter().for_each(|_| { self.pop(); }),
        );
    }
}

impl PQueue<usize> for MPQ {
    fn new_rand(n: usize) -> Self {
        let mut vs = (0..n).collect::<Vec<_>>();
        vs.shuffle(&mut thread_rng());
        vs.into_iter().collect()
    }

    fn pop(&self) -> Option<usize> {
        self.pop()
    }

    fn strong_pop(&self) -> Option<usize> {
        self.strong_pop()
    }

    fn push(&self, t: usize) {
        self.push(t);
    }
}

impl PQueue<usize> for SPQ {
    fn new_rand(n: usize) -> Self {
        let mut vs = (0..n).collect::<Vec<_>>();
        vs.shuffle(&mut thread_rng());
        SPQ(Mutex::new(vs.into_iter().collect()))
    }

    fn pop(&self) -> Option<usize> {
        self.0.lock().pop()
    }

    fn strong_pop(&self) -> Option<usize> {
        self.0.lock().pop()
    }

    fn push(&self, t: usize) {
        self.0.lock().push(t);
    }
}

const SIZES: &[usize] = &[
    0, 1, 2, 3, 4, 5,
    10, 15, 20, 50, 80,
    100, 150, 200, 500, 800, 
    1000, 1500, 2000, 5000, 8000,
    10000, 15000, 20000, 50000, 80000,
    100000,
];
const BIG_THRESHOLD: usize = 23;

fn push_once_bench<T, M, PQ>(group: &mut BenchmarkGroup<M>)
where
    T: Send + Sync + Copy,
    M: Measurement,
    PQ: PQueue<T>,
    rand_distr::Standard: Distribution<T>,
{
    let mut rng = thread_rng();

    for &size in &SIZES[..BIG_THRESHOLD] {
        group.bench_with_input(
            BenchmarkId::new(type_name::<PQ>(), size),
            &PQ::new_rand(size),
            |b, pq| b.iter_batched_ref(
                || (pq.clone(), rng.gen()),
                |(pq, t)| pq.once_push(*t),
                BatchSize::SmallInput,
            ),
        );
    }

    for &size in &SIZES[BIG_THRESHOLD..] {
        group.bench_with_input(
            BenchmarkId::new(type_name::<PQ>(), size),
            &PQ::new_rand(size),
            |b, pq| b.iter_batched_ref(
                || (pq.clone(), rng.gen()),
                |(pq, t)| pq.push(*t),
                BatchSize::LargeInput,
            ),
        );
    }
}

fn pop_once_bench<T, M, PQ>(group: &mut BenchmarkGroup<M>)
where
    T: Send + Sync + Copy,
    M: Measurement,
    PQ: PQueue<T>,
{
    for &size in &SIZES[..BIG_THRESHOLD] {
        group.bench_with_input(
            BenchmarkId::new(type_name::<PQ>(), size),
            &PQ::new_rand(size),
            |b, pq| b.iter_batched_ref(
                || pq.clone(),
                |pq| pq.once_pop(),
                BatchSize::SmallInput,
            ),
        );
    }

    for &size in &SIZES[BIG_THRESHOLD..] {
        group.bench_with_input(
            BenchmarkId::new(type_name::<PQ>(), size),
            &PQ::new_rand(size),
            |b, pq| b.iter_batched_ref(
                || pq.clone(),
                |pq| pq.once_pop(),
                BatchSize::SmallInput,
            ),
        );
    }
}

fn spop_once_bench<T, M, PQ>(group: &mut BenchmarkGroup<M>)
where
    T: Send + Sync + Copy,
    M: Measurement,
    PQ: PQueue<T>,
{
    for &size in &SIZES[..BIG_THRESHOLD] {
        group.bench_with_input(
            BenchmarkId::new(type_name::<PQ>(), size),
            &PQ::new_rand(size),
            |b, pq| b.iter_batched_ref(
                || pq.clone(),
                |pq| pq.once_strong_pop(),
                BatchSize::SmallInput,
            ),
        );
    }

    for &size in &SIZES[BIG_THRESHOLD..] {
        group.bench_with_input(
            BenchmarkId::new(type_name::<PQ>(), size),
            &PQ::new_rand(size),
            |b, pq| b.iter_batched_ref(
                || pq.clone(),
                |pq| pq.once_strong_pop(),
                BatchSize::SmallInput,
            ),
        );
    }
}

fn compare_once(c: &mut Criterion) {
    let pc = PlotConfiguration::default().summary_scale(Logarithmic);
    {
        let mut group = c.benchmark_group("Push once");
        group.plot_config(pc.clone());
        push_once_bench::<_, _, MPQ>(&mut group);
        push_once_bench::<_, _, SPQ>(&mut group);
    }
    {
        let mut group = c.benchmark_group("Pop once");
        group.plot_config(pc.clone());
        pop_once_bench::<_, _, MPQ>(&mut group);
        pop_once_bench::<_, _, SPQ>(&mut group);
    }
    {
        let mut group = c.benchmark_group("Strong pop once");
        group.plot_config(pc.clone());
        spop_once_bench::<_, _, MPQ>(&mut group);
        spop_once_bench::<_, _, SPQ>(&mut group);
    }
}

criterion_group!(benches, compare_once);
criterion_main!(benches);
