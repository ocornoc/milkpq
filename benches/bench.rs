use rayon::prelude::*;
use milkpq::MilkPQ;
use rand::prelude::*;
use criterion::{Criterion, criterion_group, criterion_main, BenchmarkId};

type MPQ = MilkPQ<i32>;

fn once_pop(mpq: &MPQ) {
    mpq.pop();
}

fn seq_pop(mpq: &MPQ, n: usize) {
    for _ in 0..n {
        mpq.pop();
    }
}

fn par_pop(mpq: &MPQ, n: usize) {
    (0..n).into_par_iter()
        .for_each(|_| { mpq.pop(); });
}

fn once_strong_pop(mpq: &MPQ) {
    mpq.strong_pop();
}

fn seq_strong_pop(mpq: &MPQ, n: usize) {
    for _ in 0..n {
        mpq.strong_pop();
    }
}

fn par_strong_pop(mpq: &MPQ, n: usize) {
    (0..n).into_par_iter()
        .for_each(|_| { mpq.strong_pop(); });
}

fn once_push(mpq: &MPQ, t: i32) {
    mpq.push(t);
}

fn seq_push<I: Iterator<Item = i32>>(mpq: &MPQ, iter: I) {
    for t in iter {
        mpq.push(t);
    }
}

fn par_push<I: ParallelIterator<Item = i32>>(mpq: &MPQ, iter: I) {
    iter.for_each(|t| { mpq.push(t); });
}

fn seq_mix<I: Iterator<Item = i32>>(mpq: &MPQ, iter: I, mut pops: usize) {
    for t in iter {
        mpq.push(t);
        
        if pops > 0 {
            pops -= 1;
            mpq.pop();
        }
    }
}

fn seq_seq_mix<I: Iterator<Item = i32> + Send>(mpq: &MPQ, iter: I, pops: usize) {
    rayon::join(
        || for t in iter {
            mpq.push(t);
        },
        || for _ in 0..pops {
            mpq.pop();
        },
    );
}

fn seq_par_mix<I: Iterator<Item = i32> + Send>(mpq: &MPQ, iter: I, pops: usize) {
    rayon::join(
        || for t in iter {
            mpq.push(t);
        },
        || (0..pops).into_par_iter().for_each(|_| { mpq.pop(); }),
    );
}

fn par_seq_mix<I: ParallelIterator<Item = i32>>(mpq: &MPQ, iter: I, pops: usize) {
    rayon::join(
        || iter.for_each(|t| mpq.push(t)),
        || for _ in 0..pops {
            mpq.pop();
        },
    );
}

fn par_par_mix<I: ParallelIterator<Item = i32>>(mpq: &MPQ, iter: I, pops: usize) {
    rayon::join(
        || iter.for_each(|t| mpq.push(t)),
        || (0..pops).into_par_iter().for_each(|_| { mpq.pop(); }),
    );
}

fn once_bench(c: &mut Criterion, mpq: &MPQ, name: &'static str) {
    c.bench_with_input(BenchmarkId::new("pop once", "Empty MilkPQ"), &MPQ::new(), |b, mpq| {
        b.iter(|| once_pop(&mpq));
    });
    c.bench_with_input(BenchmarkId::new("pop once", name), &mpq.clone(), |b, mpq| {
        b.iter(|| once_pop(&mpq))
    });

    c.bench_with_input(BenchmarkId::new("strong pop once", "Empty MilkPQ"), &MPQ::new(), |b, mpq| {
        b.iter(|| once_strong_pop(&mpq));
    });
    c.bench_with_input(BenchmarkId::new("strong pop once", name), &mpq.clone(), |b, mpq| {
        b.iter(|| once_strong_pop(&mpq))
    });

    c.bench_with_input(BenchmarkId::new("push once", "Empty MilkPQ"), &MPQ::new(), |b, mpq| {
        b.iter(|| once_push(&mpq, 5000));
    });
    c.bench_with_input(BenchmarkId::new("push once", name), &mpq.clone(), |b, mpq| {
        b.iter(|| once_push(&mpq, 5000))
    });
}

fn pop_bench(c: &mut Criterion, mpq: &MPQ, name: &'static str) {
    let pops = 5000;
    let mut group = c.benchmark_group("Pop only");

    group.bench_with_input(
        BenchmarkId::new("Sequential", "Empty MilkPQ"),
        &MPQ::new(),
        |b, mpq| b.iter(|| seq_pop(mpq, pops))
    );
    group.bench_with_input(
        BenchmarkId::new("Sequential", name),
        &mpq.clone(),
        |b, mpq| b.iter(|| seq_pop(mpq, pops))
    );

    group.bench_with_input(
        BenchmarkId::new("Parallel", "Empty MilkPQ"),
        &MPQ::new(),
        |b, mpq| b.iter(|| par_pop(mpq, pops))
    );
    group.bench_with_input(
        BenchmarkId::new("Parallel", name),
        &mpq.clone(),
        |b, mpq| b.iter(|| par_pop(mpq, pops))
    );
}

fn strong_pop_bench(c: &mut Criterion, mpq: &MPQ, name: &'static str) {
    let pops = 5000;
    let mut group = c.benchmark_group("Strong pop only");

    group.bench_with_input(
        BenchmarkId::new("Sequential", "Empty MilkPQ"),
        &MPQ::new(),
        |b, mpq| b.iter(|| seq_strong_pop(mpq, pops))
    );
    group.bench_with_input(
        BenchmarkId::new("Sequential", name),
        &mpq.clone(),
        |b, mpq| b.iter(|| seq_strong_pop(mpq, pops))
    );

    group.bench_with_input(
        BenchmarkId::new("Parallel", "Empty MilkPQ"),
        &MPQ::new(),
        |b, mpq| b.iter(|| par_strong_pop(mpq, pops))
    );
    group.bench_with_input(
        BenchmarkId::new("Parallel", name),
        &mpq.clone(),
        |b, mpq| b.iter(|| par_strong_pop(mpq, pops))
    );
}

fn push_bench(c: &mut Criterion, mpq: &MPQ, insert: &Vec<i32>, name: &'static str) {
    let mut group = c.benchmark_group("Push only");

    group.bench_with_input(
        BenchmarkId::new("Sequential", "Empty MilkPQ"),
        &(MPQ::new(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| seq_push(mpq, insert.iter().cloned()))
    );
    group.bench_with_input(
        BenchmarkId::new("Sequential", name),
        &(mpq.clone(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| seq_push(mpq, insert.iter().cloned()))
    );

    group.bench_with_input(
        BenchmarkId::new("Parallel", "Empty MilkPQ"),
        &(MPQ::new(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| par_push(mpq, insert.par_iter().cloned()))
    );
    group.bench_with_input(
        BenchmarkId::new("Parallel", name),
        &(mpq.clone(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| par_push(mpq, insert.par_iter().cloned()))
    );
}

fn mix_bench(c: &mut Criterion, mpq: &MPQ, insert: &Vec<i32>, name: &'static str) {
    let pops = insert.len();
    let mut group = c.benchmark_group("Mixed Pop/Push");

    group.bench_with_input(
        BenchmarkId::new("SPSC single thead", "Empty MilkPQ"),
        &(MPQ::new(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| seq_mix(mpq, insert.iter().copied(), pops))
    );
    group.bench_with_input(
        BenchmarkId::new("SPSC single thead", name),
        &(mpq.clone(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| seq_mix(mpq, insert.iter().copied(), pops))
    );

    group.bench_with_input(
        BenchmarkId::new("SPSC split thread", "Empty MilkPQ"),
        &(MPQ::new(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| seq_seq_mix(mpq, insert.iter().copied(), pops))
    );
    group.bench_with_input(
        BenchmarkId::new("SPSC split thread", name),
        &(mpq.clone(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| seq_seq_mix(mpq, insert.iter().copied(), pops))
    );

    group.bench_with_input(
        BenchmarkId::new("MPSC", "Empty MilkPQ"),
        &(MPQ::new(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| par_seq_mix(mpq, insert.par_iter().copied(), pops))
    );
    group.bench_with_input(
        BenchmarkId::new("MPSC", name),
        &(mpq.clone(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| par_seq_mix(mpq, insert.par_iter().copied(), pops))
    );

    group.bench_with_input(
        BenchmarkId::new("SPMC", "Empty MilkPQ"),
        &(MPQ::new(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| seq_par_mix(mpq, insert.iter().copied(), pops))
    );
    group.bench_with_input(
        BenchmarkId::new("SPMC", name),
        &(mpq.clone(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| seq_par_mix(mpq, insert.iter().copied(), pops))
    );

    group.bench_with_input(
        BenchmarkId::new("MPMC", "Empty MilkPQ"),
        &(MPQ::new(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| par_par_mix(mpq, insert.par_iter().copied(), pops))
    );
    group.bench_with_input(
        BenchmarkId::new("MPMC", name),
        &(mpq.clone(), insert.clone()),
        |b, (mpq, insert)| b.iter(|| par_par_mix(mpq, insert.par_iter().copied(), pops))
    );
}

fn test(c: &mut Criterion) {
    let mut vs = (0..10000).collect::<Vec<_>>();
    vs.shuffle(&mut thread_rng());
    let mpq = vs.clone().into_iter().collect::<MPQ>();
    vs.shuffle(&mut thread_rng());
    let name = "10K MilkPQ";

    once_bench(c, &mpq, name);
    pop_bench(c, &mpq, name);
    strong_pop_bench(c, &mpq, name);
    mix_bench(c, &mpq, &vs, name);
}

criterion_group!(benches, test);
criterion_main!(benches);
