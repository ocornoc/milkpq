use criterion::{black_box, Criterion, criterion_group, criterion_main};

fn test(c: &mut Criterion) {

}

criterion_group!(benches, test);
criterion_main!(benches);
