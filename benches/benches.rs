use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledStore};
use rand::{distributions::Uniform, thread_rng, Rng};
use std::time::Duration;
use tempfile::TempDir;

fn random_data(count: usize, max_length: usize) -> Vec<String> {
    let mut input = Vec::with_capacity(count);
    for _ in 0..count {
        let len = thread_rng().gen_range(1..=max_length);
        input.push(
            thread_rng()
                .sample_iter(Uniform::new(char::from(32), char::from(126)))
                .take(len)
                .map(char::from)
                .collect(),
        );
    }

    input
}

fn bench_write(c: &mut Criterion) {
    const DATA_COUNT: usize = 100;
    const MAX_LENGTH: usize = 100000;

    let key = random_data(DATA_COUNT, MAX_LENGTH);
    let value = random_data(DATA_COUNT, MAX_LENGTH);

    let mut group = c.benchmark_group("bench_write");
    group.bench_function("kvs_set", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let path: std::path::PathBuf = temp_dir.path().into();
                (temp_dir, KvStore::open(path).unwrap())
            },
            // use _temp_dir to delay its lifetime
            |(_temp_dir, mut store)| {
                for i in 0..DATA_COUNT {
                    store.set(key[i].clone(), value[i].clone()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("sled_set", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let path: std::path::PathBuf = temp_dir.path().into();
                (temp_dir, SledStore::open(path).unwrap())
            },
            |(_temp_dir, mut store)| {
                for i in 0..DATA_COUNT {
                    store.set(key[i].clone(), value[i].clone()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_read(c: &mut Criterion) {
    const DATA_COUNT: usize = 100;
    const READ_COUNT: usize = 1000;
    const MAX_LENGTH: usize = 100000;

    let key = random_data(DATA_COUNT, MAX_LENGTH);
    let value = random_data(DATA_COUNT, MAX_LENGTH);
    let sequence: Vec<usize> = thread_rng()
        .sample_iter(Uniform::new(1, DATA_COUNT))
        .take(READ_COUNT)
        .collect();

    let mut group = c.benchmark_group("bench_write");
    group.bench_function("kvs_get", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let mut store = KvStore::open(temp_dir.path()).unwrap();
                for i in 0..DATA_COUNT {
                    store.set(key[i].clone(), value[i].clone()).unwrap();
                }
                (temp_dir, store)
            },
            |(_temp_dir, mut store)| {
                for seq in &sequence {
                    store.get(key[*seq].clone()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("sled_get", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let mut store = SledStore::open(temp_dir.path()).unwrap();
                for i in 0..DATA_COUNT {
                    store.set(key[i].clone(), value[i].clone()).unwrap();
                }
                (temp_dir, store)
            },
            |(_temp_dir, mut store)| {
                for seq in &sequence {
                    store.get(key[*seq].clone()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(20));
    targets = bench_write, bench_read
);
criterion_main!(benches);
