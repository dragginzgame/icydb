extern crate bencher;

mod blob;

use bencher::benchmark_main;

benchmark_main!(blob::benchmarks);
