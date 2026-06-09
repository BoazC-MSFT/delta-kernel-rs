//! Benchmark that injects simulated storage latency to demonstrate the impact of
//! file-level caching. Run separately from the main benchmarks to avoid masking
//! regressions in non-cache code paths:
//!
//! ```text
//! cargo bench --bench latency_bench            # 10ms latency (default)
//! BENCH_LATENCY_MS=50 cargo bench --bench latency_bench  # 50ms latency
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use delta_kernel_benchmarks::latency_store::LatencyStore;
use delta_kernel_benchmarks::models::{default_read_configs, ReadOperation, Spec};
use delta_kernel_benchmarks::runners::{create_read_runner, WorkloadRunner};
use delta_kernel_benchmarks::utils::load_all_workloads;
use test_utils::CountingReporter;

fn latency_benchmarks(c: &mut Criterion) {
    let workloads = match load_all_workloads() {
        Ok(workloads) if !workloads.is_empty() => workloads,
        Ok(_) => panic!("No workloads found"),
        Err(e) => panic!("Failed to load workloads: {e}"),
    };

    let reporter = Arc::new(CountingReporter::new());
    let runtime = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create tokio runtime"));

    for workload in &workloads {
        if let Spec::Read(read_spec) = &workload.spec {
            for config in default_read_configs() {
                let runner = create_read_runner(
                    &workload.table_info,
                    &workload.case_name,
                    read_spec,
                    ReadOperation::ReadMetadata,
                    config,
                    runtime.clone(),
                    Some(LatencyStore::wrap),
                )
                .expect("Failed to create read runner");
                run_benchmark(c, runner.as_ref(), &reporter);
            }
        }
    }
}

fn run_benchmark(c: &mut Criterion, runner: &dyn WorkloadRunner, reporter: &CountingReporter) {
    let bench_ran = AtomicBool::new(false);
    c.bench_function(&format!("latency/{}", runner.name()), |b| {
        bench_ran.store(true, Ordering::Relaxed);
        b.iter(|| runner.execute().expect("Benchmark execution failed"))
    });
    if bench_ran.load(Ordering::Relaxed) {
        reporter.reset();
        runner.execute().expect("IO profiling iteration failed");
        reporter.print_summary(runner.name());
    }
}

criterion_group!(benches, latency_benchmarks);
criterion_main!(benches);
