//! An [`ObjectStore`] wrapper that injects artificial latency into `get` calls.
//!
//! This simulates the cost of remote storage (e.g. Azure Blob, S3) where each HTTP GET
//! incurs network round-trip time. It makes file-level caching improvements measurable
//! in local benchmarks that would otherwise hit a fast local filesystem.
//!
//! Used by `latency_bench` to demonstrate cache effectiveness. Override the default
//! 10ms latency with the `BENCH_LATENCY_MS` environment variable:
//!
//! ```text
//! cargo bench --bench latency_bench                               # 10ms per request (default)
//! BENCH_LATENCY_MS=50 cargo bench --bench latency_bench           # 50ms per request
//! ```

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use futures::stream::BoxStream;

/// Wraps an inner [`ObjectStore`] and sleeps for a fixed duration before every `get` call.
#[derive(Debug)]
pub struct LatencyStore {
    inner: Arc<dyn ObjectStore>,
    latency: Duration,
    get_count: AtomicU64,
    get_ranges_count: AtomicU64,
}

impl LatencyStore {
    pub fn new(inner: Arc<dyn ObjectStore>, latency: Duration) -> Self {
        Self {
            inner,
            latency,
            get_count: AtomicU64::new(0),
            get_ranges_count: AtomicU64::new(0),
        }
    }

    /// Wraps `store` with latency injection (10ms default, override with `BENCH_LATENCY_MS`).
    pub fn wrap(store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        const DEFAULT_LATENCY_MS: u64 = 10;
        let ms = std::env::var("BENCH_LATENCY_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LATENCY_MS);
        eprintln!("[bench] Injecting {ms}ms latency per ObjectStore get");
        Arc::new(Self::new(store, Duration::from_millis(ms)))
    }
}

impl Drop for LatencyStore {
    fn drop(&mut self) {
        let gets = self.get_count.load(Ordering::Relaxed);
        let ranges = self.get_ranges_count.load(Ordering::Relaxed);
        eprintln!("[latency-store] TOTAL get_opts={gets}, get_ranges={ranges}");
    }
}

impl fmt::Display for LatencyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LatencyStore({}ms, {})",
            self.latency.as_millis(),
            self.inner
        )
    }
}

#[async_trait::async_trait]
impl ObjectStore for LatencyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.get_count.fetch_add(1, Ordering::Relaxed);
        std::thread::sleep(self.latency);
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.get_ranges_count.fetch_add(1, Ordering::Relaxed);
        std::thread::sleep(self.latency);
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: delta_kernel::object_store::CopyOptions,
    ) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
