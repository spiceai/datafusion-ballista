//! Prototype lab for `ballista_core::io_gate` (vcore lending on I/O waits).
//!
//! `SCENARIO=io`: background loops scan a table whose object store answers
//! after `SLOW_MS` (an async sleep standing in for a slow HTTP origin) while a
//! probe query over a local table measures how long other work waits.
//!
//! `SCENARIO=cpu`: concurrent loops run a CPU-heavy aggregate over local
//! Parquet, to measure the gate's overhead and any false vcore releases.
//!
//! `IO_GATE=1` enables the gate for the session and lets the executor run
//! `INFLIGHT_FACTOR` x vcores tasks at once.

use ballista::datafusion::{
    common::Result,
    execution::{SessionStateBuilder, options::ParquetReadOptions},
    prelude::{SessionConfig, SessionContext},
};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_core::config::{
    BALLISTA_IO_GATE_ENABLED, BALLISTA_IO_GATE_MAX_INFLIGHT_TASKS,
    BALLISTA_IO_GATE_RELEASE_THRESHOLD_MS,
};
use ballista_core::io_gate::global_stats;
use ballista_examples::test_util;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

static SLOW_CALLS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct SlowStore {
    inner: LocalFileSystem,
    delay: Duration,
}

impl fmt::Display for SlowStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SlowStore")
    }
}

impl SlowStore {
    /// Only delay reads issued from executor task threads, so planning on the
    /// client/scheduler is not affected.
    async fn maybe_wait(&self) {
        if std::thread::current().name() == Some("task_runner") {
            SLOW_CALLS.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(self.delay).await;
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for SlowStore {
    async fn put_opts(
        &self,
        l: &Path,
        p: PutPayload,
        o: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(
        &self,
        l: &Path,
        o: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
        self.maybe_wait().await;
        self.inner.get_opts(l, o).await
    }
    async fn get_ranges(
        &self,
        l: &Path,
        r: &[Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.maybe_wait().await;
        self.inner.get_ranges(l, r).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        p: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(
        &self,
        p: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(
        &self,
        f: &Path,
        t: &Path,
        o: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(f, t, o).await
    }
}

fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}

fn gate_enabled() -> bool {
    std::env::var("IO_GATE").ok().as_deref() == Some("1")
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).expect("no NaN latencies"));
    let idx = ((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize;
    v[idx]
}

fn summary(name: &str, v: &[f64]) {
    if v.is_empty() {
        println!("{name:<24} n=0");
        return;
    }
    let mut v = v.to_vec();
    println!(
        "{name:<24} n={:<5} p50={:>9.1}ms p99={:>9.1}ms p99.9={:>9.1}ms max={:>9.1}ms",
        v.len(),
        pct(&mut v, 50.0),
        pct(&mut v, 99.0),
        pct(&mut v, 99.9),
        pct(&mut v, 100.0)
    );
}

/// Process CPU seconds (user+sys) from /proc/self/stat.
fn cpu_secs() -> f64 {
    let s = std::fs::read_to_string("/proc/self/stat").expect("procfs");
    let after = &s[s.rfind(')').expect("comm") + 2..];
    let f: Vec<&str> = after.split_whitespace().collect();
    let ticks: f64 =
        f[11].parse::<f64>().expect("utime") + f[12].parse::<f64>().expect("stime");
    ticks / 100.0
}

fn print_gate_stats() {
    let s = global_stats();
    let ms = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed) as f64 / 1e6;
    println!(
        "gate: parks={} yields={} releases={} reacquires={} reacquire_wait={:.1}ms lent={:.1}ms slow_store_calls={}",
        s.parks.load(Ordering::Relaxed),
        s.self_wakes.load(Ordering::Relaxed),
        s.releases.load(Ordering::Relaxed),
        s.reacquire_waits.load(Ordering::Relaxed),
        ms(&s.reacquire_wait_nanos),
        ms(&s.lent_nanos),
        SLOW_CALLS.load(Ordering::Relaxed),
    );
}

async fn sum_id(ctx: &SessionContext, table: &str) -> Result<i64> {
    let batches = ctx
        .sql(&format!("select sum(id) as s from {table} where id >= 0"))
        .await?
        .collect()
        .await?;
    let a = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum(id) is Int64");
    assert!(!a.is_null(0));
    Ok(a.value(0))
}

async fn context(vcores: usize, target_partitions: usize) -> Result<SessionContext> {
    let test_data = test_util::examples_test_data();
    let runtime_env = RuntimeEnvBuilder::new().build()?;
    runtime_env.register_object_store(
        &url::Url::parse("slow://data").expect("url"),
        Arc::new(SlowStore {
            inner: LocalFileSystem::new_with_prefix(&test_data).expect("testdata dir"),
            delay: Duration::from_millis(env_u64("SLOW_MS", 1000)),
        }),
    );
    let mut config = SessionConfig::new_with_ballista()
        .with_target_partitions(target_partitions)
        .with_ballista_standalone_parallelism(vcores);
    if gate_enabled() {
        let factor = env_u64("INFLIGHT_FACTOR", 4) as usize;
        config = config
            .set_bool(BALLISTA_IO_GATE_ENABLED, true)
            .set_usize(BALLISTA_IO_GATE_MAX_INFLIGHT_TASKS, vcores * factor)
            .set_u64(
                BALLISTA_IO_GATE_RELEASE_THRESHOLD_MS,
                env_u64("RELEASE_MS", 50),
            );
    }
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime_env.into())
        .with_default_features()
        .build();
    SessionContext::standalone_with_state(state).await
}

async fn io_scenario(vcores: usize) -> Result<()> {
    let probes = env_u64("PROBES", 60) as usize;
    let bg_loops = env_u64("BG_LOOPS", 2) as usize;
    let test_data = test_util::examples_test_data();
    let ctx = context(vcores, 1).await?;
    ctx.register_parquet(
        "fast",
        &format!("{test_data}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "slow",
        "slow://data/alltypes_plain.parquet",
        ParquetReadOptions::default(),
    )
    .await?;
    assert_eq!(sum_id(&ctx, "fast").await?, 28);
    assert_eq!(sum_id(&ctx, "slow").await?, 28);

    let mut idle = Vec::new();
    for _ in 0..probes {
        let t = Instant::now();
        assert_eq!(sum_id(&ctx, "fast").await?, 28);
        idle.push(t.elapsed().as_secs_f64() * 1e3);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let bg: Vec<_> = (0..bg_loops)
        .map(|_| {
            let ctx = ctx.clone();
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let mut lat = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let t = Instant::now();
                    assert_eq!(sum_id(&ctx, "slow").await.expect("slow query"), 28);
                    lat.push(t.elapsed().as_secs_f64() * 1e3);
                }
                lat
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let cpu0 = cpu_secs();
    let wall0 = Instant::now();
    let mut loaded = Vec::new();
    for _ in 0..probes {
        let t = Instant::now();
        assert_eq!(sum_id(&ctx, "fast").await?, 28);
        loaded.push(t.elapsed().as_secs_f64() * 1e3);
    }
    // Keep the slow loops running long enough for a usable sample.
    let min_slow = env_u64("MIN_SLOW_SECS", 30);
    let remaining = Duration::from_secs(min_slow).saturating_sub(wall0.elapsed());
    tokio::time::sleep(remaining).await;
    let wall = wall0.elapsed().as_secs_f64();
    let cpu = cpu_secs() - cpu0;
    stop.store(true, Ordering::Relaxed);
    let mut slow_lat = Vec::new();
    for h in bg {
        slow_lat.extend(h.await.expect("bg loop"));
    }
    summary("probe idle", &idle);
    summary("probe under slow load", &loaded);
    summary("slow query", &slow_lat);
    println!(
        "slow query throughput: {:.2}/s; process CPU {:.2} cores over {wall:.1}s",
        slow_lat.len() as f64 / wall,
        cpu / wall
    );
    Ok(())
}

async fn write_cpu_data(dir: &str) -> Result<()> {
    if std::path::Path::new(dir).join("_done").exists() {
        return Ok(());
    }
    let rows = env_u64("ROWS", 20_000_000);
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
    ctx.sql(&format!(
        "select value as k, (value * 7919) % 1000003 as v from generate_series(1, {rows})"
    ))
    .await?
    .repartition(datafusion::logical_expr::Partitioning::RoundRobinBatch(8))?
    .write_parquet(dir, Default::default(), None)
    .await?;
    std::fs::write(std::path::Path::new(dir).join("_done"), b"")?;
    Ok(())
}

async fn cpu_scenario(vcores: usize) -> Result<()> {
    let dir = std::env::var("DATA_DIR").expect("DATA_DIR for SCENARIO=cpu");
    write_cpu_data(&dir).await?;
    let ctx = context(vcores, vcores).await?;
    ctx.register_parquet("t", &dir, ParquetReadOptions::default())
        .await?;
    let sql = "select k % 1000 as g, count(*) as c, sum(v) as s, avg(v * v) as a \
               from t group by g order by g";
    // Warm-up.
    ctx.sql(sql).await?.collect().await?;

    let loops = env_u64("LOOPS", vcores as u64) as usize;
    let secs = env_u64("SECS", 60);
    let deadline = Instant::now() + Duration::from_secs(secs);
    let cpu0 = cpu_secs();
    let wall0 = Instant::now();
    let tasks: Vec<_> = (0..loops)
        .map(|_| {
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let mut lat = Vec::new();
                while Instant::now() < deadline {
                    let t = Instant::now();
                    let rows: usize = ctx
                        .sql(sql)
                        .await
                        .expect("plan")
                        .collect()
                        .await
                        .expect("query")
                        .iter()
                        .map(|b| b.num_rows())
                        .sum();
                    assert_eq!(rows, 1000);
                    lat.push(t.elapsed().as_secs_f64() * 1e3);
                }
                lat
            })
        })
        .collect();
    let mut lat = Vec::new();
    for t in tasks {
        lat.extend(t.await.expect("loop"));
    }
    let wall = wall0.elapsed().as_secs_f64();
    summary("cpu query", &lat);
    println!(
        "throughput: {:.2} queries/s; process CPU {:.2} cores over {wall:.1}s",
        lat.len() as f64 / wall,
        (cpu_secs() - cpu0) / wall
    );
    Ok(())
}

/// CPU-heavy loops and slow-I/O loops at the same time: what lending costs
/// the I/O-bound queries when they must win a vcore back from CPU work.
async fn mixed_scenario(vcores: usize) -> Result<()> {
    let dir = std::env::var("DATA_DIR").expect("DATA_DIR for SCENARIO=mixed");
    write_cpu_data(&dir).await?;
    let ctx = context(vcores, vcores).await?;
    ctx.register_parquet("t", &dir, ParquetReadOptions::default())
        .await?;
    ctx.register_parquet(
        "slow",
        "slow://data/alltypes_plain.parquet",
        ParquetReadOptions::default(),
    )
    .await?;
    let cpu_sql = "select k % 1000 as g, count(*) as c, sum(v) as s, avg(v * v) as a \
                   from t group by g order by g";
    ctx.sql(cpu_sql).await?.collect().await?;
    assert_eq!(sum_id(&ctx, "slow").await?, 28);

    let deadline = Instant::now() + Duration::from_secs(env_u64("SECS", 60));
    let spawn_loops = |n: u64, slow: bool| -> Vec<_> {
        (0..n)
            .map(|_| {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    let mut lat = Vec::new();
                    while Instant::now() < deadline {
                        let t = Instant::now();
                        if slow {
                            assert_eq!(sum_id(&ctx, "slow").await.expect("slow"), 28);
                        } else {
                            ctx.sql(cpu_sql)
                                .await
                                .expect("plan")
                                .collect()
                                .await
                                .expect("cpu query");
                        }
                        lat.push(t.elapsed().as_secs_f64() * 1e3);
                    }
                    lat
                })
            })
            .collect()
    };
    let cpu0 = cpu_secs();
    let wall0 = Instant::now();
    let cpu_loops = spawn_loops(env_u64("LOOPS", vcores as u64), false);
    let slow_loops = spawn_loops(env_u64("BG_LOOPS", 2), true);
    let (mut cpu_lat, mut slow_lat) = (Vec::new(), Vec::new());
    for h in cpu_loops {
        cpu_lat.extend(h.await.expect("cpu loop"));
    }
    for h in slow_loops {
        slow_lat.extend(h.await.expect("slow loop"));
    }
    let wall = wall0.elapsed().as_secs_f64();
    summary("cpu query", &cpu_lat);
    summary("slow query", &slow_lat);
    println!(
        "throughput: cpu {:.2}/s slow {:.2}/s; process CPU {:.2} cores over {wall:.1}s",
        cpu_lat.len() as f64 / wall,
        slow_lat.len() as f64 / wall,
        (cpu_secs() - cpu0) / wall
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let vcores = env_u64("VCORES", 2) as usize;
    let scenario = std::env::var("SCENARIO").unwrap_or_else(|_| "io".to_string());
    println!(
        "scenario={scenario} gate={} vcores={vcores} inflight_factor={} release_ms={} slow_ms={} bg_loops={}",
        gate_enabled(),
        env_u64("INFLIGHT_FACTOR", 4),
        env_u64("RELEASE_MS", 50),
        env_u64("SLOW_MS", 1000),
        env_u64("BG_LOOPS", 2),
    );
    match scenario.as_str() {
        "io" => io_scenario(vcores).await?,
        "cpu" => cpu_scenario(vcores).await?,
        "mixed" => mixed_scenario(vcores).await?,
        other => panic!("unknown SCENARIO {other}"),
    }
    print_gate_stats();
    Ok(())
}
