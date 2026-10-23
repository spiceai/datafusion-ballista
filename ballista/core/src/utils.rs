// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::config::{BallistaConfig, ShuffleFormat};
use crate::error::{BallistaError, Result};
use crate::extension::SessionConfigExt;
use crate::serde::scheduler::PartitionStats;

use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionState};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, metrics};
use futures::future::Either;
use futures::{Stream, StreamExt};
use log::error;
use std::io::BufWriter;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs::File, pin::Pin};
use tonic::codegen::StdError;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Endpoint, Error, Server};

/// Configuration for gRPC client connections.
///
/// This struct holds timeout and keep-alive settings that are applied
/// when establishing gRPC connections from executors to schedulers or
/// between distributed components.
///
/// # Examples
///
/// ```
/// use ballista_core::config::BallistaConfig;
/// use ballista_core::utils::GrpcClientConfig;
///
/// let ballista_config = BallistaConfig::default();
/// let grpc_config = GrpcClientConfig::from(&ballista_config);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GrpcClientConfig {
    /// Connection timeout in seconds
    pub connect_timeout_seconds: u64,
    /// Request timeout in seconds
    pub timeout_seconds: u64,
    /// TCP keep-alive interval in seconds
    pub tcp_keepalive_seconds: u64,
    /// HTTP/2 keep-alive ping interval in seconds
    pub http2_keepalive_interval_seconds: u64,
    /// Should client use tls
    pub use_tls: bool,
    /// Returns the maximum message size for gRPC clients in bytes.
    pub max_message_size: usize,
    /// Number of retries for IO operations.
    pub io_retries_times: u8,
    /// Wait time in milliseconds between IO retries.
    pub io_retry_wait_time_ms: u64,
    /// HTTP/2 initial connection-level flow-control window in bytes. 0 = tonic default.
    pub initial_connection_window_size: u32,
    /// HTTP/2 initial stream-level flow-control window in bytes. 0 = tonic default.
    pub initial_stream_window_size: u32,
}

impl From<&BallistaConfig> for GrpcClientConfig {
    fn from(config: &BallistaConfig) -> Self {
        Self {
            connect_timeout_seconds: config.grpc_client_connect_timeout_seconds() as u64,
            timeout_seconds: config.grpc_client_timeout_seconds() as u64,
            tcp_keepalive_seconds: config.grpc_client_tcp_keepalive_seconds() as u64,
            http2_keepalive_interval_seconds: config
                .grpc_client_http2_keepalive_interval_seconds()
                as u64,
            use_tls: config.use_tls(),
            max_message_size: config.grpc_client_max_message_size(),
            io_retries_times: config.io_retries_times() as u8,
            io_retry_wait_time_ms: config.io_retry_wait_time_ms() as u64,
            initial_connection_window_size: config
                .grpc_client_initial_connection_window_size(),
            initial_stream_window_size: config.grpc_client_initial_stream_window_size(),
        }
    }
}

impl Default for GrpcClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout_seconds: 20,
            timeout_seconds: 20,
            tcp_keepalive_seconds: 3600,
            http2_keepalive_interval_seconds: 300,
            use_tls: false,
            max_message_size: 16 * 1024 * 1024,
            io_retries_times: 3,
            io_retry_wait_time_ms: 3000,
            initial_connection_window_size: 67108864,
            initial_stream_window_size: 16777216,
        }
    }
}

/// Configuration for gRPC server.
///
/// This struct holds timeout and keep-alive settings that are applied
/// when creating gRPC servers in executors and schedulers.
///
/// # Examples
///
/// ```
/// use ballista_core::utils::GrpcServerConfig;
///
/// let server_config = GrpcServerConfig::default();
/// let server = ballista_core::utils::create_grpc_server(&server_config);
/// ```
#[derive(Debug, Clone)]
pub struct GrpcServerConfig {
    /// Request timeout in seconds
    pub timeout_seconds: u64,
    /// TCP keep-alive interval in seconds
    pub tcp_keepalive_seconds: u64,
    /// HTTP/2 keep-alive ping interval in seconds
    pub http2_keepalive_interval_seconds: u64,
    /// HTTP/2 keep-alive ping timeout in seconds
    pub http2_keepalive_timeout_seconds: u64,
}

impl Default for GrpcServerConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 20,
            tcp_keepalive_seconds: 3600,
            http2_keepalive_interval_seconds: 300,
            http2_keepalive_timeout_seconds: 20,
        }
    }
}

/// Default session builder using the provided configuration
pub fn default_session_builder(
    config: SessionConfig,
) -> datafusion::common::Result<SessionState> {
    use crate::extension::{
        ballista_aggregate_functions, ballista_scalar_functions,
        ballista_window_functions,
    };

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build()?))
        .with_optimizer_rules(crate::optimizer::ballista_default_optimizer_rules())
        .with_scalar_functions(ballista_scalar_functions())
        .with_aggregate_functions(ballista_aggregate_functions())
        .with_window_functions(ballista_window_functions())
        .build();

    Ok(state)
}

/// Creates a default session configuration with Ballista extensions.
pub fn default_config_producer() -> SessionConfig {
    SessionConfig::new_with_ballista()
}

/// Creates [IpcWriteOptions] using the compression codec configured in the BallistaConfig
pub fn create_write_options(
    compression_type: Option<CompressionType>,
) -> std::result::Result<IpcWriteOptions, DataFusionError> {
    IpcWriteOptions::default()
        .try_with_compression(compression_type)
        .map_err(|err| {
            DataFusionError::Internal(format!("Failed to set compression codec: {err}"))
        })
}

/// Batches a stream fed to a blocking writer.
pub(crate) struct Written<W> {
    pub(crate) writer: W,
    pub(crate) num_rows: usize,
    pub(crate) num_batches: usize,
}

/// Feeds `stream`'s batches to `writer` with `write`, on the blocking pool,
/// keeping up to `capacity` batches buffered while a write is in flight.
///
/// A blocking job only writes batches already received; it never waits for
/// input. A task whose input waits on remote I/O lends its vcore and needs one
/// back before it can feed its writer, so a writer waiting for input on a
/// blocking thread could hold that thread from the task holding the vcore.
///
/// A stream error is returned once the write in flight has ended, which is
/// logged if it failed too.
pub(crate) async fn write_batches_blocking<S, W, F>(
    stream: &mut S,
    writer: W,
    capacity: usize,
    write: F,
) -> Result<Written<W>>
where
    S: Stream<Item = datafusion::error::Result<RecordBatch>> + Unpin + ?Sized,
    W: Send + 'static,
    F: Fn(&mut W, &RecordBatch) -> Result<()> + Clone + Send + 'static,
{
    async fn join<W>(job: tokio::task::JoinHandle<Result<W>>) -> Result<W> {
        job.await.map_err(|e| {
            BallistaError::General(format!("Disk writer task failed: {e}"))
        })?
    }

    let capacity = capacity.max(1);
    let mut writer = Some(writer);
    let mut job: Option<tokio::task::JoinHandle<Result<W>>> = None;
    let mut buffered = Vec::new();
    let mut ended = false;
    let mut num_rows = 0;
    let mut num_batches = 0;
    loop {
        // Hand whatever is buffered to the writer as soon as it is free.
        if !buffered.is_empty()
            && let Some(mut w) = writer.take()
        {
            let batches = std::mem::take(&mut buffered);
            let write = write.clone();
            job = Some(tokio::task::spawn_blocking(
                crate::vcore_permit::carry_blocking(move || {
                    for batch in &batches {
                        write(&mut w, batch)?;
                    }
                    Ok(w)
                }),
            ));
        }
        let read = !ended && buffered.len() < capacity;
        let input = match (read, job.as_mut()) {
            // Wait for whichever comes first: the next batch, or the write in
            // flight, so batches buffered behind it go out without waiting for
            // more input.
            (true, Some(running)) => {
                match futures::future::select(running, stream.next()).await {
                    Either::Left((joined, _)) => {
                        job = None;
                        writer = Some(joined.map_err(|e| {
                            BallistaError::General(format!(
                                "Disk writer task failed: {e}"
                            ))
                        })??);
                        continue;
                    }
                    Either::Right((input, _)) => input,
                }
            }
            (true, None) => stream.next().await,
            // The buffer is full or the input has ended: wait for the writer.
            (false, Some(_)) => {
                if let Some(running) = job.take() {
                    writer = Some(join(running).await?);
                }
                continue;
            }
            (false, None) => {
                // Nothing in flight and the input has ended, so everything
                // buffered has been written.
                let (true, true, Some(writer)) = (ended, buffered.is_empty(), writer)
                else {
                    return Err(BallistaError::Internal(
                        "disk writer stopped with batches unwritten".to_string(),
                    ));
                };
                return Ok(Written {
                    writer,
                    num_rows,
                    num_batches,
                });
            }
        };
        match input {
            Some(Ok(batch)) => {
                num_rows += batch.num_rows();
                num_batches += 1;
                buffered.push(batch);
            }
            Some(Err(e)) => {
                if let Some(running) = job.take()
                    && let Err(write_err) = join(running).await
                {
                    error!("Disk writer also failed: {write_err}");
                }
                return Err(e.into());
            }
            None => ended = true,
        }
    }
}

/// Runs `f`, a synchronous file operation, on the blocking pool as part of
/// the calling task.
pub(crate) async fn run_blocking_io<R: Send + 'static>(
    f: impl FnOnce() -> Result<R> + Send + 'static,
) -> Result<R> {
    tokio::task::spawn_blocking(crate::vcore_permit::carry_blocking(f))
        .await
        .map_err(|e| BallistaError::General(format!("Disk writer task failed: {e}")))?
}

/// Stream data to disk in Arrow IPC format.
///
/// Every synchronous file operation runs on the blocking pool, keeping the
/// tokio worker thread unblocked, and is carried into the calling task's
/// vcore scope, so the writes count as the task running. A blocking job
/// only writes batches already received and never waits for input, so a lent
/// task's writer holds no blocking thread while its input waits.
pub async fn write_stream_to_disk(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
    path: &Path,
    disk_write_metric: &metrics::Time,
    channel_capacity: usize,
    compression_type: Option<CompressionType>,
) -> Result<PartitionStats> {
    let schema = stream.schema();
    let path_owned = path.to_owned();
    let writer = run_blocking_io(move || {
        let file = BufWriter::new(File::create(&path_owned).map_err(|e| {
            error!("Failed to create partition file at {:?}: {e:?}", path_owned);
            BallistaError::IoError(e)
        })?);
        let options = create_write_options(compression_type)?;
        Ok(StreamWriter::try_new_with_options(
            file,
            schema.as_ref(),
            options,
        )?)
    })
    .await?;

    let write_metric = disk_write_metric.clone();
    let written = write_batches_blocking(
        stream,
        writer,
        channel_capacity,
        move |writer: &mut StreamWriter<BufWriter<File>>, batch: &RecordBatch| {
            let timer = write_metric.timer();
            writer.write(batch)?;
            timer.done();
            Ok(())
        },
    )
    .await?;

    let mut writer = written.writer;
    let write_metric = disk_write_metric.clone();
    let path_owned = path.to_owned();
    let num_bytes = run_blocking_io(move || {
        let timer = write_metric.timer();
        writer.finish()?;
        timer.done();
        Ok(std::fs::metadata(&path_owned).map(|m| m.len()).unwrap_or(0))
    })
    .await?;

    Ok(PartitionStats::new(
        Some(written.num_rows as u64),
        Some(written.num_batches as u64),
        Some(num_bytes),
    ))
}

/// Write stream to disk using the specified shuffle format (Spice fork).
///
/// This function dispatches to the appropriate writer based on the format:
/// - ArrowIpc: Uses Arrow IPC streaming format via [`write_stream_to_disk`]
/// - Vortex: Uses Vortex columnar format (requires 'vortex' feature)
pub async fn write_stream_to_disk_with_format(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
    path: &Path,
    disk_write_metric: &metrics::Time,
    channel_capacity: usize,
    compression_type: Option<CompressionType>,
    format: ShuffleFormat,
) -> Result<PartitionStats> {
    match format {
        ShuffleFormat::ArrowIpc => {
            write_stream_to_disk(
                stream,
                path,
                disk_write_metric,
                channel_capacity,
                compression_type,
            )
            .await
        }
        #[cfg(feature = "vortex")]
        ShuffleFormat::Vortex => {
            let path = path.to_str().ok_or_else(|| {
                BallistaError::General(format!(
                    "Shuffle file path {path:?} is not valid UTF-8"
                ))
            })?;
            crate::execution_plans::write_stream_to_disk_vortex(
                stream,
                path,
                disk_write_metric,
            )
            .await
        }
        #[cfg(not(feature = "vortex"))]
        ShuffleFormat::Vortex => Err(BallistaError::General(
            "Vortex format is not available. Enable the 'vortex' feature to use Vortex shuffle format.".to_string(),
        )),
    }
}

/// Get the file extension for the given shuffle format
pub fn shuffle_file_extension(format: ShuffleFormat) -> &'static str {
    match format {
        ShuffleFormat::ArrowIpc => "arrow",
        ShuffleFormat::Vortex => "vortex",
    }
}

/// Collects all record batches from a stream into a vector.
pub async fn collect_stream(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
) -> Result<Vec<RecordBatch>> {
    let mut batches = vec![];
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Creates a gRPC client connection with the specified configuration.
pub async fn create_grpc_client_connection<D>(
    dst: D,
    config: &GrpcClientConfig,
) -> std::result::Result<Channel, Error>
where
    D: std::convert::TryInto<tonic::transport::Endpoint>,
    D::Error: Into<StdError>,
{
    let endpoint = tonic::transport::Endpoint::new(dst)?
        .connect_timeout(Duration::from_secs(config.connect_timeout_seconds))
        .timeout(Duration::from_secs(config.timeout_seconds))
        // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::from_secs(config.tcp_keepalive_seconds)))
        .http2_keep_alive_interval(Duration::from_secs(
            config.http2_keepalive_interval_seconds,
        ))
        // Use a fixed timeout for keep-alive pings to keep configuration simple
        // since this is a standalone configuration
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);
    endpoint.connect().await
}

/// Creates a gRPC client endpoint (without connecting) for customization.
/// This is typically used when TLS or other custom configuration is needed.
/// If `config` is provided, standard timeout and keepalive settings are applied.
pub fn create_grpc_client_endpoint<D>(
    dst: D,
    config: Option<&GrpcClientConfig>,
) -> std::result::Result<Endpoint, Error>
where
    D: std::convert::TryInto<tonic::transport::Endpoint>,
    D::Error: Into<StdError>,
{
    let endpoint = tonic::transport::Endpoint::new(dst)?;
    if let Some(config) = config {
        let mut endpoint = endpoint
            .connect_timeout(Duration::from_secs(config.connect_timeout_seconds))
            .timeout(Duration::from_secs(config.timeout_seconds))
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(config.tcp_keepalive_seconds)))
            .http2_keep_alive_interval(Duration::from_secs(
                config.http2_keepalive_interval_seconds,
            ))
            .keep_alive_timeout(Duration::from_secs(20))
            .keep_alive_while_idle(true);
        if config.initial_connection_window_size > 0 {
            endpoint = endpoint.initial_connection_window_size(Some(
                config.initial_connection_window_size,
            ));
        }
        if config.initial_stream_window_size > 0 {
            endpoint = endpoint
                .initial_stream_window_size(Some(config.initial_stream_window_size));
        }
        Ok(endpoint)
    } else {
        Ok(endpoint)
    }
}

/// Creates a gRPC server builder with the specified configuration.
pub fn create_grpc_server(config: &GrpcServerConfig) -> Server {
    Server::builder()
        .timeout(Duration::from_secs(config.timeout_seconds))
        // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::from_secs(config.tcp_keepalive_seconds)))
        .http2_keepalive_interval(Some(Duration::from_secs(
            config.http2_keepalive_interval_seconds,
        )))
        .http2_keepalive_timeout(Some(Duration::from_secs(
            config.http2_keepalive_timeout_seconds,
        )))
}

/// Binds a gRPC server's listening socket, for use with tonic's
/// `serve_with_incoming` / `serve_with_incoming_shutdown`. Unlike tonic's
/// `serve`, which binds lazily inside the future it returns, the socket is
/// listening by the time this returns — so use this whenever a peer may be
/// told to connect as soon as the server is started.
///
/// tonic ignores the builder's `tcp_nodelay` and `tcp_keepalive` when serving
/// from a pre-bound listener, so this applies the same values that
/// [`create_grpc_server`] sets, for the same reasons. The remaining settings
/// still come from the builder.
///
/// # Panics
///
/// The listener is registered with the Tokio reactor, so this must be called
/// from within a Tokio runtime.
pub fn create_grpc_server_incoming(
    addr: SocketAddr,
    config: &GrpcServerConfig,
) -> Result<TcpIncoming> {
    Ok(TcpIncoming::bind(addr)?
        .with_nodelay(Some(true))
        .with_keepalive(Some(Duration::from_secs(config.tcp_keepalive_seconds))))
}

/// Recursively collects metrics from an execution plan and all its children.
pub fn collect_plan_metrics(plan: &dyn ExecutionPlan) -> Vec<MetricsSet> {
    let mut metrics_array = Vec::<MetricsSet>::new();
    if let Some(metrics) = plan.metrics() {
        metrics_array.push(metrics);
    }
    plan.children().iter().for_each(|c| {
        collect_plan_metrics(c.as_ref())
            .into_iter()
            .for_each(|e| metrics_array.push(e))
    });
    metrics_array
}

/// Given an interval in seconds, get the time in seconds before now
pub fn get_time_before(interval_seconds: u64) -> u64 {
    let now_epoch_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    now_epoch_ts
        .checked_sub(Duration::from_secs(interval_seconds))
        .unwrap_or_else(|| Duration::from_secs(0))
        .as_secs()
}

/// Current time since UNIX EPOCH. In milliseconds.
pub fn get_current_time() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_client_config_from_ballista_config() {
        let ballista_config = BallistaConfig::default();
        let grpc_config = GrpcClientConfig::from(&ballista_config);

        // Verify the conversion picks up the right values
        assert_eq!(
            grpc_config.connect_timeout_seconds,
            ballista_config.grpc_client_connect_timeout_seconds() as u64
        );
        assert_eq!(
            grpc_config.timeout_seconds,
            ballista_config.grpc_client_timeout_seconds() as u64
        );
        assert_eq!(
            grpc_config.tcp_keepalive_seconds,
            ballista_config.grpc_client_tcp_keepalive_seconds() as u64
        );
        assert_eq!(
            grpc_config.http2_keepalive_interval_seconds,
            ballista_config.grpc_client_http2_keepalive_interval_seconds() as u64
        );
    }

    #[test]
    fn test_create_grpc_client_endpoint_with_config() {
        let config = GrpcClientConfig {
            connect_timeout_seconds: 10,
            timeout_seconds: 30,
            tcp_keepalive_seconds: 1800,
            http2_keepalive_interval_seconds: 150,
            use_tls: false,
            max_message_size: 16 * 1024 * 1024,
            io_retries_times: 3,
            io_retry_wait_time_ms: 3000,
            initial_connection_window_size: 67108864,
            initial_stream_window_size: 16777216,
        };
        let result = create_grpc_client_endpoint("http://localhost:50051", Some(&config));
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_grpc_client_endpoint_invalid_url() {
        let result = create_grpc_client_endpoint("not a valid url", None);
        assert!(result.is_err());
    }

    /// The point of binding up front is that the port is reachable before
    /// anything is served on it, so a peer told to connect back cannot arrive
    /// too early.
    #[tokio::test]
    async fn test_create_grpc_server_incoming_binds_eagerly() {
        let incoming = create_grpc_server_incoming(
            "127.0.0.1:0".parse().unwrap(),
            &GrpcServerConfig::default(),
        )
        .expect("bind");
        let addr = incoming.local_addr().expect("local addr");

        // `incoming` is never handed to a server, and yet:
        tokio::net::TcpStream::connect(addr)
            .await
            .expect("port is already listening");
    }

    /// Binding eagerly means a port conflict surfaces here, as an error, rather
    /// than later inside the spawned server task.
    #[tokio::test]
    async fn test_create_grpc_server_incoming_port_in_use() {
        let first = create_grpc_server_incoming(
            "127.0.0.1:0".parse().unwrap(),
            &GrpcServerConfig::default(),
        )
        .expect("bind");
        let addr = first.local_addr().expect("local addr");

        let result = create_grpc_server_incoming(addr, &GrpcServerConfig::default());
        assert!(result.is_err());
    }

    /// The disk writer's work counts as the task running: while it writes a
    /// batch, the vcore stays with the task even if the input is waiting on
    /// remote I/O, and is lent only once the writer waits for input.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disk_writer_keeps_the_vcore_while_it_writes() {
        use crate::vcore_permit::{VcorePermit, install_hook_for_tests};
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::ipc::reader::StreamReader;
        use datafusion::common::runtime::IoWaitExt;
        use tokio::sync::{Semaphore, oneshot};

        const LEND: Duration = Duration::from_millis(20);
        const WITHIN: Duration = Duration::from_secs(5);
        install_hook_for_tests();
        let vcores = Arc::new(Semaphore::new(1));
        let permit = VcorePermit::new(Arc::clone(&vcores), LEND);
        permit.acquire().await.expect("vcore");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .expect("batch");
        let expected = vec![batch.clone(), batch.clone()];
        let (remote_started, remote_waiting) = oneshot::channel();
        let (release_remote, remote_ready) = oneshot::channel();
        let remote = Box::pin(async move {
            remote_started.send(()).expect("remote started");
            remote_ready.await.expect("release remote");
            Ok(batch)
        })
        .io_wait();
        let mut stream = futures::stream::iter([Ok(expected[0].clone())])
            .chain(futures::stream::once(remote));

        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("data.arrow");
        let options = create_write_options(Some(CompressionType::ZSTD)).unwrap();
        let writer = StreamWriter::try_new_with_options(
            BufWriter::new(File::create(&path).unwrap()),
            schema.as_ref(),
            options,
        )
        .unwrap();
        let (write_started, writing) = oneshot::channel();
        let (release_write, write_ready) = std::sync::mpsc::channel();
        // Hold the first write inside the real blocking writer until the test
        // releases it. Its duration no longer depends on compression speed.
        let writer = (writer, Some((write_started, write_ready)));
        let task = tokio::spawn(permit.run(async move {
            write_batches_blocking(&mut stream, writer, 8, |(writer, gate), batch| {
                if let Some((started, ready)) = gate.take() {
                    started.send(()).expect("write started");
                    ready.recv().expect("release write");
                }
                writer.write(batch)?;
                Ok(())
            })
            .await
            .expect("write batches")
        }));

        tokio::time::timeout(WITHIN, writing)
            .await
            .expect("writer starts")
            .expect("writer signal");
        tokio::time::timeout(WITHIN, remote_waiting)
            .await
            .expect("input waits on remote source")
            .expect("remote signal");
        assert!(
            tokio::time::timeout(LEND * 5, Arc::clone(&vcores).acquire_owned())
                .await
                .is_err(),
            "lent the vcore while the blocking writer was active"
        );

        release_write.send(()).expect("release first write");
        let lent = tokio::time::timeout(WITHIN, Arc::clone(&vcores).acquire_owned())
            .await
            .expect("never lent while the writer waited for input")
            .expect("lent vcore");
        drop(lent);
        release_remote.send(()).expect("release second batch");
        let written = tokio::time::timeout(WITHIN, task)
            .await
            .expect("writer finishes")
            .expect("task");
        assert_eq!(written.num_rows, 6);
        assert_eq!(written.num_batches, 2);
        let (mut writer, _) = written.writer;
        writer.finish().expect("finish IPC stream");
        drop(writer);
        let actual: Vec<_> = StreamReader::try_new(File::open(path).unwrap(), None)
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .expect("read batches");
        assert_eq!(actual, expected);
        permit.close();
        assert_eq!(vcores.available_permits(), 1);
    }

    /// A task holding a vcore can always get a blocking thread for its disk
    /// writer, even when a lent task's writer is open and every blocking
    /// thread is otherwise taken. A lent task needs that vcore before it can
    /// feed its writer, so a writer parked on a blocking thread while it waits
    /// would deadlock the two.
    #[test]
    fn lent_task_writer_leaves_blocking_threads_for_the_task_holding_the_vcore() {
        use crate::vcore_permit::{VcorePermit, install_hook_for_tests};
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::runtime::IoWaitExt;
        use datafusion::physical_plan::metrics::{
            ExecutionPlanMetricsSet, MetricBuilder,
        };
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use std::time::Instant;
        use tokio::sync::Semaphore;

        const LEND: Duration = Duration::from_millis(20);
        const WITHIN: Duration = Duration::from_secs(5);
        install_hook_for_tests();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let schema =
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
            )
            .expect("batch");
            let dir = tempfile::tempdir().expect("dir");
            let metrics = ExecutionPlanMetricsSet::new();
            let write_time = MetricBuilder::new(&metrics).subset_time("write_time", 0);
            let write = |input, name: &str| {
                let mut stream: Pin<Box<dyn RecordBatchStream + Send>> =
                    Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), input));
                let path = dir.path().join(name);
                let write_time = write_time.clone();
                async move {
                    write_stream_to_disk(&mut stream, &path, &write_time, 8, None)
                        .await
                        .expect("write")
                        .num_rows
                }
            };
            let vcores = Arc::new(Semaphore::new(1));

            // Its input waits on a remote source, so its vcore is lent with its
            // writer open.
            let lent = VcorePermit::new(Arc::clone(&vcores), LEND);
            lent.acquire().await.expect("vcore");
            let remote_batch = batch.clone();
            let remote = Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok(remote_batch)
            })
            .io_wait();
            let lent_task = tokio::spawn(
                lent.run(write(futures::stream::once(remote).boxed(), "lent.arrow")),
            );
            let deadline = Instant::now() + WITHIN;
            while vcores.available_permits() == 0 && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            assert_eq!(vcores.available_permits(), 1, "never lent");

            // Another task takes the lent vcore and writes.
            let holder = VcorePermit::new(Arc::clone(&vcores), LEND);
            holder.acquire().await.expect("vcore");
            let holder_task = tokio::spawn(holder.run(write(
                futures::stream::iter([Ok(batch.clone())]).boxed(),
                "holder.arrow",
            )));

            let rows = tokio::time::timeout(WITHIN, holder_task)
                .await
                .expect("the task holding the vcore never got a blocking thread")
                .expect("join");
            assert_eq!(rows, Some(3));
            holder.close();
            let rows = tokio::time::timeout(WITHIN, lent_task)
                .await
                .expect("the lent task never finished")
                .expect("join");
            assert_eq!(rows, Some(3));
        });
    }

    /// A batch received while the previous write was in flight is written as
    /// soon as that write ends, not once the next batch arrives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn buffered_batch_is_written_while_the_input_waits() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::metrics::{
            ExecutionPlanMetricsSet, MetricBuilder,
        };
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use std::time::Instant;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..1_000_000))],
        )
        .expect("batch");
        let batch_bytes = batch.get_array_memory_size() as u64;
        // A and B arrive together, so B is buffered while A is written; C then
        // waits on a slow source.
        let last = batch.clone();
        let slow = futures::stream::once(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(last)
        });
        let input =
            futures::stream::iter([Ok(batch.clone()), Ok(batch.clone())]).chain(slow);
        let mut stream: Pin<Box<dyn RecordBatchStream + Send>> =
            Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), input));

        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("data.arrow");
        let metrics = ExecutionPlanMetricsSet::new();
        let write_time = MetricBuilder::new(&metrics).subset_time("write_time", 0);
        let writing_path = path.clone();
        let task = tokio::spawn(async move {
            write_stream_to_disk(&mut stream, &writing_path, &write_time, 8, None)
                .await
                .expect("write")
        });

        let file_len = || std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let deadline = Instant::now() + Duration::from_secs(1);
        while file_len() < 2 * batch_bytes && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            file_len() >= 2 * batch_bytes,
            "only {} of {} bytes on disk while the input waited",
            file_len(),
            2 * batch_bytes
        );
        assert!(!task.is_finished(), "the slow batch arrived early");
        let stats = task.await.expect("task");
        assert_eq!(stats.num_rows(), Some(3_000_000));
        assert_eq!(stats.num_batches(), Some(3));
    }
}
