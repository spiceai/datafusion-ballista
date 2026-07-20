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

//! Shuffle storage abstraction for storing shuffle data on local disk or object stores.
//!
//! This module provides a unified interface for reading and writing shuffle data
//! to different storage backends including local filesystem, Amazon S3, and Azure Blob Storage.

use crate::JobId;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::metrics;
use futures::StreamExt;
use log::{debug, error};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path as ObjectPath;
use object_store::prefix::PrefixStore;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use std::fmt::{Debug, Display};
use std::fs::File;
use std::io::{BufReader, Cursor};
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

use crate::error::{BallistaError, Result};
use crate::serde::scheduler::PartitionStats;

/// Defines the type of storage to use for shuffle data.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ShuffleStorageType {
    /// Store shuffle data on local disk (default behavior).
    #[default]
    Local,
    /// Store shuffle data in Amazon S3.
    S3,
    /// Store shuffle data in Azure Blob Storage (ABFS).
    Azure,
}

impl Display for ShuffleStorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShuffleStorageType::Local => write!(f, "local"),
            ShuffleStorageType::S3 => write!(f, "s3"),
            ShuffleStorageType::Azure => write!(f, "azure"),
        }
    }
}

impl std::str::FromStr for ShuffleStorageType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "local" | "disk" => Ok(ShuffleStorageType::Local),
            "s3" | "aws" => Ok(ShuffleStorageType::S3),
            "azure" | "abfs" | "adls" => Ok(ShuffleStorageType::Azure),
            _ => Err(format!(
                "Unknown shuffle storage type: '{}'. Valid options are: local, s3, azure",
                s
            )),
        }
    }
}

/// Configuration for S3 shuffle storage.
#[derive(Clone, Debug, Default)]
pub struct S3ShuffleConfig {
    /// S3 bucket name for shuffle data.
    pub bucket: Option<String>,
    /// AWS region.
    pub region: Option<String>,
    /// S3 endpoint URL (for MinIO or custom S3-compatible storage).
    pub endpoint: Option<String>,
    /// AWS access key ID.
    pub access_key_id: Option<String>,
    /// AWS secret access key.
    pub secret_access_key: Option<String>,
    /// Allow HTTP connections (default is HTTPS only).
    pub allow_http: bool,
}

/// Configuration for Azure Blob Storage shuffle storage.
#[derive(Clone, Debug, Default)]
pub struct AzureShuffleConfig {
    /// Azure storage account name.
    pub account: Option<String>,
    /// Azure storage container name.
    pub container: Option<String>,
    /// Azure storage access key.
    pub access_key: Option<String>,
    /// Azure SAS token (alternative to access key).
    pub sas_token: Option<String>,
}

/// Configuration for shuffle storage.
#[derive(Clone, Debug, Default)]
pub struct ShuffleStorageConfig {
    /// The type of storage to use.
    pub storage_type: ShuffleStorageType,
    /// Base URL/path for shuffle data storage.
    /// For local: file path (e.g., /tmp/ballista)
    /// For S3: s3://bucket/prefix
    /// For Azure: abfs://container@account.dfs.core.windows.net/prefix
    pub base_url: Option<String>,
    /// S3-specific configuration.
    pub s3_config: S3ShuffleConfig,
    /// Azure-specific configuration.
    pub azure_config: AzureShuffleConfig,
}

impl ShuffleStorageConfig {
    /// Creates a new local storage configuration with the given work directory.
    pub fn new_local(work_dir: &str) -> Self {
        Self {
            storage_type: ShuffleStorageType::Local,
            base_url: Some(work_dir.to_string()),
            ..Default::default()
        }
    }

    /// Creates a new S3 storage configuration.
    pub fn new_s3(bucket: &str, prefix: Option<&str>, region: Option<&str>) -> Self {
        let base_url = match prefix {
            Some(p) => format!("s3://{}/{}", bucket, p),
            None => format!("s3://{}", bucket),
        };
        Self {
            storage_type: ShuffleStorageType::S3,
            base_url: Some(base_url),
            s3_config: S3ShuffleConfig {
                bucket: Some(bucket.to_string()),
                region: region.map(|s| s.to_string()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Creates a new Azure Blob Storage configuration.
    pub fn new_azure(account: &str, container: &str, prefix: Option<&str>) -> Self {
        let base_url = match prefix {
            Some(p) => format!(
                "abfs://{}@{}.dfs.core.windows.net/{}",
                container, account, p
            ),
            None => format!("abfs://{}@{}.dfs.core.windows.net", container, account),
        };
        Self {
            storage_type: ShuffleStorageType::Azure,
            base_url: Some(base_url),
            azure_config: AzureShuffleConfig {
                account: Some(account.to_string()),
                container: Some(container.to_string()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Creates a storage configuration from a storage type and URL.
    ///
    /// Parses the URL to extract backend-specific fields (bucket, account, container, prefix).
    /// Credentials are resolved from environment variables by the underlying object store builders.
    pub fn from_type_and_url(
        storage_type: ShuffleStorageType,
        url: &str,
    ) -> Result<Self> {
        match storage_type {
            ShuffleStorageType::Local => Ok(Self::new_local(url)),
            ShuffleStorageType::S3 => {
                let parsed = Url::parse(url).map_err(|e| {
                    BallistaError::General(format!(
                        "Failed to parse S3 shuffle URL '{url}': {e}"
                    ))
                })?;
                let bucket = parsed.host_str().ok_or_else(|| {
                    BallistaError::General(format!(
                        "No bucket found in S3 shuffle URL '{url}'"
                    ))
                })?;
                let path = parsed.path().trim_start_matches('/');
                let prefix = if path.is_empty() { None } else { Some(path) };
                Ok(Self::new_s3(bucket, prefix, None))
            }
            ShuffleStorageType::Azure => {
                let parsed = Url::parse(url).map_err(|e| {
                    BallistaError::General(format!(
                        "Failed to parse Azure shuffle URL '{url}': {e}"
                    ))
                })?;
                // Azure URL format: abfs://container@account.dfs.core.windows.net/prefix
                let host = parsed.host_str().ok_or_else(|| {
                    BallistaError::General(format!(
                        "No host found in Azure shuffle URL '{url}'"
                    ))
                })?;
                let account = host
                    .strip_suffix(".dfs.core.windows.net")
                    .or_else(|| host.strip_suffix(".blob.core.windows.net"))
                    .ok_or_else(|| {
                        BallistaError::General(format!(
                            "Cannot extract Azure account name from host '{host}' in URL '{url}'"
                        ))
                    })?;
                let container = parsed.username();
                if container.is_empty() {
                    return Err(BallistaError::General(format!(
                        "No container found in Azure shuffle URL '{url}'. Expected format: abfs://container@account.dfs.core.windows.net/prefix"
                    )));
                }
                let path = parsed.path().trim_start_matches('/');
                let prefix = if path.is_empty() { None } else { Some(path) };
                Ok(Self::new_azure(account, container, prefix))
            }
        }
    }
}

/// Trait for shuffle storage operations.
#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait ShuffleStorage: Send + Sync + Debug {
    /// Write a record batch to storage and return the path where it was written.
    async fn write_shuffle_data(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition_id: usize,
        input_partition: usize,
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        write_metric: &metrics::Time,
    ) -> Result<(String, PartitionStats)>;

    /// Read shuffle data from storage.
    async fn read_shuffle_data(&self, path: &str) -> Result<Vec<RecordBatch>>;

    /// Delete shuffle data for a job.
    async fn delete_job_data(&self, job_id: &JobId) -> Result<()>;

    /// Get the base path/URL for this storage.
    fn base_path(&self) -> &str;

    /// Check if a path is accessible by this storage backend.
    fn can_handle(&self, path: &str) -> bool;
}

/// Local filesystem shuffle storage implementation.
#[derive(Debug)]
pub struct LocalShuffleStorage {
    work_dir: String,
}

impl LocalShuffleStorage {
    /// Creates a new local shuffle storage with the given work directory.
    pub fn new(work_dir: &str) -> Self {
        Self {
            work_dir: work_dir.to_string(),
        }
    }
}

#[async_trait]
impl ShuffleStorage for LocalShuffleStorage {
    async fn write_shuffle_data(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition_id: usize,
        input_partition: usize,
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        write_metric: &metrics::Time,
    ) -> Result<(String, PartitionStats)> {
        let mut path = PathBuf::from(&self.work_dir);
        path.push(job_id.as_str());
        path.push(format!("{}", stage_id));
        path.push(format!("{}", partition_id));
        std::fs::create_dir_all(&path)?;

        let filename = if input_partition == partition_id {
            "data.arrow".to_string()
        } else {
            format!("data-{}.arrow", input_partition)
        };
        path.push(&filename);

        let path_str = path.to_str().unwrap().to_string();
        debug!("Writing shuffle data to local path: {}", path_str);

        let timer = write_metric.timer();
        let file = File::create(&path).map_err(|e| {
            error!("Failed to create shuffle file at {}: {:?}", path_str, e);
            BallistaError::IoError(e)
        })?;

        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))?;

        let mut writer =
            StreamWriter::try_new_with_options(file, schema.as_ref(), options)?;

        let mut num_rows = 0;
        let mut num_batches = 0;
        let mut num_bytes = 0;

        for batch in batches {
            num_batches += 1;
            num_rows += batch.num_rows();
            num_bytes += batch.get_array_memory_size();
            writer.write(&batch)?;
        }

        writer.finish()?;
        timer.done();

        let stats = PartitionStats::new(
            Some(num_rows as u64),
            Some(num_batches),
            Some(num_bytes as u64),
        );

        Ok((path_str, stats))
    }

    async fn read_shuffle_data(&self, path: &str) -> Result<Vec<RecordBatch>> {
        let file = File::open(path).map_err(|e| {
            BallistaError::General(format!(
                "Failed to open shuffle file at {}: {:?}",
                path, e
            ))
        })?;
        let reader = BufReader::new(file);
        let stream_reader = StreamReader::try_new(reader, None)?;

        let mut batches = Vec::new();
        for batch_result in stream_reader {
            batches.push(batch_result?);
        }

        Ok(batches)
    }

    async fn delete_job_data(&self, job_id: &JobId) -> Result<()> {
        let mut path = PathBuf::from(&self.work_dir);
        path.push(job_id.as_str());
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
        }
        Ok(())
    }

    fn base_path(&self) -> &str {
        &self.work_dir
    }

    fn can_handle(&self, path: &str) -> bool {
        // Local storage can handle paths that don't start with a URL scheme
        !path.starts_with("s3://")
            && !path.starts_with("abfs://")
            && !path.starts_with("az://")
    }
}

/// Object store based shuffle storage implementation (for S3 and Azure).
#[derive(Debug)]
pub struct ObjectStoreShuffleStorage {
    /// Either the raw bucket/container store (when `base_url` has no path) or a
    /// [`PrefixStore`] wrapping it (when `base_url` includes a path like
    /// `s3://bucket/shuffle/prefix`). All keys passed to `self.store` are
    /// job-relative — `PrefixStore` reattaches the URL path prefix transparently.
    store: Arc<dyn ObjectStore>,
    base_url: String,
    /// Path portion of `base_url`, normalised without leading/trailing slashes
    /// (e.g. "shuffle/prefix" for `s3://bucket/shuffle/prefix`). Empty when the
    /// URL has no path. Used only by [`Self::extract_object_path`] to strip the
    /// prefix from caller-supplied full URLs before handing the key to the
    /// already-prefixed `self.store`.
    path_prefix: String,
    storage_type: ShuffleStorageType,
}

/// Extracts the path component of an object-store URL as a normalised key prefix.
/// Returns "" if the URL has no path (e.g. `s3://bucket`) or fails to parse.
fn extract_path_prefix(base_url: &str) -> String {
    Url::parse(base_url)
        .ok()
        .map(|u| u.path().trim_matches('/').to_string())
        .unwrap_or_default()
}

/// Wraps `store` in a [`PrefixStore`] when `prefix` is non-empty so every
/// subsequent operation runs in the prefix's namespace; returns the store
/// unchanged otherwise.
fn apply_path_prefix<S>(store: S, prefix: &str) -> Arc<dyn ObjectStore>
where
    S: ObjectStore + 'static,
{
    if prefix.is_empty() {
        Arc::new(store)
    } else {
        Arc::new(PrefixStore::new(store, prefix.to_string()))
    }
}

impl ObjectStoreShuffleStorage {
    /// Creates a new S3 shuffle storage.
    pub fn new_s3(config: &ShuffleStorageConfig) -> Result<Self> {
        let s3_config = &config.s3_config;
        let bucket = s3_config.bucket.as_ref().ok_or_else(|| {
            BallistaError::General("S3 bucket not configured".to_string())
        })?;

        let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

        if let Some(region) = &s3_config.region {
            builder = builder.with_region(region);
        }

        if let Some(endpoint) = &s3_config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        if let (Some(access_key), Some(secret_key)) =
            (&s3_config.access_key_id, &s3_config.secret_access_key)
        {
            builder = builder
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key);
        }

        if s3_config.allow_http {
            builder = builder.with_allow_http(true);
        }

        let store = builder.build().map_err(|e| {
            BallistaError::General(format!("Failed to create S3 object store: {:?}", e))
        })?;

        let base_url = config
            .base_url
            .clone()
            .unwrap_or_else(|| format!("s3://{}", bucket));
        let path_prefix = extract_path_prefix(&base_url);
        let store = apply_path_prefix(store, &path_prefix);

        Ok(Self {
            store,
            base_url,
            path_prefix,
            storage_type: ShuffleStorageType::S3,
        })
    }

    /// Creates a new Azure Blob Storage shuffle storage.
    pub fn new_azure(config: &ShuffleStorageConfig) -> Result<Self> {
        let azure_config = &config.azure_config;
        let account = azure_config.account.as_ref().ok_or_else(|| {
            BallistaError::General("Azure storage account not configured".to_string())
        })?;
        let container = azure_config.container.as_ref().ok_or_else(|| {
            BallistaError::General("Azure storage container not configured".to_string())
        })?;

        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(account)
            .with_container_name(container);

        if let Some(access_key) = &azure_config.access_key {
            builder = builder.with_access_key(access_key);
        }

        if let Some(sas_token) = &azure_config.sas_token {
            // Parse SAS token into key-value pairs
            // SAS token format: ?sv=2021-06-08&ss=bf&srt=sco&...
            let query_pairs: Vec<(String, String)> = sas_token
                .trim_start_matches('?')
                .split('&')
                .filter_map(|pair| {
                    let mut parts = pair.splitn(2, '=');
                    match (parts.next(), parts.next()) {
                        (Some(key), Some(value)) => {
                            Some((key.to_string(), value.to_string()))
                        }
                        _ => None,
                    }
                })
                .collect();
            builder = builder.with_sas_authorization(query_pairs);
        }

        let store = builder.build().map_err(|e| {
            BallistaError::General(format!(
                "Failed to create Azure object store: {:?}",
                e
            ))
        })?;

        let base_url = config.base_url.clone().unwrap_or_else(|| {
            format!("abfs://{}@{}.dfs.core.windows.net", container, account)
        });
        let path_prefix = extract_path_prefix(&base_url);
        let store = apply_path_prefix(store, &path_prefix);

        Ok(Self {
            store,
            base_url,
            path_prefix,
            storage_type: ShuffleStorageType::Azure,
        })
    }

    /// Creates object store storage from configuration.
    pub fn from_config(config: &ShuffleStorageConfig) -> Result<Self> {
        match config.storage_type {
            ShuffleStorageType::S3 => Self::new_s3(config),
            ShuffleStorageType::Azure => Self::new_azure(config),
            ShuffleStorageType::Local => Err(BallistaError::General(
                "Use LocalShuffleStorage for local storage".to_string(),
            )),
        }
    }

    /// Constructs an `ObjectStoreShuffleStorage` for tests that need to inject an
    /// in-memory or other custom `ObjectStore` instead of building an S3 / Azure
    /// client. Applies the same `PrefixStore` wrapping as the production
    /// constructors so the test setup matches behaviour.
    #[doc(hidden)]
    pub fn new_for_test(
        inner_store: Arc<dyn ObjectStore>,
        base_url: String,
        path_prefix: String,
        storage_type: ShuffleStorageType,
    ) -> Self {
        let store = if path_prefix.is_empty() {
            inner_store
        } else {
            Arc::new(PrefixStore::new(inner_store, path_prefix.clone()))
        };
        Self {
            store,
            base_url,
            path_prefix,
            storage_type,
        }
    }

    /// Returns a reference to the underlying object store.
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// Constructs the full URL for a shuffle partition along with the job-relative
    /// object-store key. The returned `ObjectPath` is relative to the storage's
    /// `path_prefix` — `self.store` is already a [`PrefixStore`] when a prefix is set,
    /// so it reattaches the prefix transparently.
    pub fn make_full_url(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition_id: usize,
        input_partition: usize,
        file_ext: &str,
    ) -> (String, ObjectPath) {
        let relative_path =
            self.make_path(job_id, stage_id, partition_id, input_partition, file_ext);
        let full_url = format!("{}/{}", self.base_url, relative_path);
        let object_path = ObjectPath::from(relative_path);
        (full_url, object_path)
    }

    /// Starts a streaming multipart upload for a shuffle partition.
    ///
    /// Returns a `WriteMultipart` writer and the full URL where data will be written.
    /// The caller should serialize batches and write them to the returned writer,
    /// then call `finish()` to complete the upload.
    pub async fn start_multipart_write(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition_id: usize,
        input_partition: usize,
        file_ext: &str,
    ) -> Result<(WriteMultipart, String)> {
        let (full_url, object_path) =
            self.make_full_url(job_id, stage_id, partition_id, input_partition, file_ext);

        debug!("Starting multipart upload to object store: {}", full_url);

        let upload = self.store.put_multipart(&object_path).await.map_err(|e| {
            BallistaError::General(format!(
                "Failed to start multipart upload to {}: {:?}",
                full_url, e
            ))
        })?;

        let write = WriteMultipart::new(upload);
        Ok((write, full_url))
    }

    fn make_path(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition_id: usize,
        input_partition: usize,
        file_ext: &str,
    ) -> String {
        let filename = if input_partition == partition_id {
            format!("data.{file_ext}")
        } else {
            format!("data-{input_partition}.{file_ext}")
        };
        format!("{}/{}/{}/{}", job_id, stage_id, partition_id, filename)
    }
}

#[async_trait]
impl ShuffleStorage for ObjectStoreShuffleStorage {
    async fn write_shuffle_data(
        &self,
        job_id: &JobId,
        stage_id: usize,
        partition_id: usize,
        input_partition: usize,
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        write_metric: &metrics::Time,
    ) -> Result<(String, PartitionStats)> {
        let relative_path =
            self.make_path(job_id, stage_id, partition_id, input_partition, "arrow");
        let full_url = format!("{}/{}", self.base_url, relative_path);

        debug!("Writing shuffle data to object store: {}", full_url);

        let timer = write_metric.timer();

        // Write batches to an in-memory buffer first
        let mut buffer = Vec::new();
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))?;

        let (_total_rows, _total_batches) = {
            let mut writer = StreamWriter::try_new_with_options(
                Cursor::new(&mut buffer),
                schema.as_ref(),
                options,
            )?;

            let mut num_rows = 0;
            let mut num_batches = 0;

            for batch in &batches {
                num_rows += batch.num_rows();
                num_batches += 1;
                writer.write(batch)?;
            }

            writer.finish()?;
            (num_rows, num_batches)
        };

        let num_bytes = buffer.len();

        // Upload via the (possibly prefix-wrapped) store with the job-relative key.
        let object_path = ObjectPath::from(relative_path);
        let payload = PutPayload::from(Bytes::from(buffer));

        self.store.put(&object_path, payload).await.map_err(|e| {
            BallistaError::General(format!(
                "Failed to upload shuffle data to {}: {:?}",
                full_url, e
            ))
        })?;

        timer.done();

        let stats = PartitionStats::new(
            Some(batches.iter().map(|b| b.num_rows() as u64).sum()),
            Some(batches.len() as u64),
            Some(num_bytes as u64),
        );

        Ok((full_url, stats))
    }

    async fn read_shuffle_data(&self, path: &str) -> Result<Vec<RecordBatch>> {
        // Extract the object path from the full URL
        let object_path = self.extract_object_path(path)?;

        debug!("Reading shuffle data from object store: {}", path);

        let get_result = self.store.get(&object_path).await.map_err(|e| {
            BallistaError::General(format!(
                "Failed to read shuffle data from {}: {:?}",
                path, e
            ))
        })?;

        let bytes = get_result.bytes().await.map_err(|e| {
            BallistaError::General(format!("Failed to read bytes from {}: {:?}", path, e))
        })?;

        let cursor = Cursor::new(bytes.to_vec());
        let stream_reader = StreamReader::try_new(cursor, None)?;

        let mut batches = Vec::new();
        for batch_result in stream_reader {
            batches.push(batch_result?);
        }

        Ok(batches)
    }

    async fn delete_job_data(&self, job_id: &JobId) -> Result<()> {
        let prefix = ObjectPath::from(job_id.as_str().to_string());

        // List all objects with the job_id prefix (relative to the storage's path_prefix —
        // PrefixStore reattaches the URL path prefix on every operation).
        let mut list_stream = self.store.list(Some(&prefix));
        let mut objects_to_delete = Vec::new();

        while let Some(result) = list_stream.next().await {
            match result {
                Ok(meta) => objects_to_delete.push(meta.location),
                Err(e) => {
                    return Err(BallistaError::General(format!(
                        "Failed to list objects for job {}: {:?}",
                        job_id, e
                    )));
                }
            }
        }

        // Delete all objects
        for path in objects_to_delete {
            self.store.delete(&path).await.map_err(|e| {
                BallistaError::General(format!(
                    "Failed to delete object {:?}: {:?}",
                    path, e
                ))
            })?;
        }

        Ok(())
    }

    fn base_path(&self) -> &str {
        &self.base_url
    }

    fn can_handle(&self, path: &str) -> bool {
        match self.storage_type {
            ShuffleStorageType::S3 => path.starts_with("s3://"),
            ShuffleStorageType::Azure => {
                path.starts_with("abfs://") || path.starts_with("az://")
            }
            ShuffleStorageType::Local => false,
        }
    }
}

impl ObjectStoreShuffleStorage {
    /// Resolves a caller-supplied URL or path to a key relative to the storage's
    /// `path_prefix`. `self.store` is already prefix-wrapped, so handing it the
    /// full URL path would double-prefix — strip `self.path_prefix` first.
    fn extract_object_path(&self, path: &str) -> Result<ObjectPath> {
        let raw = match Url::parse(path) {
            Ok(url) => url.path().trim_start_matches('/').to_string(),
            // Not a URL — treat as an already-relative key.
            Err(_) => return Ok(ObjectPath::from(path)),
        };
        let relative = if self.path_prefix.is_empty() {
            raw.as_str()
        } else {
            raw.strip_prefix(&self.path_prefix)
                .map(|rest| rest.trim_start_matches('/'))
                // Fall back to the raw key if it doesn't start with our prefix —
                // happens in tests that hand-construct URLs against unrelated stores.
                .unwrap_or(raw.as_str())
        };
        Ok(ObjectPath::from(relative))
    }
}

/// Factory for creating shuffle storage instances.
pub struct ShuffleStorageFactory;

impl ShuffleStorageFactory {
    /// Creates a shuffle storage instance based on the configuration.
    pub fn create(config: &ShuffleStorageConfig) -> Result<Arc<dyn ShuffleStorage>> {
        match config.storage_type {
            ShuffleStorageType::Local => {
                let work_dir = config.base_url.as_ref().ok_or_else(|| {
                    BallistaError::General("Work directory not configured".to_string())
                })?;
                Ok(Arc::new(LocalShuffleStorage::new(work_dir)))
            }
            ShuffleStorageType::S3 => {
                Ok(Arc::new(ObjectStoreShuffleStorage::new_s3(config)?))
            }
            ShuffleStorageType::Azure => {
                Ok(Arc::new(ObjectStoreShuffleStorage::new_azure(config)?))
            }
        }
    }

    /// Creates a local shuffle storage with the given work directory.
    pub fn create_local(work_dir: &str) -> Arc<dyn ShuffleStorage> {
        Arc::new(LocalShuffleStorage::new(work_dir))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use tempfile::TempDir;

    fn create_test_batch() -> (RecordBatch, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        (batch, schema)
    }

    #[tokio::test]
    async fn test_local_shuffle_storage_write_read() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalShuffleStorage::new(temp_dir.path().to_str().unwrap());

        let (batch, schema) = create_test_batch();
        let metrics = ExecutionPlanMetricsSet::new();
        let time_metric =
            metrics::MetricBuilder::new(&metrics).subset_time("write_time", 0);

        let test_job = JobId::new("test_job");
        let (path, stats) = storage
            .write_shuffle_data(
                &test_job,
                1,
                0,
                0,
                vec![batch.clone()],
                schema,
                &time_metric,
            )
            .await
            .unwrap();

        assert!(path.contains("test_job"));
        assert_eq!(stats.num_rows, Some(3));
        assert_eq!(stats.num_batches, Some(1));

        let read_batches = storage.read_shuffle_data(&path).await.unwrap();
        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_local_shuffle_storage_delete() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalShuffleStorage::new(temp_dir.path().to_str().unwrap());

        let (batch, schema) = create_test_batch();
        let metrics = ExecutionPlanMetricsSet::new();
        let time_metric =
            metrics::MetricBuilder::new(&metrics).subset_time("write_time", 0);

        let test_job = JobId::new("test_job");
        let (path, _) = storage
            .write_shuffle_data(&test_job, 1, 0, 0, vec![batch], schema, &time_metric)
            .await
            .unwrap();

        assert!(std::path::Path::new(&path).exists());

        storage
            .delete_job_data(&JobId::from("test_job"))
            .await
            .unwrap();
        assert!(!std::path::Path::new(&path).exists());
    }

    #[test]
    fn test_shuffle_storage_type_parse() {
        assert_eq!(
            "local".parse::<ShuffleStorageType>().unwrap(),
            ShuffleStorageType::Local
        );
        assert_eq!(
            "s3".parse::<ShuffleStorageType>().unwrap(),
            ShuffleStorageType::S3
        );
        assert_eq!(
            "azure".parse::<ShuffleStorageType>().unwrap(),
            ShuffleStorageType::Azure
        );
        assert_eq!(
            "abfs".parse::<ShuffleStorageType>().unwrap(),
            ShuffleStorageType::Azure
        );
    }

    #[test]
    fn test_storage_config_new_local() {
        let config = ShuffleStorageConfig::new_local("/tmp/ballista");
        assert_eq!(config.storage_type, ShuffleStorageType::Local);
        assert_eq!(config.base_url, Some("/tmp/ballista".to_string()));
    }

    #[test]
    fn test_storage_config_new_s3() {
        let config =
            ShuffleStorageConfig::new_s3("my-bucket", Some("shuffle"), Some("us-east-1"));
        assert_eq!(config.storage_type, ShuffleStorageType::S3);
        assert_eq!(config.base_url, Some("s3://my-bucket/shuffle".to_string()));
        assert_eq!(config.s3_config.bucket, Some("my-bucket".to_string()));
        assert_eq!(config.s3_config.region, Some("us-east-1".to_string()));
    }

    #[test]
    fn test_storage_config_new_azure() {
        let config =
            ShuffleStorageConfig::new_azure("myaccount", "mycontainer", Some("shuffle"));
        assert_eq!(config.storage_type, ShuffleStorageType::Azure);
        assert_eq!(
            config.base_url,
            Some("abfs://mycontainer@myaccount.dfs.core.windows.net/shuffle".to_string())
        );
    }

    #[test]
    fn test_from_type_and_url_local() {
        let config = ShuffleStorageConfig::from_type_and_url(
            ShuffleStorageType::Local,
            "/tmp/ballista",
        )
        .unwrap();
        assert_eq!(config.storage_type, ShuffleStorageType::Local);
        assert_eq!(config.base_url, Some("/tmp/ballista".to_string()));
    }

    #[test]
    fn test_from_type_and_url_s3() {
        let config = ShuffleStorageConfig::from_type_and_url(
            ShuffleStorageType::S3,
            "s3://my-bucket/shuffle/prefix",
        )
        .unwrap();
        assert_eq!(config.storage_type, ShuffleStorageType::S3);
        assert_eq!(
            config.base_url,
            Some("s3://my-bucket/shuffle/prefix".to_string())
        );
        assert_eq!(config.s3_config.bucket, Some("my-bucket".to_string()));
    }

    #[test]
    fn test_from_type_and_url_s3_no_prefix() {
        let config = ShuffleStorageConfig::from_type_and_url(
            ShuffleStorageType::S3,
            "s3://my-bucket",
        )
        .unwrap();
        assert_eq!(config.storage_type, ShuffleStorageType::S3);
        assert_eq!(config.base_url, Some("s3://my-bucket".to_string()));
        assert_eq!(config.s3_config.bucket, Some("my-bucket".to_string()));
    }

    #[test]
    fn test_from_type_and_url_azure() {
        let config = ShuffleStorageConfig::from_type_and_url(
            ShuffleStorageType::Azure,
            "abfs://mycontainer@myaccount.dfs.core.windows.net/shuffle",
        )
        .unwrap();
        assert_eq!(config.storage_type, ShuffleStorageType::Azure);
        assert_eq!(
            config.base_url,
            Some("abfs://mycontainer@myaccount.dfs.core.windows.net/shuffle".to_string())
        );
        assert_eq!(config.azure_config.account, Some("myaccount".to_string()));
        assert_eq!(
            config.azure_config.container,
            Some("mycontainer".to_string())
        );
    }

    #[test]
    fn test_from_type_and_url_azure_no_prefix() {
        let config = ShuffleStorageConfig::from_type_and_url(
            ShuffleStorageType::Azure,
            "abfs://mycontainer@myaccount.dfs.core.windows.net",
        )
        .unwrap();
        assert_eq!(config.storage_type, ShuffleStorageType::Azure);
        assert_eq!(config.azure_config.account, Some("myaccount".to_string()));
    }

    #[test]
    fn test_from_type_and_url_s3_invalid_url() {
        let result =
            ShuffleStorageConfig::from_type_and_url(ShuffleStorageType::S3, "not-a-url");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_path_prefix() {
        assert_eq!(extract_path_prefix("s3://bucket"), "");
        assert_eq!(extract_path_prefix("s3://bucket/"), "");
        assert_eq!(extract_path_prefix("s3://bucket/shuffle"), "shuffle");
        assert_eq!(
            extract_path_prefix("s3://bucket/shuffle/prefix"),
            "shuffle/prefix"
        );
        assert_eq!(
            extract_path_prefix("s3://bucket/shuffle/prefix/"),
            "shuffle/prefix"
        );
        assert_eq!(
            extract_path_prefix("abfs://container@account.dfs.core.windows.net/shuffle"),
            "shuffle"
        );
        assert_eq!(extract_path_prefix("not-a-url"), "");
    }

    /// Builds an `ObjectStoreShuffleStorage` over the supplied [`InMemory`] store
    /// with the same `PrefixStore`-based wiring `new_s3` / `new_azure` apply in
    /// production. Returns the inner store so tests can directly inspect the
    /// final object key (the wrapped store would strip the prefix on listing).
    fn build_storage_for_test(
        base_url: &str,
    ) -> (ObjectStoreShuffleStorage, Arc<dyn ObjectStore>) {
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let path_prefix = extract_path_prefix(base_url);
        let store: Arc<dyn ObjectStore> = if path_prefix.is_empty() {
            Arc::clone(&inner)
        } else {
            Arc::new(PrefixStore::new(Arc::clone(&inner), path_prefix.clone()))
        };
        let storage = ObjectStoreShuffleStorage {
            store,
            base_url: base_url.to_string(),
            path_prefix,
            storage_type: ShuffleStorageType::S3,
        };
        (storage, inner)
    }

    /// `make_full_url` reports a full URL for downstream consumers but hands the
    /// store a job-relative key — `PrefixStore` reattaches the URL path prefix.
    #[test]
    fn test_make_full_url_returns_relative_object_path() {
        let (storage, _inner) = build_storage_for_test("s3://my-bucket/shuffle/prefix");

        let job_a = JobId::new("job_a");
        let (full_url, object_path) = storage.make_full_url(&job_a, 1, 40, 40, "arrow");
        assert_eq!(
            full_url,
            "s3://my-bucket/shuffle/prefix/job_a/1/40/data.arrow"
        );
        assert_eq!(object_path.as_ref(), "job_a/1/40/data.arrow");
    }

    #[test]
    fn test_make_full_url_no_prefix_round_trip() {
        let (storage, _inner) = build_storage_for_test("s3://my-bucket");

        let job_a = JobId::new("job_a");
        let (full_url, object_path) = storage.make_full_url(&job_a, 1, 0, 0, "arrow");
        assert_eq!(full_url, "s3://my-bucket/job_a/1/0/data.arrow");
        assert_eq!(object_path.as_ref(), "job_a/1/0/data.arrow");
    }

    /// Regression test for the writer-side prefix bug: an end-to-end write must
    /// land under the URL path prefix in the underlying bucket. Before the fix,
    /// the object landed at `job_a/1/0/data.arrow` while the reader looked under
    /// `shuffle/prefix/job_a/1/0/data.arrow` and got NotFound.
    #[tokio::test]
    async fn test_object_store_round_trip_with_prefix() {
        let (storage, inner) = build_storage_for_test("s3://my-bucket/shuffle/prefix");

        let (batch, schema) = create_test_batch();
        let metrics = ExecutionPlanMetricsSet::new();
        let time_metric =
            metrics::MetricBuilder::new(&metrics).subset_time("write_time", 0);

        let job_a = JobId::new("job_a");
        let (full_url, _stats) = storage
            .write_shuffle_data(&job_a, 1, 0, 0, vec![batch], schema, &time_metric)
            .await
            .unwrap();
        assert_eq!(
            full_url,
            "s3://my-bucket/shuffle/prefix/job_a/1/0/data.arrow"
        );

        // The actual S3 key in the underlying bucket must include the URL path prefix.
        let inner_keys: Vec<String> = inner
            .list(None)
            .filter_map(|r| async move { r.ok().map(|m| m.location.to_string()) })
            .collect::<Vec<_>>()
            .await;
        assert_eq!(inner_keys, vec!["shuffle/prefix/job_a/1/0/data.arrow"]);

        let read_batches = storage.read_shuffle_data(&full_url).await.unwrap();
        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 3);

        storage
            .delete_job_data(&JobId::from("job_a"))
            .await
            .unwrap();
        assert!(storage.read_shuffle_data(&full_url).await.is_err());
    }
}
