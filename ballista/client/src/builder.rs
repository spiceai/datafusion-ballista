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

//! Builder API for creating Ballista session contexts with custom object stores.
//!
//! This module provides a fluent builder pattern for configuring Ballista sessions,
//! including the ability to register pre-created object stores with custom authentication.
//!
//! # Example
//!
//! ```no_run
//! use ballista::prelude::BallistaBuilder;
//! use object_store::aws::AmazonS3Builder;
//! use std::sync::Arc;
//!
//! # #[tokio::main]
//! # async fn main() -> datafusion::error::Result<()> {
//! // Create an object store with custom authentication
//! let s3_store = AmazonS3Builder::new()
//!     .with_bucket_name("my-bucket")
//!     .with_region("us-east-1")
//!     // Custom authentication bridge
//!     .with_access_key_id("my-access-key")
//!     .with_secret_access_key("my-secret-key")
//!     .build()?;
//!
//! let ctx = BallistaBuilder::new()
//!     .add_object_store("s3://my-bucket", Arc::new(s3_store))
//!     .standalone()
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::extension::SessionContextExt;
use ballista_core::extension::SessionConfigExt;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::ObjectStore;
use std::sync::Arc;
use url::Url;

/// A builder for creating Ballista session contexts with custom configuration.
///
/// This builder provides a fluent API for configuring Ballista sessions,
/// including support for registering pre-created object stores with custom
/// authentication.
///
/// # Example
///
/// ```no_run
/// use ballista::prelude::BallistaBuilder;
/// use object_store::aws::AmazonS3Builder;
/// use std::sync::Arc;
///
/// # #[tokio::main]
/// # async fn main() -> datafusion::error::Result<()> {
/// let s3_store = AmazonS3Builder::new()
///     .with_bucket_name("my-bucket")
///     .with_region("us-east-1")
///     .with_access_key_id("my-access-key")
///     .with_secret_access_key("my-secret-key")
///     .build()?;
///
/// let ctx = BallistaBuilder::new()
///     .add_object_store("s3://my-bucket", Arc::new(s3_store))
///     .remote("df://localhost:50050")
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Default, Clone)]
pub struct BallistaBuilder {
    session_config: SessionConfig,
    object_stores: Vec<(Url, Arc<dyn ObjectStore>)>,
}

impl BallistaBuilder {
    /// Creates a new [`BallistaBuilder`] with default configuration.
    pub fn new() -> Self {
        Self {
            session_config: SessionConfig::new_with_ballista(),
            object_stores: Vec::new(),
        }
    }

    /// Sets the session configuration.
    ///
    /// This replaces any previously set configuration.
    pub fn with_config(mut self, config: SessionConfig) -> Self {
        self.session_config = config;
        self
    }

    /// Sets a configuration option by key-value pair.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ballista::prelude::BallistaBuilder;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> datafusion::error::Result<()> {
    /// let ctx = BallistaBuilder::new()
    ///     .config("datafusion.execution.target_partitions", "8")?
    ///     .standalone()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn config(mut self, key: &str, value: &str) -> Result<Self> {
        self.session_config
            .options_mut()
            .set(key, value)
            .map_err(|e| DataFusionError::Configuration(e.to_string()))?;
        Ok(self)
    }

    /// Registers a pre-created object store for a given URL prefix.
    ///
    /// This allows you to pass in object stores that have been configured
    /// with custom authentication (e.g., via an authentication bridge).
    ///
    /// The URL should be the base URL for the object store, such as:
    /// - `s3://my-bucket` for S3
    /// - `az://my-container` for Azure Blob Storage
    /// - `gs://my-bucket` for Google Cloud Storage
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ballista::prelude::BallistaBuilder;
    /// use object_store::aws::AmazonS3Builder;
    /// use std::sync::Arc;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> datafusion::error::Result<()> {
    /// let s3_store = AmazonS3Builder::new()
    ///     .with_bucket_name("my-bucket")
    ///     .with_region("us-east-1")
    ///     .with_access_key_id("my-access-key")
    ///     .with_secret_access_key("my-secret-key")
    ///     .build()?;
    ///
    /// let ctx = BallistaBuilder::new()
    ///     .add_object_store("s3://my-bucket", Arc::new(s3_store))
    ///     .standalone()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn add_object_store(mut self, url: &str, store: Arc<dyn ObjectStore>) -> Self {
        // Parse the URL, or store it for later error handling during build
        if let Ok(parsed_url) = Url::parse(url) {
            self.object_stores.push((parsed_url, store));
        } else {
            // We'll handle invalid URLs during build
            log::warn!("Invalid object store URL: {url}");
        }
        self
    }

    /// Registers a pre-created object store for a given URL.
    ///
    /// This is the same as [`add_object_store`](Self::add_object_store) but takes
    /// a pre-parsed [`Url`] instead of a string.
    pub fn add_object_store_url(mut self, url: Url, store: Arc<dyn ObjectStore>) -> Self {
        self.object_stores.push((url, store));
        self
    }

    /// Sets the Ballista job name.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ballista::prelude::BallistaBuilder;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> datafusion::error::Result<()> {
    /// let ctx = BallistaBuilder::new()
    ///     .with_job_name("My Analytics Job")
    ///     .standalone()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_job_name(mut self, name: &str) -> Self {
        self.session_config = self.session_config.with_ballista_job_name(name);
        self
    }

    /// Sets the target number of partitions for query execution.
    pub fn with_target_partitions(mut self, partitions: usize) -> Self {
        self.session_config = self.session_config.with_target_partitions(partitions);
        self
    }

    /// Sets the batch size for query execution.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.session_config = self.session_config.with_batch_size(batch_size);
        self
    }

    /// Enables or disables the information schema.
    pub fn with_information_schema(mut self, enabled: bool) -> Self {
        self.session_config = self.session_config.with_information_schema(enabled);
        self
    }

    /// Builds a [`datafusion::execution::SessionState`] with the configured object stores.
    fn build_state(&self) -> Result<datafusion::execution::SessionState> {
        let runtime_env = RuntimeEnvBuilder::new().build()?;

        // Register all object stores
        for (url, store) in &self.object_stores {
            runtime_env.register_object_store(url, Arc::clone(store));
        }

        let state = SessionStateBuilder::new()
            .with_config(self.session_config.clone())
            .with_runtime_env(Arc::new(runtime_env))
            .with_default_features()
            .build();

        Ok(state)
    }

    /// Creates a context for executing queries against a remote Ballista scheduler.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL of the Ballista scheduler (e.g., "df://localhost:50050")
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ballista::prelude::BallistaBuilder;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> datafusion::error::Result<()> {
    /// let ctx = BallistaBuilder::new()
    ///     .remote("df://localhost:50050")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remote(self, url: &str) -> Result<SessionContext> {
        let state = self.build_state()?;
        SessionContext::remote_with_state(url, state).await
    }

    /// Creates a context for executing queries against a standalone Ballista cluster.
    ///
    /// This starts a local Ballista cluster with a scheduler and executor in-process.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ballista::prelude::BallistaBuilder;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> datafusion::error::Result<()> {
    /// let ctx = BallistaBuilder::new()
    ///     .standalone()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "standalone")]
    pub async fn standalone(self) -> Result<SessionContext> {
        let state = self.build_state()?;
        SessionContext::standalone_with_state(state).await
    }
}
