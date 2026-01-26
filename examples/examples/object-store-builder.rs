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

use ballista::prelude::BallistaBuilder;
use datafusion::error::Result;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;

/// Bucket name to be used for this example
const S3_BUCKET: &str = "ballista";
/// S3 access key
const S3_ACCESS_KEY_ID: &str = "MINIO";
/// S3 secret key
const S3_SECRET_KEY: &str = "MINIOSECRET";
/// S3 endpoint
const S3_ENDPOINT: &str = "http://localhost:9000";

///
/// # Using BallistaBuilder with Pre-Created Object Store
///
/// This example demonstrates how to use the `BallistaBuilder` API to create
/// a Ballista context with a pre-created object store. This is useful when
/// you have custom authentication (e.g., an authentication bridge) that needs
/// to configure the object store before passing it to Ballista.
///
/// ## Prerequisites
///
/// Start minio to act as S3 object store:
///
/// ```bash
/// docker run --rm -p 9000:9000 -p 9001:9001 \
///     -e "MINIO_ACCESS_KEY=MINIO" \
///     -e "MINIO_SECRET_KEY=MINIOSECRET" \
///     quay.io/minio/minio server /data --console-address ":9001"
/// ```
///
/// Then run this example:
///
/// ```bash
/// cargo run --example object-store-builder
/// ```
///
/// ## Using the BallistaBuilder
///
/// The `BallistaBuilder` provides a fluent API for creating Ballista contexts:
///
/// ```rust,no_run
/// use ballista::prelude::BallistaBuilder;
/// use object_store::aws::AmazonS3Builder;
/// use std::sync::Arc;
///
/// // Create an object store with your custom authentication
/// let s3_store = AmazonS3Builder::new()
///     .with_bucket_name("my-bucket")
///     .with_region("us-east-1")
///     .with_access_key_id("my-access-key")
///     .with_secret_access_key("my-secret-key")
///     .build()
///     .unwrap();
///
/// // Use the builder to create a context with the object store
/// let ctx = BallistaBuilder::new()
///     .with_object_store("s3://my-bucket", Arc::new(s3_store))
///     .standalone()
///     .await
///     .unwrap();
/// ```
#[tokio::main]
async fn main() -> Result<()> {
    let test_data = ballista_examples::test_util::examples_test_data();

    // Create an S3 object store with custom configuration
    // This could be configured via an authentication bridge
    let s3_store = AmazonS3Builder::new()
        .with_bucket_name(S3_BUCKET)
        .with_endpoint(S3_ENDPOINT)
        .with_access_key_id(S3_ACCESS_KEY_ID)
        .with_secret_access_key(S3_SECRET_KEY)
        .with_allow_http(true)
        .build()?;

    // Use BallistaBuilder to create a context with the pre-created object store
    let ctx = BallistaBuilder::new()
        .with_job_name("Object Store Builder Example")
        .add_object_store(&format!("s3://{S3_BUCKET}"), Arc::new(s3_store))
        .standalone()
        .await?;

    // Register a local parquet file
    ctx.register_parquet(
        "test",
        &format!("{test_data}/alltypes_plain.parquet"),
        Default::default(),
    )
    .await?;

    // Write data to S3
    let write_dir_path = &format!("s3://{S3_BUCKET}/builder_example.parquet");
    ctx.sql("SELECT * FROM test")
        .await?
        .write_parquet(write_dir_path, Default::default(), Default::default())
        .await?;

    println!("Successfully wrote data to {}", write_dir_path);

    // Read the data back from S3
    ctx.register_parquet("s3_table", write_dir_path, Default::default())
        .await?;

    let result = ctx
        .sql("SELECT id, string_col, timestamp_col FROM s3_table WHERE id > 4")
        .await?
        .collect()
        .await?;

    println!("Query results:");
    datafusion::arrow::util::pretty::print_batches(&result)?;

    Ok(())
}
