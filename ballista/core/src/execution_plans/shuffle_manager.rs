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

//! In-memory shuffle manager for storing shuffle data in executor memory.
//!
//! This module provides a thread-safe in-memory store for shuffle data that
//! can be used as an alternative to disk-based shuffle storage. When enabled,
//! shuffle writers store data in memory and shuffle readers fetch it directly
//! from memory instead of reading from disk.

use dashmap::DashMap;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::config::ShuffleFormat;
use crate::error::{BallistaError, Result};

/// Key for identifying a shuffle partition in the in-memory store.
/// Format: "{job_id}/{stage_id}/{partition_id}" or "{job_id}/{stage_id}/{output_partition}/{input_partition}"
pub type ShufflePartitionKey = String;

/// In-memory representation of shuffle data.
/// Supports both Arrow RecordBatch and Vortex formats.
#[derive(Debug, Clone)]
pub enum InMemoryShuffleData {
    /// Arrow RecordBatch format (default)
    Arrow(Vec<RecordBatch>),
    /// Vortex columnar format (requires 'vortex' feature)
    #[cfg(feature = "vortex")]
    Vortex(Vec<vortex_array::ArrayRef>),
}

/// Data stored for a single shuffle partition.
#[derive(Debug, Clone)]
pub struct ShufflePartitionData {
    /// The schema of the record batches
    pub schema: SchemaRef,
    /// The data for this partition (Arrow or Vortex format)
    pub data: InMemoryShuffleData,
    /// Total number of rows across all batches
    pub num_rows: u64,
    /// Total number of batches
    pub num_batches: u64,
    /// Approximate size in bytes (based on array memory size)
    pub num_bytes: u64,
    /// The format of the data
    pub format: ShuffleFormat,
}

impl ShufflePartitionData {
    /// Creates a new ShufflePartitionData from a schema and Arrow batches.
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        let num_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let num_batches = batches.len() as u64;
        let num_bytes: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        Self {
            schema,
            data: InMemoryShuffleData::Arrow(batches),
            num_rows,
            num_batches,
            num_bytes,
            format: ShuffleFormat::ArrowIpc,
        }
    }

    /// Creates a new ShufflePartitionData from a schema and Vortex arrays.
    #[cfg(feature = "vortex")]
    pub fn new_vortex(
        schema: SchemaRef,
        arrays: Vec<vortex_array::ArrayRef>,
        num_rows: u64,
        num_bytes: u64,
    ) -> Self {
        let num_batches = arrays.len() as u64;

        Self {
            schema,
            data: InMemoryShuffleData::Vortex(arrays),
            num_rows,
            num_batches,
            num_bytes,
            format: ShuffleFormat::Vortex,
        }
    }

    /// Returns the batches if stored in Arrow format, otherwise converts from Vortex.
    #[allow(deprecated)]
    pub fn to_batches(&self) -> Result<Vec<RecordBatch>> {
        match &self.data {
            InMemoryShuffleData::Arrow(batches) => Ok(batches.clone()),
            #[cfg(feature = "vortex")]
            InMemoryShuffleData::Vortex(arrays) => {
                use vortex_array::arrow::IntoArrowArray;
                arrays
                    .iter()
                    .map(|array| {
                        let arrow_array =
                            array.clone().into_arrow_preferred().map_err(|e| {
                                BallistaError::General(format!(
                                    "Failed to convert Vortex array to Arrow: {e}"
                                ))
                            })?;
                        let struct_array = arrow_array
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::StructArray>()
                            .ok_or_else(|| {
                                BallistaError::General(
                                    "Expected StructArray from Vortex conversion"
                                        .to_string(),
                                )
                            })?;
                        Ok(RecordBatch::from(struct_array))
                    })
                    .collect()
            }
        }
    }
}

/// Thread-safe in-memory storage for shuffle partition data.
///
/// This manager stores shuffle data in memory, keyed by a string that
/// uniquely identifies the shuffle partition (job_id/stage_id/partition_id).
/// It is designed to be shared across all tasks in an executor.
#[derive(Debug, Default)]
pub struct InMemoryShuffleManager {
    /// Map from partition key to partition data
    partitions: DashMap<ShufflePartitionKey, ShufflePartitionData>,
}

impl InMemoryShuffleManager {
    /// Creates a new empty in-memory shuffle manager.
    pub fn new() -> Self {
        Self {
            partitions: DashMap::new(),
        }
    }

    /// Stores shuffle partition data in memory.
    ///
    /// # Arguments
    /// * `key` - Unique identifier for the partition (e.g., "job_id/stage_id/partition_id")
    /// * `data` - The partition data to store
    pub fn store_partition(&self, key: ShufflePartitionKey, data: ShufflePartitionData) {
        log::debug!(
            "Storing shuffle partition in memory: {} ({} batches, {} rows, {} bytes)",
            key,
            data.num_batches,
            data.num_rows,
            data.num_bytes
        );
        self.partitions.insert(key, data);
    }

    /// Retrieves shuffle partition data from memory.
    ///
    /// # Arguments
    /// * `key` - Unique identifier for the partition
    ///
    /// # Returns
    /// * `Ok(ShufflePartitionData)` if the partition exists
    /// * `Err(BallistaError)` if the partition is not found
    pub fn get_partition(&self, key: &str) -> Result<ShufflePartitionData> {
        self.partitions
            .get(key)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| {
                BallistaError::General(format!(
                    "Shuffle partition not found in memory: {key}"
                ))
            })
    }

    /// Checks if a shuffle partition exists in memory.
    pub fn contains_partition(&self, key: &str) -> bool {
        self.partitions.contains_key(key)
    }

    /// Removes a shuffle partition from memory.
    ///
    /// # Returns
    /// The removed partition data if it existed
    pub fn remove_partition(&self, key: &str) -> Option<ShufflePartitionData> {
        self.partitions.remove(key).map(|(_, v)| v)
    }

    /// Removes all partitions for a given job.
    ///
    /// # Arguments
    /// * `job_id` - The job identifier
    pub fn remove_job_partitions(&self, job_id: &str) {
        let prefix = format!("{job_id}/");
        self.partitions.retain(|k, _| !k.starts_with(&prefix));
        log::debug!("Removed all shuffle partitions for job: {job_id}");
    }

    /// Removes all partitions for a given stage within a job.
    ///
    /// This is called when a stage's output has been fully consumed by the next stage,
    /// allowing the memory to be reclaimed immediately rather than waiting for job completion.
    ///
    /// # Arguments
    /// * `job_id` - The job identifier
    /// * `stage_id` - The stage identifier
    ///
    /// # Returns
    /// The number of partitions that were removed
    pub fn remove_stage_partitions(&self, job_id: &str, stage_id: usize) -> usize {
        let prefix = format!("{job_id}/{stage_id}/");
        let initial_count = self.partitions.len();
        self.partitions.retain(|k, _| !k.starts_with(&prefix));
        let removed = initial_count - self.partitions.len();
        log::debug!(
            "Removed {} shuffle partitions for stage: {}/{}",
            removed,
            job_id,
            stage_id
        );
        removed
    }

    /// Returns the total number of partitions stored in memory.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Returns the approximate total memory usage in bytes.
    pub fn total_memory_usage(&self) -> u64 {
        self.partitions
            .iter()
            .map(|entry| entry.value().num_bytes)
            .sum()
    }

    /// Generates the partition key for a simple partition (no repartitioning).
    pub fn partition_key(job_id: &str, stage_id: usize, partition_id: usize) -> String {
        format!("{job_id}/{stage_id}/{partition_id}/data")
    }

    /// Generates the partition key for a hash-partitioned output.
    pub fn hash_partition_key(
        job_id: &str,
        stage_id: usize,
        output_partition: usize,
        input_partition: usize,
    ) -> String {
        format!("{job_id}/{stage_id}/{output_partition}/data-{input_partition}")
    }

    /// Clears all stored partitions.
    pub fn clear(&self) {
        self.partitions.clear();
        log::debug!("Cleared all shuffle partitions from memory");
    }
}

/// Global in-memory shuffle manager instance.
///
/// This is a singleton that can be accessed from anywhere in the executor.
/// It is initialized lazily on first access.
static GLOBAL_SHUFFLE_MANAGER: std::sync::LazyLock<Arc<InMemoryShuffleManager>> =
    std::sync::LazyLock::new(|| Arc::new(InMemoryShuffleManager::new()));

/// Returns the global in-memory shuffle manager instance.
pub fn global_shuffle_manager() -> Arc<InMemoryShuffleManager> {
    GLOBAL_SHUFFLE_MANAGER.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_store_and_retrieve() {
        let manager = InMemoryShuffleManager::new();
        let batch = create_test_batch();
        let schema = batch.schema();
        let data = ShufflePartitionData::new(schema.clone(), vec![batch]);

        let key = InMemoryShuffleManager::partition_key("job1", 1, 0);
        manager.store_partition(key.clone(), data);

        assert!(manager.contains_partition(&key));

        let retrieved = manager.get_partition(&key).unwrap();
        assert_eq!(retrieved.num_rows, 3);
        assert_eq!(retrieved.num_batches, 1);
        let batches = retrieved.to_batches().unwrap();
        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn test_remove_job_partitions() {
        let manager = InMemoryShuffleManager::new();
        let batch = create_test_batch();
        let schema = batch.schema();

        // Store partitions for two jobs
        for job in ["job1", "job2"] {
            for stage in 0..2 {
                for partition in 0..3 {
                    let key =
                        InMemoryShuffleManager::partition_key(job, stage, partition);
                    let data =
                        ShufflePartitionData::new(schema.clone(), vec![batch.clone()]);
                    manager.store_partition(key, data);
                }
            }
        }

        assert_eq!(manager.partition_count(), 12);

        manager.remove_job_partitions("job1");
        assert_eq!(manager.partition_count(), 6);

        // Verify job2 partitions still exist
        let key = InMemoryShuffleManager::partition_key("job2", 0, 0);
        assert!(manager.contains_partition(&key));
    }

    #[test]
    fn test_hash_partition_key() {
        let key = InMemoryShuffleManager::hash_partition_key("job1", 1, 2, 3);
        assert_eq!(key, "job1/1/2/data-3");
    }

    #[test]
    fn test_remove_stage_partitions() {
        let manager = InMemoryShuffleManager::new();
        let batch = create_test_batch();
        let schema = batch.schema();

        // Store partitions for multiple stages in the same job
        for stage in 0..3 {
            for partition in 0..4 {
                let key = InMemoryShuffleManager::partition_key("job1", stage, partition);
                let data = ShufflePartitionData::new(schema.clone(), vec![batch.clone()]);
                manager.store_partition(key, data);
            }
        }

        assert_eq!(manager.partition_count(), 12);

        // Remove stage 1 partitions
        let removed = manager.remove_stage_partitions("job1", 1);
        assert_eq!(removed, 4);
        assert_eq!(manager.partition_count(), 8);

        // Verify stage 0 and 2 partitions still exist
        let key0 = InMemoryShuffleManager::partition_key("job1", 0, 0);
        let key2 = InMemoryShuffleManager::partition_key("job1", 2, 0);
        assert!(manager.contains_partition(&key0));
        assert!(manager.contains_partition(&key2));

        // Verify stage 1 partitions are gone
        let key1 = InMemoryShuffleManager::partition_key("job1", 1, 0);
        assert!(!manager.contains_partition(&key1));
    }

    #[test]
    fn test_remove_stage_partitions_different_jobs() {
        let manager = InMemoryShuffleManager::new();
        let batch = create_test_batch();
        let schema = batch.schema();

        // Store partitions for stage 1 in two different jobs
        for job in ["job1", "job2"] {
            for partition in 0..3 {
                let key = InMemoryShuffleManager::partition_key(job, 1, partition);
                let data = ShufflePartitionData::new(schema.clone(), vec![batch.clone()]);
                manager.store_partition(key, data);
            }
        }

        assert_eq!(manager.partition_count(), 6);

        // Remove stage 1 from job1 only
        let removed = manager.remove_stage_partitions("job1", 1);
        assert_eq!(removed, 3);
        assert_eq!(manager.partition_count(), 3);

        // Verify job2 stage 1 partitions still exist
        let key = InMemoryShuffleManager::partition_key("job2", 1, 0);
        assert!(manager.contains_partition(&key));
    }

    #[test]
    fn test_remove_partition_returns_data() {
        let manager = InMemoryShuffleManager::new();
        let batch = create_test_batch();
        let schema = batch.schema();
        let data = ShufflePartitionData::new(schema.clone(), vec![batch]);

        let key = InMemoryShuffleManager::partition_key("job1", 1, 0);
        manager.store_partition(key.clone(), data);

        assert!(manager.contains_partition(&key));

        // Remove should return the data
        let removed = manager.remove_partition(&key);
        assert!(removed.is_some());
        let removed_data = removed.unwrap();
        assert_eq!(removed_data.num_rows, 3);
        assert_eq!(removed_data.num_batches, 1);

        // Partition should no longer exist
        assert!(!manager.contains_partition(&key));

        // Second remove should return None
        let removed_again = manager.remove_partition(&key);
        assert!(removed_again.is_none());
    }

    #[test]
    fn test_total_memory_usage() {
        let manager = InMemoryShuffleManager::new();
        let batch = create_test_batch();
        let schema = batch.schema();

        // Store multiple partitions
        for i in 0..3 {
            let key = InMemoryShuffleManager::partition_key("job1", 1, i);
            let data = ShufflePartitionData::new(schema.clone(), vec![batch.clone()]);
            manager.store_partition(key, data);
        }

        // Memory usage should be > 0
        let usage = manager.total_memory_usage();
        assert!(usage > 0);

        // Remove partitions and verify usage decreases
        manager.remove_job_partitions("job1");
        assert_eq!(manager.total_memory_usage(), 0);
    }

    #[test]
    fn test_clear() {
        let manager = InMemoryShuffleManager::new();
        let batch = create_test_batch();
        let schema = batch.schema();

        for i in 0..5 {
            let key = InMemoryShuffleManager::partition_key("job1", 1, i);
            let data = ShufflePartitionData::new(schema.clone(), vec![batch.clone()]);
            manager.store_partition(key, data);
        }

        assert_eq!(manager.partition_count(), 5);
        manager.clear();
        assert_eq!(manager.partition_count(), 0);
    }
}
