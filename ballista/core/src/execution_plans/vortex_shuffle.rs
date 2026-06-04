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

//! Vortex format support for shuffle operations.
//!
//! This module provides Vortex-based serialization for shuffle data,
//! offering an alternative to Arrow IPC format with potentially better
//! compression and performance characteristics.
//!
//! Vortex IPC format is used for streaming data between processes.

use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use log::debug;

use vortex_array::ArrayRef;
use vortex_array::LEGACY_SESSION;
use vortex_array::arrow::FromArrowArray;
use vortex_array::arrow::IntoArrowArray;
use vortex_array::iter::ArrayIteratorAdapter;
use vortex_error::VortexResult;
use vortex_ipc::iterator::{ArrayIteratorIPC, SyncIPCReader};

use crate::error::BallistaError;
use crate::serde::scheduler::PartitionStats;

/// Writer for Vortex format shuffle data
pub struct VortexWriteTracker {
    /// Number of record batches written
    pub num_batches: usize,
    /// Total number of rows written
    pub num_rows: usize,
    /// Path to the output file
    pub path: PathBuf,
    file: BufWriter<File>,
    #[allow(dead_code)] // May be needed for schema validation in the future
    schema: SchemaRef,
    buffer: Vec<ArrayRef>,
}

impl VortexWriteTracker {
    /// Create a new Vortex writer for the given path
    pub fn try_new(path: PathBuf, schema: SchemaRef) -> Result<Self> {
        let file = File::create(&path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            num_batches: 0,
            num_rows: 0,
            path,
            file: writer,
            schema,
            buffer: Vec::new(),
        })
    }

    /// Write a record batch to the buffer
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        // Convert Arrow RecordBatch to Vortex Array
        let vortex_array = ArrayRef::from_arrow(batch, false)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        self.buffer.push(vortex_array);
        self.num_batches += 1;
        self.num_rows += batch.num_rows();
        Ok(())
    }

    /// Finish writing and close the file
    pub fn finish(mut self) -> Result<()> {
        // Write all buffered arrays using IPC format
        if !self.buffer.is_empty() {
            // Get the dtype from the first array
            let dtype = self.buffer[0].dtype().clone();

            // Create an ArrayIterator from the buffer
            let iter = self
                .buffer
                .into_iter()
                .map(|a| Ok(a) as VortexResult<ArrayRef>);
            let array_iter = ArrayIteratorAdapter::new(dtype, iter);

            // Convert to IPC bytes
            let ipc_data = array_iter
                .into_ipc(&*LEGACY_SESSION)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                .collect_to_buffer()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            self.file.write_all(ipc_data.as_ref()).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to write Vortex IPC data: {e}"
                ))
            })?;
        }

        self.file.flush().map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to flush Vortex file: {e}"
            ))
        })?;

        Ok(())
    }
}

/// Stream for reading Vortex shuffle files locally
pub struct LocalVortexShuffleStream {
    arrays: std::vec::IntoIter<ArrayRef>,
    schema: SchemaRef,
}

impl LocalVortexShuffleStream {
    /// Create a new stream from a Vortex file path
    pub fn try_new(
        path: &str,
        schema: SchemaRef,
    ) -> std::result::Result<Self, BallistaError> {
        let file = File::open(path).map_err(|e| {
            BallistaError::General(format!(
                "Failed to open Vortex partition file at {path}: {e:?}"
            ))
        })?;

        let mut buf_reader = BufReader::new(file);
        let mut data = Vec::new();
        buf_reader.read_to_end(&mut data).map_err(|e| {
            BallistaError::General(format!("Failed to read Vortex file at {path}: {e:?}"))
        })?;

        // Create default session with all canonical encodings
        let session = &*LEGACY_SESSION;

        // Read IPC data
        let cursor = Cursor::new(data);
        let reader = SyncIPCReader::try_new(cursor, session).map_err(|e| {
            BallistaError::General(format!(
                "Failed to create Vortex IPC reader at {path}: {e:?}"
            ))
        })?;

        let arrays: Vec<ArrayRef> = reader
            .map(|r| {
                r.map_err(|e| {
                    BallistaError::General(format!("Failed to read array: {e:?}"))
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(Self {
            arrays: arrays.into_iter(),
            schema,
        })
    }
}

impl Stream for LocalVortexShuffleStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.arrays.next() {
            Some(array) => {
                // Convert Vortex array back to Arrow
                let arrow_array = array.into_arrow_preferred().map_err(|e| {
                    datafusion::error::DataFusionError::External(Box::new(e))
                })?;

                // The arrow_array should be a StructArray since we converted from RecordBatch
                let struct_array = arrow_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StructArray>(
                );

                match struct_array {
                    Some(sa) => {
                        let batch = RecordBatch::from(sa);
                        Poll::Ready(Some(Ok(batch)))
                    }
                    None => Poll::Ready(Some(Err(
                        datafusion::error::DataFusionError::Internal(
                            "Expected StructArray from Vortex".to_string(),
                        ),
                    ))),
                }
            }
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for LocalVortexShuffleStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Write a stream to disk in Vortex IPC format
pub async fn write_stream_to_disk_vortex(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
    path: &str,
    disk_write_metric: &datafusion::physical_plan::metrics::Time,
) -> std::result::Result<PartitionStats, BallistaError> {
    use futures::StreamExt;

    let file = File::create(path).map_err(|e| {
        log::error!("Failed to create Vortex partition file at {path}: {e:?}");
        BallistaError::IoError(e)
    })?;

    let mut num_rows = 0;
    let mut num_batches = 0;
    let mut num_bytes = 0;
    let mut arrays: Vec<ArrayRef> = Vec::new();

    while let Some(result) = stream.next().await {
        let batch = result?;

        let batch_size_bytes: usize = batch.get_array_memory_size();
        num_batches += 1;
        num_rows += batch.num_rows();
        num_bytes += batch_size_bytes;

        // Convert Arrow RecordBatch to Vortex Array
        let vortex_array = ArrayRef::from_arrow(&batch, false).map_err(|e| {
            BallistaError::General(format!("Failed to convert to Vortex: {e}"))
        })?;
        arrays.push(vortex_array);
    }

    // Write all arrays using IPC format
    let timer = disk_write_metric.timer();
    let mut writer = BufWriter::new(file);

    if !arrays.is_empty() {
        // Get the dtype from the first array
        let dtype = arrays[0].dtype().clone();

        // Create an ArrayIterator from the buffer
        let iter = arrays.into_iter().map(|a| Ok(a) as VortexResult<ArrayRef>);
        let array_iter = ArrayIteratorAdapter::new(dtype, iter);

        // Convert to IPC bytes
        let ipc_data = array_iter
            .into_ipc(&*LEGACY_SESSION)
            .map_err(|e| {
                BallistaError::General(format!("Failed to create Vortex IPC: {e}"))
            })?
            .collect_to_buffer()
            .map_err(|e| {
                BallistaError::General(format!("Failed to write Vortex IPC: {e}"))
            })?;

        writer.write_all(ipc_data.as_ref()).map_err(|e| {
            BallistaError::General(format!("Failed to write to file: {e}"))
        })?;
    }

    writer.flush().map_err(|e| {
        BallistaError::General(format!("Failed to flush Vortex file: {e}"))
    })?;
    timer.done();

    debug!(
        "Wrote Vortex shuffle file to {}: {} rows, {} batches, {} bytes",
        path, num_rows, num_batches, num_bytes
    );

    Ok(PartitionStats::new(
        Some(num_rows as u64),
        Some(num_batches),
        Some(num_bytes as u64),
    ))
}

/// Get the file extension for Vortex files
pub fn vortex_file_extension() -> &'static str {
    "vortex"
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
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

    #[tokio::test]
    async fn test_vortex_write_read_roundtrip() {
        let work_dir = TempDir::new().unwrap();
        let path = work_dir.path().join("test.vortex");

        let batch = create_test_batch();
        let schema = batch.schema();

        // Write
        {
            let mut writer =
                VortexWriteTracker::try_new(path.clone(), schema.clone()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Read
        {
            let stream =
                LocalVortexShuffleStream::try_new(path.to_str().unwrap(), schema)
                    .unwrap();
            let mut pinned = Box::pin(stream);

            use futures::StreamExt;
            let result = pinned.next().await.unwrap().unwrap();
            assert_eq!(result.num_rows(), 3);
            assert_eq!(result.num_columns(), 2);
        }
    }
}
