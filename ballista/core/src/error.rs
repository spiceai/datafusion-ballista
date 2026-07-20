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

//! Ballista error types

use std::{
    error::Error,
    fmt::{Display, Formatter},
    io, result,
    sync::Arc,
};

use crate::serde::protobuf::failed_task::FailedReason;
use crate::serde::protobuf::{ExecutionError, FailedTask, FetchPartitionError, IoError};
use datafusion::error::DataFusionError;
use datafusion::{arrow::error::ArrowError, sql::sqlparser::parser};
use futures::future::Aborted;

/// Result type alias for Ballista operations.
pub type Result<T> = result::Result<T, BallistaError>;

/// Ballista error types for distributed query execution.
#[derive(Debug)]
pub enum BallistaError {
    /// Feature is not yet implemented.
    NotImplemented(String),
    /// General error with a descriptive message.
    General(String),
    /// Internal error indicating a bug or unexpected state.
    Internal(String),
    /// Configuration error with invalid settings.
    Configuration(String),
    /// Error from Arrow operations.
    ArrowError(Box<ArrowError>),
    /// Error from DataFusion operations.
    DataFusionError(Box<DataFusionError>),
    /// SQL parsing error.
    SqlError(parser::ParserError),
    /// I/O operation error.
    IoError(io::Error),
    /// gRPC transport error.
    TonicError(tonic::transport::Error),
    /// gRPC status error.
    GrpcError(Box<tonic::Status>),
    /// gRPC connection failure.
    GrpcConnectionError(String),
    /// Tokio task join error.
    TokioError(tokio::task::JoinError),
    /// gRPC action error.
    GrpcActionError(String),
    /// Shuffle fetch failed: (executor_id, map_stage_id, map_partition_id, message).
    FetchFailed(String, usize, usize, String),
    /// Operation was cancelled.
    Cancelled,
}

#[allow(clippy::from_over_into)]
impl<T> Into<Result<T>> for BallistaError {
    fn into(self) -> Result<T> {
        Err(self)
    }
}

/// Creates a general Ballista error from a string message.
pub fn ballista_error(message: &str) -> BallistaError {
    BallistaError::General(message.to_owned())
}

impl From<String> for BallistaError {
    fn from(e: String) -> Self {
        BallistaError::General(e)
    }
}

impl From<ArrowError> for BallistaError {
    fn from(e: ArrowError) -> Self {
        match e {
            ArrowError::ExternalError(e)
                if e.downcast_ref::<BallistaError>().is_some() =>
            {
                *e.downcast::<BallistaError>().unwrap()
            }
            ArrowError::ExternalError(e)
                if e.downcast_ref::<DataFusionError>().is_some() =>
            {
                BallistaError::DataFusionError(Box::new(
                    *e.downcast::<DataFusionError>().unwrap(),
                ))
            }
            other => BallistaError::ArrowError(Box::new(other)),
        }
    }
}

impl From<parser::ParserError> for BallistaError {
    fn from(e: parser::ParserError) -> Self {
        BallistaError::SqlError(e)
    }
}

impl From<DataFusionError> for BallistaError {
    fn from(e: DataFusionError) -> Self {
        // BallistaError::FetchFailed must reach the top level so FailedTask::from
        // routes it to FailedReason::FetchPartitionError (the scheduler's
        // map-stage rerun path). When a stream is shared between consumers,
        // DataFusion wraps the underlying error as
        //   DataFusionError::Shared(Arc(DataFusionError::ArrowError(
        //     ArrowError::ExternalError(BallistaError::FetchFailed(...)))))
        // and similar wrappers appear for Context, Diagnostic, Collection, and
        // External. Drill through them so the inner BallistaError surfaces.
        match e {
            DataFusionError::ArrowError(e, _) => Self::from(*e),
            DataFusionError::External(e) => match e.downcast::<BallistaError>() {
                Ok(b) => *b,
                Err(e) => match e.downcast::<DataFusionError>() {
                    Ok(d) => Self::from(*d),
                    Err(e) => BallistaError::DataFusionError(Box::new(
                        DataFusionError::External(e),
                    )),
                },
            },
            DataFusionError::Context(_, inner) => Self::from(*inner),
            DataFusionError::Diagnostic(_, inner) => Self::from(*inner),
            DataFusionError::Collection(mut errs) if !errs.is_empty() => {
                Self::from(errs.swap_remove(0))
            }
            DataFusionError::Shared(arc) => match Arc::try_unwrap(arc) {
                Ok(inner) => Self::from(inner),
                Err(arc) => find_fetch_failed(arc.as_ref()).unwrap_or_else(|| {
                    BallistaError::DataFusionError(Box::new(DataFusionError::Shared(arc)))
                }),
            },
            _ => BallistaError::DataFusionError(Box::new(e)),
        }
    }
}

/// Walk a borrowed [`DataFusionError`] chain looking for a buried
/// [`BallistaError::FetchFailed`], reconstructing it from its cloneable fields.
fn find_fetch_failed(e: &DataFusionError) -> Option<BallistaError> {
    match e {
        DataFusionError::ArrowError(arrow, _) => find_fetch_failed_in_arrow(arrow),
        DataFusionError::External(err) => find_fetch_failed_in_dyn(err.as_ref()),
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            find_fetch_failed(inner)
        }
        DataFusionError::Collection(errs) => errs.iter().find_map(find_fetch_failed),
        DataFusionError::Shared(arc) => find_fetch_failed(arc.as_ref()),
        _ => None,
    }
}

fn find_fetch_failed_in_arrow(e: &ArrowError) -> Option<BallistaError> {
    match e {
        ArrowError::ExternalError(err) => find_fetch_failed_in_dyn(err.as_ref()),
        _ => None,
    }
}

fn find_fetch_failed_in_dyn(err: &(dyn Error + 'static)) -> Option<BallistaError> {
    if let Some(BallistaError::FetchFailed(executor_id, map_stage, map_partition, desc)) =
        err.downcast_ref::<BallistaError>()
    {
        return Some(BallistaError::FetchFailed(
            executor_id.clone(),
            *map_stage,
            *map_partition,
            desc.clone(),
        ));
    }
    if let Some(df) = err.downcast_ref::<DataFusionError>() {
        return find_fetch_failed(df);
    }
    if let Some(arrow) = err.downcast_ref::<ArrowError>() {
        return find_fetch_failed_in_arrow(arrow);
    }
    None
}

impl From<io::Error> for BallistaError {
    fn from(e: io::Error) -> Self {
        BallistaError::IoError(e)
    }
}

impl From<tonic::transport::Error> for BallistaError {
    fn from(e: tonic::transport::Error) -> Self {
        BallistaError::TonicError(e)
    }
}

impl From<tonic::Status> for BallistaError {
    fn from(e: tonic::Status) -> Self {
        BallistaError::GrpcError(Box::new(e))
    }
}

impl From<tokio::task::JoinError> for BallistaError {
    fn from(e: tokio::task::JoinError) -> Self {
        BallistaError::TokioError(e)
    }
}

impl From<datafusion_proto_common::from_proto::Error> for BallistaError {
    fn from(e: datafusion_proto_common::from_proto::Error) -> Self {
        BallistaError::General(e.to_string())
    }
}

impl From<datafusion_proto_common::to_proto::Error> for BallistaError {
    fn from(e: datafusion_proto_common::to_proto::Error) -> Self {
        BallistaError::General(e.to_string())
    }
}

impl From<futures::future::Aborted> for BallistaError {
    fn from(_: Aborted) -> Self {
        BallistaError::Cancelled
    }
}

impl Display for BallistaError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            BallistaError::NotImplemented(desc) => {
                write!(f, "Not implemented: {desc}")
            }
            BallistaError::General(desc) => write!(f, "General error: {desc}"),
            BallistaError::ArrowError(desc) => write!(f, "Arrow error: {desc}"),
            BallistaError::DataFusionError(desc) => {
                write!(f, "DataFusion error: {desc}")
            }
            BallistaError::SqlError(desc) => write!(f, "SQL error: {desc}"),
            BallistaError::IoError(desc) => write!(f, "IO error: {desc}"),
            BallistaError::TonicError(desc) => write!(f, "Tonic error: {desc}"),
            BallistaError::GrpcError(desc) => write!(f, "Grpc error: {desc}"),
            BallistaError::GrpcConnectionError(desc) => {
                write!(f, "Grpc connection error: {desc}")
            }
            BallistaError::Internal(desc) => {
                write!(f, "Internal Ballista error: {desc}")
            }
            BallistaError::TokioError(desc) => write!(f, "Tokio join error: {desc}"),
            BallistaError::GrpcActionError(desc) => {
                write!(f, "Grpc Execute Action error: {desc}")
            }
            BallistaError::FetchFailed(executor_id, map_stage, map_partition, desc) => {
                write!(
                    f,
                    "Shuffle fetch partition error from Executor {executor_id}, map_stage {map_stage}, \
                map_partition {map_partition}, error desc: {desc}"
                )
            }
            BallistaError::Cancelled => write!(f, "Task cancelled"),
            BallistaError::Configuration(desc) => {
                write!(f, "Configuration error: {desc}")
            }
        }
    }
}

impl From<BallistaError> for FailedTask {
    fn from(e: BallistaError) -> Self {
        match e {
            BallistaError::FetchFailed(
                executor_id,
                map_stage_id,
                map_partition_id,
                desc,
            ) => {
                FailedTask {
                    error: desc,
                    // fetch partition error is considered to be non-retryable
                    retryable: false,
                    count_to_failures: false,
                    failed_reason: Some(FailedReason::FetchPartitionError(
                        FetchPartitionError {
                            executor_id,
                            map_stage_id: map_stage_id as u32,
                            map_partition_id: map_partition_id as u32,
                        },
                    )),
                }
            }
            BallistaError::IoError(io) => {
                FailedTask {
                    error: format!("Task failed due to Ballista IO error: {io:?}"),
                    // IO error is considered to be temporary and retryable
                    retryable: true,
                    count_to_failures: true,
                    failed_reason: Some(FailedReason::IoError(IoError {})),
                }
            }
            BallistaError::DataFusionError(e)
                if matches!(*e, DataFusionError::IoError(_)) =>
            {
                FailedTask {
                    error: format!("Task failed due to DataFusion IO error: {e:?}"),
                    // IO error is considered to be temporary and retryable
                    retryable: true,
                    count_to_failures: true,
                    failed_reason: Some(FailedReason::IoError(IoError {})),
                }
            }
            other => FailedTask {
                error: format!("Task failed due to runtime execution error: {other:?}"),
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(FailedReason::ExecutionError(ExecutionError {})),
            },
        }
    }
}

impl Error for BallistaError {}
