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

//! `EXPLAIN ANALYZE` for distributed Ballista jobs.
//!
//! On the client side, `BallistaQueryPlanner` strips `LogicalPlan::Analyze`
//! and runs the inner plan as a regular distributed job wrapped in
//! [`DistributedExplainAnalyzeExec`]. After the child stream drains (i.e. the
//! distributed job has succeeded), this exec calls the new
//! `SchedulerGrpc::GetJobMetrics` RPC and renders per-stage / per-operator
//! metrics into a single-row `RecordBatch` matching DataFusion's `Analyze`
//! output schema.

use crate::execution_plans::DistributedQueryExec;
use crate::extension::SessionConfigExt;
use crate::serde::protobuf::{
    self, GetJobMetricsParams, GetJobMetricsResult,
    scheduler_grpc_client::SchedulerGrpcClient,
};
use crate::utils::{GrpcClientConfig, create_grpc_client_endpoint};
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_proto::logical_plan::AsLogicalPlan;
use futures::StreamExt;
use std::any::Any;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::sync::Arc;

/// Wrapper around [`DistributedQueryExec`] that fetches stage metrics from
/// the scheduler and renders them after the inner stream completes.
#[derive(Debug, Clone)]
pub struct DistributedExplainAnalyzeExec<T: 'static + AsLogicalPlan> {
    child: Arc<dyn ExecutionPlan>,
    scheduler_url: String,
    schema: SchemaRef,
    properties: PlanProperties,
    phantom: PhantomData<T>,
}

impl<T: 'static + AsLogicalPlan> DistributedExplainAnalyzeExec<T> {
    /// Creates a new explain-analyze wrapper around a `DistributedQueryExec`.
    pub fn new(
        child: Arc<DistributedQueryExec<T>>,
        scheduler_url: String,
        schema: SchemaRef,
    ) -> Self {
        let properties = Self::compute_properties(Arc::clone(&schema));
        Self {
            child,
            scheduler_url,
            schema,
            properties,
            phantom: PhantomData,
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl<T: 'static + AsLogicalPlan> DisplayAs for DistributedExplainAnalyzeExec<T> {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DistributedExplainAnalyzeExec: scheduler_url={}",
                    self.scheduler_url
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "scheduler_url={}", self.scheduler_url)
            }
        }
    }
}

impl<T: 'static + AsLogicalPlan> ExecutionPlan for DistributedExplainAnalyzeExec<T> {
    fn name(&self) -> &str {
        "DistributedExplainAnalyzeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!(
                "DistributedExplainAnalyzeExec expected one child, got {}",
                children.len()
            );
        }

        let child = children.pop().unwrap();
        if child
            .as_any()
            .downcast_ref::<DistributedQueryExec<T>>()
            .is_some()
        {
            return Ok(Arc::new(Self {
                child,
                scheduler_url: self.scheduler_url.clone(),
                schema: Arc::clone(&self.schema),
                properties: Self::compute_properties(Arc::clone(&self.schema)),
                phantom: PhantomData,
            }));
        }

        internal_err!(
            "DistributedExplainAnalyzeExec requires a DistributedQueryExec child"
        )
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "DistributedExplainAnalyzeExec only supports partition 0, got {}",
                partition
            );
        }

        let child = Arc::clone(&self.child);
        let scheduler_url = self.scheduler_url.clone();
        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&self.schema);
        let session_config = ctx.session_config().clone();

        let stream = futures::stream::once(async move {
            // Drain the child stream so the distributed job actually runs and
            // its stages publish metrics back to the scheduler.
            let mut child_stream = child.execute(partition, ctx)?;
            while let Some(batch) = child_stream.next().await {
                batch?;
            }

            let job_id = child
                .as_any()
                .downcast_ref::<DistributedQueryExec<T>>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "DistributedExplainAnalyzeExec requires a DistributedQueryExec child"
                            .into(),
                    )
                })?
                .job_id()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Distributed query completed without exposing a job_id".into(),
                    )
                })?;
            let job_metrics =
                fetch_job_metrics(&scheduler_url, &job_id, session_config).await?;
            format_metrics_as_record_batch(&job_metrics, schema)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }
}

async fn fetch_job_metrics(
    scheduler_url: &str,
    job_id: &str,
    session_config: datafusion::prelude::SessionConfig,
) -> Result<GetJobMetricsResult> {
    let grpc_interceptor = session_config.ballista_grpc_interceptor();
    let customize_endpoint =
        session_config.ballista_override_create_grpc_client_endpoint();
    let config = session_config.ballista_config();
    let grpc_config = GrpcClientConfig::from(&config);

    let mut endpoint =
        create_grpc_client_endpoint(scheduler_url.to_string(), Some(&grpc_config))
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

    if let Some(ref customize) = customize_endpoint {
        endpoint = customize
            .configure_endpoint(endpoint)
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
    }

    let connection = endpoint
        .connect()
        .await
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

    let max_message_size = config.default_grpc_client_max_message_size();
    let mut scheduler = SchedulerGrpcClient::with_interceptor(
        connection,
        grpc_interceptor.as_ref().clone(),
    )
    .max_encoding_message_size(max_message_size)
    .max_decoding_message_size(max_message_size);

    scheduler
        .get_job_metrics(GetJobMetricsParams {
            job_id: job_id.to_string(),
        })
        .await
        .map(|response| response.into_inner())
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
}

fn format_metrics_as_record_batch(
    job_metrics: &GetJobMetricsResult,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let plan = job_metrics
        .stages
        .iter()
        .map(|stage| {
            let mut stage_plan = Vec::with_capacity(stage.operators.len() + 1);
            stage_plan.push(format!(
                "=========SuccessfulStage[stage_id={}, partitions={}]=========",
                stage.stage_id, stage.partitions
            ));

            for operator in &stage.operators {
                let metrics_set: datafusion::physical_plan::metrics::MetricsSet =
                    protobuf::OperatorMetricsSet {
                        metrics: operator.metrics.clone(),
                    }
                    .try_into()
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
                let metrics = metrics_set
                    .aggregate_by_name()
                    .sorted_for_display()
                    .timestamps_removed()
                    .to_string();
                let indent = "  ".repeat(operator.depth as usize);
                stage_plan.push(format!(
                    "{indent}{}, metrics=[{metrics}]",
                    operator.operator_desc
                ));
            }

            Ok(stage_plan.join("\n"))
        })
        .collect::<Result<Vec<_>>>()?
        .join("\n\n");

    let mut type_builder = StringBuilder::with_capacity(1, "Plan with Metrics".len());
    let mut plan_builder = StringBuilder::with_capacity(1, plan.len());

    type_builder.append_value("Plan with Metrics");
    plan_builder.append_value(plan);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(type_builder.finish()),
            Arc::new(plan_builder.finish()),
        ],
    )
    .map_err(DataFusionError::from)
}

#[cfg(test)]
mod tests {
    use super::format_metrics_as_record_batch;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    use crate::serde::protobuf::{
        GetJobMetricsResult, JobStageMetrics, OperatorMetric, OperatorWithMetrics,
        operator_metric,
    };

    #[test]
    fn test_format_metrics_as_record_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]));
        let response = GetJobMetricsResult {
            stages: vec![
                JobStageMetrics {
                    stage_id: 0,
                    partitions: 1,
                    operators: vec![OperatorWithMetrics {
                        depth: 0,
                        operator_type: "ProjectionExec".to_string(),
                        operator_desc: "ProjectionExec: expr=[a]".to_string(),
                        metrics: vec![
                            OperatorMetric {
                                metric: Some(operator_metric::Metric::OutputRows(4)),
                            },
                            OperatorMetric {
                                metric: Some(operator_metric::Metric::ElapseTime(
                                    15_000_000,
                                )),
                            },
                        ],
                    }],
                },
                JobStageMetrics {
                    stage_id: 1,
                    partitions: 2,
                    operators: vec![OperatorWithMetrics {
                        depth: 0,
                        operator_type: "FilterExec".to_string(),
                        operator_desc: "FilterExec: a@0 > 1".to_string(),
                        metrics: vec![OperatorMetric {
                            metric: Some(operator_metric::Metric::OutputRows(2)),
                        }],
                    }],
                },
            ],
        };

        let batch = format_metrics_as_record_batch(&response, schema).unwrap();

        let plan_type = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let plan = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(plan_type.value(0), "Plan with Metrics");
        let expected = [
            "=========SuccessfulStage[stage_id=0, partitions=1]=========",
            "ProjectionExec: expr=[a], metrics=[output_rows=4, elapsed_compute=15.00ms]",
            "",
            "=========SuccessfulStage[stage_id=1, partitions=2]=========",
            "FilterExec: a@0 > 1, metrics=[output_rows=2]",
        ]
        .join("\n");
        assert_eq!(plan.value(0), expected);
    }
}
