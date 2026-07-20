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

use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
//use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, Metric};
use datafusion::prelude::SessionConfig;
use log::{debug, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::{ShuffleWriterExec, SortShuffleWriterExec};
use ballista_core::serde::protobuf::failed_task::FailedReason;
use ballista_core::serde::protobuf::{
    FailedTask, GraphStageInput, OperatorMetricsSet, ResultLost, SuccessfulTask, TaskKilled,
    TaskStatus, task_info,
};
use ballista_core::serde::protobuf::{RunningTask, task_status};
use ballista_core::serde::scheduler::PartitionLocation;
use ballista_core::serde::{BallistaCodec, protobuf};
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;

use crate::display::DisplayableBallistaExecutionPlan;

/// A stage in the ExecutionGraph representing a set of tasks that can be executed concurrently.
///
/// Each stage contains one task per partition. The stage progresses through a state machine:
///
/// ```text
/// UnResolvedStage           FailedStage
///       ↓            ↙           ↑
///  ResolvedStage     →     RunningStage
///                                ↓
///                         SuccessfulStage
/// ```
///
/// - `UnResolved`: Input stages are not yet complete
/// - `Resolved`: All inputs are ready, stage can be scheduled
/// - `Running`: Tasks are being executed
/// - `Successful`: All tasks completed successfully
/// - `Failed`: Stage execution failed
#[derive(Clone)]
pub enum ExecutionStage {
    /// Stage whose input stages are not all completed.
    UnResolved(UnresolvedStage),
    /// Stage with all inputs ready, waiting to be scheduled.
    Resolved(ResolvedStage),
    /// Stage with tasks currently being executed.
    Running(RunningStage),
    /// Stage that completed all tasks successfully.
    Successful(SuccessfulStage),
    /// Stage that failed during execution.
    Failed(FailedStage),
}

impl Debug for ExecutionStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionStage::UnResolved(unresolved_stage) => unresolved_stage.fmt(f),
            ExecutionStage::Resolved(resolved_stage) => resolved_stage.fmt(f),
            ExecutionStage::Running(running_stage) => running_stage.fmt(f),
            ExecutionStage::Successful(successful_stage) => successful_stage.fmt(f),
            ExecutionStage::Failed(failed_stage) => failed_stage.fmt(f),
        }
    }
}

impl ExecutionStage {
    /// Get the name of the variant
    pub fn variant_name(&self) -> &str {
        match self {
            ExecutionStage::UnResolved(_) => "Unresolved",
            ExecutionStage::Resolved(_) => "Resolved",
            ExecutionStage::Running(_) => "Running",
            ExecutionStage::Successful(_) => "Successful",
            ExecutionStage::Failed(_) => "Failed",
        }
    }

    /// Get the query plan for this query stage
    pub fn plan(&self) -> &dyn ExecutionPlan {
        match self {
            ExecutionStage::UnResolved(stage) => stage.plan.as_ref(),
            ExecutionStage::Resolved(stage) => stage.plan.as_ref(),
            ExecutionStage::Running(stage) => stage.plan.as_ref(),
            ExecutionStage::Successful(stage) => stage.plan.as_ref(),
            ExecutionStage::Failed(stage) => stage.plan.as_ref(),
        }
    }
}

/// For a stage whose input stages are not all completed, we say it's a unresolved stage
#[derive(Clone)]
pub struct UnresolvedStage {
    /// Stage ID
    pub stage_id: usize,
    /// Stage Attempt number
    pub stage_attempt_num: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    /// This stage can only be resolved an executed once all child stages are completed.
    pub inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// Record last attempt's failure reasons to avoid duplicate resubmits
    pub last_attempt_failure_reasons: HashSet<String>,
    /// [SessionConfig] used for this stage
    pub session_config: Arc<SessionConfig>,
}

/// For a stage, if it has no inputs or all of its input stages are completed,
/// then we call it as a resolved stage
#[derive(Clone)]
pub struct ResolvedStage {
    /// Stage ID
    pub stage_id: usize,
    /// Stage Attempt number
    pub stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub partitions: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// Record last attempt's failure reasons to avoid duplicate resubmits
    pub last_attempt_failure_reasons: HashSet<String>,
    /// [SessionConfig] used for this stage
    pub session_config: Arc<SessionConfig>,
}

/// Different from the resolved stage, a running stage will
/// 1. save the execution plan as encoded one to avoid serialization cost for creating task definition
/// 2. manage the task statuses
/// 3. manage the stage-level combined metrics
///    Running stages will only be maintained in memory and will not saved to the backend storage
#[derive(Clone)]
pub struct RunningStage {
    /// Stage ID
    pub stage_id: usize,
    /// Stage Attempt number
    pub stage_attempt_num: usize,
    /// Stage activation time (when was stage become running) in millis
    pub stage_running_time: u128,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub partitions: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// TaskInfo of each already scheduled task. If info is None, the partition has not yet been scheduled.
    /// The index of the Vec is the task's partition id
    pub task_infos: Vec<Option<TaskInfo>>,
    /// Track the number of failures for each partition's task attempts.
    /// The index of the Vec is the task's partition id.
    pub task_failure_numbers: Vec<usize>,
    /// Combined metrics of the already finished tasks in the stage, If it is None, no task is finished yet.
    pub stage_metrics: Option<Vec<MetricsSet>>,
    /// [SessionConfig] used for this stage
    pub session_config: Arc<SessionConfig>,
}

/// If a stage finishes successfully, its task statuses and metrics will be finalized
#[derive(Clone)]
pub struct SuccessfulStage {
    /// Stage ID
    pub stage_id: usize,
    /// Stage Attempt number
    pub stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub partitions: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// TaskInfo of each already successful task.
    /// The index of the Vec is the task's partition id
    pub task_infos: Vec<TaskInfo>,
    /// Combined metrics of the already finished tasks in the stage.
    pub stage_metrics: Vec<MetricsSet>,
    /// [SessionConfig] used for this stage
    pub session_config: Arc<SessionConfig>,
    /// Encoded form of `plan`, computed lazily on first serialization and
    /// reused on subsequent ones. A successful stage's plan is immutable, so
    /// re-encoding it on every graph save is pure overhead; the shared
    /// [`OnceLock`] survives clones, letting later saves skip the work.
    pub encoded_plan: Arc<OnceLock<Vec<u8>>>,
}

/// If a stage fails, it will be with an error message
#[derive(Clone)]
pub struct FailedStage {
    /// Stage ID
    pub stage_id: usize,
    /// Stage Attempt number
    pub stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub partitions: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    #[allow(dead_code)] // not used at the moment, will be used later
    pub output_links: Vec<usize>,
    /// `ExecutionPlan` for this stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// TaskInfo of each already scheduled tasks. If info is None, the partition has not yet been scheduled
    /// The index of the Vec is the task's partition id
    pub task_infos: Vec<Option<TaskInfo>>,
    /// Combined metrics of the already finished tasks in the stage, If it is None, no task is finished yet.
    #[allow(dead_code)] // not used at the moment, will be used later
    pub stage_metrics: Option<Vec<MetricsSet>>,
    /// Error message
    pub error_message: String,
}

/// Information about a task's execution lifecycle and current status.
#[derive(Clone)]
#[allow(dead_code)] // we may use the fields later
pub struct TaskInfo {
    /// Unique task identifier within the execution graph.
    pub task_id: usize,
    /// ID of the executor that ran (or is running) this task.
    ///
    /// Carried at the top level so it survives terminal status transitions
    /// to `Failed` — `FailedTask` does not embed an `executor_id`, so without
    /// this field the scheduler would lose the executor mapping for any
    /// failed partition. Populated at task launch and preserved through
    /// `update_task_info`.
    pub executor_id: String,
    /// Timestamp when the task was scheduled (in milliseconds since epoch).
    pub scheduled_time: u128,
    /// Timestamp when the task was launched on an executor (in milliseconds since epoch).
    pub launch_time: u128,
    /// Timestamp when actual execution started (in milliseconds since epoch).
    pub start_exec_time: u128,
    /// Timestamp when execution finished (in milliseconds since epoch).
    pub end_exec_time: u128,
    /// Timestamp when the task result was received (in milliseconds since epoch).
    pub finish_time: u128,
    /// Current status of the task (Running, Successful, Failed).
    pub task_status: task_status::Status,
}

impl UnresolvedStage {
    /// Creates a new unresolved stage with the given child stage dependencies.
    pub fn new(
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_links: Vec<usize>,
        child_stage_ids: Vec<usize>,
        session_config: Arc<SessionConfig>,
    ) -> Self {
        let mut inputs: HashMap<usize, StageOutput> = HashMap::new();
        for input_stage_id in child_stage_ids {
            inputs.insert(input_stage_id, StageOutput::new());
        }

        Self {
            stage_id,
            stage_attempt_num: 0,
            output_links,
            inputs,
            plan,
            last_attempt_failure_reasons: Default::default(),
            session_config,
        }
    }

    /// Creates a new unresolved stage with pre-populated inputs (used for stage rollback).
    pub fn new_with_inputs(
        stage_id: usize,
        stage_attempt_num: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_links: Vec<usize>,
        inputs: HashMap<usize, StageOutput>,
        last_attempt_failure_reasons: HashSet<String>,
        session_config: Arc<SessionConfig>,
    ) -> Self {
        Self {
            stage_id,
            stage_attempt_num,
            output_links,
            inputs,
            plan,
            last_attempt_failure_reasons,
            session_config,
        }
    }

    /// Add input partitions published from an input stage.
    pub fn add_input_partitions(
        &mut self,
        stage_id: usize,
        locations: Vec<PartitionLocation>,
    ) -> Result<()> {
        if let Some(stage_inputs) = self.inputs.get_mut(&stage_id) {
            for partition in locations {
                stage_inputs.add_partition(partition);
            }
        } else {
            return Err(BallistaError::Internal(format!(
                "Error adding input partitions to stage {}, {} is not a valid child stage ID",
                self.stage_id, stage_id
            )));
        }

        Ok(())
    }

    /// Remove input partitions from an input stage on a given executor.
    /// Return the HashSet of removed map partition ids
    pub fn remove_input_partitions(
        &mut self,
        input_stage_id: usize,
        _input_partition_id: usize,
        executor_id: &str,
    ) -> Result<HashSet<usize>> {
        if let Some(stage_output) = self.inputs.get_mut(&input_stage_id) {
            let mut bad_map_partitions = HashSet::new();
            stage_output
                .partition_locations
                .iter_mut()
                .for_each(|(_partition, locs)| {
                    locs.iter().for_each(|loc| {
                        if loc.executor_meta.id == executor_id {
                            bad_map_partitions.insert(loc.map_partition_id);
                        }
                    });

                    locs.retain(|loc| loc.executor_meta.id != executor_id);
                });
            stage_output.complete = false;
            Ok(bad_map_partitions)
        } else {
            Err(BallistaError::Internal(format!(
                "Error remove input partition for Stage {}, {} is not a valid child stage ID",
                self.stage_id, input_stage_id
            )))
        }
    }

    /// Marks the input stage ID as complete.
    pub fn complete_input(&mut self, stage_id: usize) {
        if let Some(input) = self.inputs.get_mut(&stage_id) {
            input.complete = true;
        }
    }

    /// Returns true if all inputs are complete and we can resolve all
    /// UnresolvedShuffleExec operators to ShuffleReadExec
    pub fn resolvable(&self) -> bool {
        self.inputs.iter().all(|(_, input)| input.is_complete())
    }

    /// Change to the resolved state
    pub fn to_resolved(&self, options: &ConfigOptions) -> Result<ResolvedStage> {
        let input_locations = self
            .inputs
            .iter()
            .map(|(stage, input)| (*stage, input.partition_locations.clone()))
            .collect();
        let plan = crate::planner::remove_unresolved_shuffles(
            self.plan.clone(),
            &input_locations,
        )?;

        // ballista specific JoinSelection, as datafusion rule can't be used here.
        // Datafusion JoinSelection may produce plans which need change of partitions
        // in order to be valid.
        //
        // we should consider changing ballista core to support adding new stages
        // if plan changes.

        let optimize_join =
            crate::physical_optimizer::join_selection::JoinSelection::new();
        let plan = optimize_join.optimize(plan, options)?;

        let optimize_aggregate = AggregateStatistics::new();
        let plan = optimize_aggregate.optimize(plan, options)?;

        Ok(ResolvedStage::new(
            self.stage_id,
            self.stage_attempt_num,
            plan,
            self.output_links.clone(),
            self.inputs.clone(),
            self.last_attempt_failure_reasons.clone(),
            self.session_config.clone(),
        ))
    }

    pub(crate) fn decode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: protobuf::UnResolvedStage,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
        session_config: Arc<SessionConfig>,
    ) -> Result<UnresolvedStage> {
        let plan = decode_plan(&stage.plan, codec, session_ctx)?;
        let inputs = decode_inputs(stage.inputs)?;

        Ok(UnresolvedStage {
            stage_id: stage.stage_id as usize,
            stage_attempt_num: stage.stage_attempt_num as usize,
            output_links: stage.output_links.into_iter().map(|l| l as usize).collect(),
            plan,
            inputs,
            last_attempt_failure_reasons: HashSet::from_iter(
                stage.last_attempt_failure_reasons,
            ),
            session_config,
        })
    }

    pub(crate) fn encode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: UnresolvedStage,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::UnResolvedStage> {
        let plan = encode_plan(stage.plan, codec)?;
        let inputs = encode_inputs(stage.inputs)?;

        Ok(protobuf::UnResolvedStage {
            stage_id: stage.stage_id as u32,
            stage_attempt_num: stage.stage_attempt_num as u32,
            output_links: stage.output_links.into_iter().map(|l| l as u32).collect(),
            inputs,
            plan,
            last_attempt_failure_reasons: Vec::from_iter(
                stage.last_attempt_failure_reasons,
            ),
        })
    }
}

impl Debug for UnresolvedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);

        write!(
            f,
            "=========UnResolvedStage[stage_id={}.{}, children={}]=========\nInputs{:?}\n{}",
            self.stage_id,
            self.stage_attempt_num,
            self.inputs.len(),
            self.inputs,
            plan
        )
    }
}

impl ResolvedStage {
    /// Creates a new resolved stage ready for task scheduling.
    pub fn new(
        stage_id: usize,
        stage_attempt_num: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_links: Vec<usize>,
        inputs: HashMap<usize, StageOutput>,
        last_attempt_failure_reasons: HashSet<String>,
        session_config: Arc<SessionConfig>,
    ) -> Self {
        let partitions = get_stage_partitions(plan.clone());

        Self {
            stage_id,
            stage_attempt_num,
            partitions,
            output_links,
            inputs,
            plan,
            last_attempt_failure_reasons,
            session_config,
        }
    }

    /// Change to the running state
    pub fn to_running(&self) -> RunningStage {
        RunningStage::new(
            self.stage_id,
            self.stage_attempt_num,
            self.plan.clone(),
            self.partitions,
            self.output_links.clone(),
            self.inputs.clone(),
            self.session_config.clone(),
        )
    }

    /// Change to the unresolved state
    pub fn to_unresolved(&self) -> Result<UnresolvedStage> {
        let new_plan = crate::planner::rollback_resolved_shuffles(self.plan.clone())?;

        let unresolved = UnresolvedStage::new_with_inputs(
            self.stage_id,
            self.stage_attempt_num,
            new_plan,
            self.output_links.clone(),
            self.inputs.clone(),
            self.last_attempt_failure_reasons.clone(),
            self.session_config.clone(),
        );
        Ok(unresolved)
    }

    pub(crate) fn decode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: protobuf::ResolvedStage,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
        session_config: Arc<SessionConfig>,
    ) -> Result<ResolvedStage> {
        let plan = decode_plan(&stage.plan, codec, session_ctx)?;
        let inputs = decode_inputs(stage.inputs)?;

        Ok(ResolvedStage {
            stage_id: stage.stage_id as usize,
            stage_attempt_num: stage.stage_attempt_num as usize,
            partitions: stage.partitions as usize,
            output_links: stage.output_links.into_iter().map(|l| l as usize).collect(),
            inputs,
            plan,
            last_attempt_failure_reasons: HashSet::from_iter(
                stage.last_attempt_failure_reasons,
            ),
            session_config,
        })
    }

    pub(crate) fn encode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: ResolvedStage,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::ResolvedStage> {
        let plan = encode_plan(stage.plan, codec)?;
        let inputs = encode_inputs(stage.inputs)?;

        Ok(protobuf::ResolvedStage {
            stage_id: stage.stage_id as u32,
            stage_attempt_num: stage.stage_attempt_num as u32,
            partitions: stage.partitions as u32,
            output_links: stage.output_links.into_iter().map(|l| l as u32).collect(),
            inputs,
            plan,
            last_attempt_failure_reasons: Vec::from_iter(
                stage.last_attempt_failure_reasons,
            ),
        })
    }
}

impl Debug for ResolvedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);

        write!(
            f,
            "=========ResolvedStage[stage_id={}.{}, partitions={}]=========\n{}",
            self.stage_id, self.stage_attempt_num, self.partitions, plan
        )
    }
}

impl RunningStage {
    /// Creates a new running stage with task tracking initialized.
    pub fn new(
        stage_id: usize,
        stage_attempt_num: usize,
        plan: Arc<dyn ExecutionPlan>,
        partitions: usize,
        output_links: Vec<usize>,
        inputs: HashMap<usize, StageOutput>,
        session_config: Arc<SessionConfig>,
    ) -> Self {
        Self {
            stage_id,
            stage_attempt_num,
            stage_running_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            partitions,
            output_links,
            inputs,
            plan,
            task_infos: vec![None; partitions],
            task_failure_numbers: vec![0; partitions],
            stage_metrics: None,
            session_config,
        }
    }

    /// Converts this running stage to a successful stage after all tasks complete.
    pub fn to_successful(&self) -> SuccessfulStage {
        let task_infos = self
            .task_infos
            .iter()
            .enumerate()
            .map(|(partition_id, info)| {
                info.clone().unwrap_or_else(|| {
                    panic!(
                        "TaskInfo for task {}.{}/{} should not be none",
                        self.stage_id, self.stage_attempt_num, partition_id
                    )
                })
            })
            .collect();
        let stage_metrics = self.stage_metrics.clone().unwrap_or_else(|| {
            warn!("The metrics for stage {} should not be none", self.stage_id);
            vec![]
        });
        SuccessfulStage {
            stage_id: self.stage_id,
            stage_attempt_num: self.stage_attempt_num,
            partitions: self.partitions,
            output_links: self.output_links.clone(),
            inputs: self.inputs.clone(),
            plan: self.plan.clone(),
            task_infos,
            stage_metrics,
            session_config: self.session_config.clone(),
            encoded_plan: Arc::new(OnceLock::new()),
        }
    }

    /// Converts this running stage to a failed stage. Still-running tasks are recorded
    /// as cancelled (`Failed(TaskKilled)`) since failing the stage cancels them.
    pub fn to_failed(&self, error_message: String) -> FailedStage {
        let task_infos = self
            .task_infos
            .iter()
            .map(|task_info| {
                task_info.as_ref().map(|info| {
                    if matches!(info.task_status, task_status::Status::Running(_)) {
                        TaskInfo {
                            task_status: task_status::Status::Failed(FailedTask {
                                error: "killed".to_string(),
                                retryable: false,
                                count_to_failures: false,
                                failed_reason: Some(FailedReason::TaskKilled(TaskKilled {})),
                            }),
                            ..info.clone()
                        }
                    } else {
                        info.clone()
                    }
                })
            })
            .collect();

        FailedStage {
            stage_id: self.stage_id,
            stage_attempt_num: self.stage_attempt_num,
            partitions: self.partitions,
            output_links: self.output_links.clone(),
            plan: self.plan.clone(),
            task_infos,
            stage_metrics: self.stage_metrics.clone(),
            error_message,
        }
    }

    /// Change to the unresolved state and bump the stage attempt number
    pub fn to_unresolved(
        &self,
        failure_reasons: HashSet<String>,
    ) -> Result<UnresolvedStage> {
        let new_plan = crate::planner::rollback_resolved_shuffles(self.plan.clone())?;

        let unresolved = UnresolvedStage::new_with_inputs(
            self.stage_id,
            self.stage_attempt_num + 1,
            new_plan,
            self.output_links.clone(),
            self.inputs.clone(),
            failure_reasons,
            self.session_config.clone(),
        );
        Ok(unresolved)
    }

    /// Change to the resolved state, bumping the stage attempt number.
    /// Running stages are never persisted as running, so they are encoded as
    /// resolved stages.
    pub fn to_resolved(&self) -> ResolvedStage {
        ResolvedStage::new(
            self.stage_id,
            self.stage_attempt_num + 1,
            self.plan.clone(),
            self.output_links.clone(),
            self.inputs.clone(),
            HashSet::new(),
            self.session_config.clone(),
        )
    }

    /// Returns `true` if all tasks for this stage are successful
    pub fn is_successful(&self) -> bool {
        self.task_infos.iter().all(|info| {
            matches!(
                info,
                Some(TaskInfo {
                    task_status: task_status::Status::Successful(_),
                    ..
                })
            )
        })
    }

    /// Returns the number of successful tasks
    pub fn successful_tasks(&self) -> usize {
        self.task_infos
            .iter()
            .filter(|info| {
                matches!(
                    info,
                    Some(TaskInfo {
                        task_status: task_status::Status::Successful(_),
                        ..
                    })
                )
            })
            .count()
    }

    /// Returns the number of scheduled tasks
    pub fn scheduled_tasks(&self) -> usize {
        self.task_infos.iter().filter(|s| s.is_some()).count()
    }

    /// Returns a vector of currently running tasks in this stage
    pub fn running_tasks(&self) -> Vec<(usize, usize, usize, String)> {
        self.task_infos
            .iter()
            .enumerate()
            .filter_map(|(partition, info)| match info {
                Some(TaskInfo {task_id,
                         task_status: task_status::Status::Running(RunningTask { executor_id }), ..}) => {
                    Some((*task_id, self.stage_id, partition, executor_id.clone()))
                }
                _ => None,
            })
            .collect()
    }

    /// Returns the number of tasks in this stage which are available for scheduling.
    /// If the stage is not yet resolved, then this will return `0`, otherwise it will
    /// return the number of tasks where the task info is not yet set.
    pub fn available_tasks(&self) -> usize {
        self.task_infos.iter().filter(|s| s.is_none()).count()
    }

    /// Update the TaskInfo for task partition
    pub fn update_task_info(&mut self, partition_id: usize, status: TaskStatus) -> bool {
        debug!("Updating TaskInfo for partition {partition_id}");
        // The task info for a partition can be `None` if the task was reset
        // (e.g. after its executor was lost / heartbeat-timed-out) before a
        // late, in-flight status update arrived from that executor. Ignore the
        // stale update instead of unwrapping: panicking here kills the scheduler
        // event-loop worker, which closes the event channel and wedges the whole
        // scheduler ("Fail to send event due to channel closed").
        let Some(task_info) = self.task_infos[partition_id].as_ref() else {
            warn!(
                "Ignoring TaskStatus update with TID {} for partition {partition_id} because no task is currently scheduled there (task was reset or not yet scheduled)",
                status.task_id
            );
            return false;
        };
        let task_id = task_info.task_id;
        if (status.task_id as usize) < task_id {
            warn!(
                "Ignore TaskStatus update with TID {} because there is more recent task attempt with TID {} running for partition {}",
                status.task_id, task_id, partition_id
            );
            return false;
        }
        let scheduled_time = task_info.scheduled_time;
        let executor_id = task_info.executor_id.clone();
        let task_status = status.status.unwrap();
        let updated_task_info = TaskInfo {
            task_id,
            executor_id,
            scheduled_time,
            launch_time: status.launch_time as u128,
            start_exec_time: status.start_exec_time as u128,
            end_exec_time: status.end_exec_time as u128,
            finish_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            task_status: task_status.clone(),
        };
        self.task_infos[partition_id] = Some(updated_task_info);

        if let task_status::Status::Failed(failed_task) = task_status {
            // if the failed task is retryable, increase the task failure count for this partition
            if failed_task.retryable {
                self.task_failure_numbers[partition_id] += 1;
            }
        } else {
            self.task_failure_numbers[partition_id] = 0;
        }
        true
    }

    /// update and combine the task metrics to the stage metrics
    pub fn update_task_metrics(
        &mut self,
        partition: usize,
        metrics: Vec<OperatorMetricsSet>,
    ) -> Result<()> {
        // For some cases, task metrics not set, especially for testings.
        if metrics.is_empty() {
            return Ok(());
        }

        let new_metrics_set = if let Some(combined_metrics) = &mut self.stage_metrics {
            if metrics.len() != combined_metrics.len() {
                return Err(BallistaError::Internal(format!(
                    "Error updating task metrics to stage {}, task metrics array size {} does not equal \
                with the stage metrics array size {} for task {}",
                    self.stage_id,
                    metrics.len(),
                    combined_metrics.len(),
                    partition
                )));
            }
            let metrics_values_array = metrics
                .into_iter()
                .map(|ms| {
                    ms.metrics
                        .into_iter()
                        .map(|m| m.try_into())
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            combined_metrics
                .iter_mut()
                .zip(metrics_values_array)
                .map(|(first, second)| {
                    Self::combine_metrics_set(first, second, partition)
                })
                .collect()
        } else {
            metrics
                .into_iter()
                .map(|ms| ms.try_into())
                .collect::<Result<Vec<_>>>()?
        };
        self.stage_metrics = Some(new_metrics_set);

        Ok(())
    }

    /// Combines metrics from a completed task into the stage's aggregate metrics.
    pub fn combine_metrics_set(
        first: &mut MetricsSet,
        second: Vec<MetricValue>,
        partition: usize,
    ) -> MetricsSet {
        for metric_value in second {
            // TODO recheck the lable logic
            let new_metric = Arc::new(Metric::new(metric_value, Some(partition)));
            first.push(new_metric);
        }
        first.aggregate_by_name()
    }

    /// Returns the number of times the task for the given partition has failed.
    pub fn task_failure_number(&self, partition_id: usize) -> usize {
        self.task_failure_numbers[partition_id]
    }

    /// Reset the task info for the given task partition. This should be called when a task failed and need to be
    /// re-scheduled.
    pub fn reset_task_info(&mut self, partition_id: usize) {
        self.task_infos[partition_id] = None;
    }

    /// Reset the running and completed tasks on a given executor
    /// Returns the number of running tasks that were reset
    pub fn reset_tasks(&mut self, executor: &str) -> usize {
        let mut reset = 0;
        for task in self.task_infos.iter_mut() {
            match task {
                Some(TaskInfo {
                    task_status: task_status::Status::Running(RunningTask { executor_id }),
                    ..
                }) if *executor == *executor_id => {
                    *task = None;
                    reset += 1;
                }
                Some(TaskInfo {
                    task_status:
                        task_status::Status::Successful(SuccessfulTask {
                            executor_id,
                            partitions: _,
                        }),
                    ..
                }) if *executor == *executor_id => {
                    *task = None;
                    reset += 1;
                }
                _ => {}
            }
        }
        reset
    }

    /// Remove input partitions from an input stage on a given executor.
    /// Return the HashSet of removed map partition ids
    pub fn remove_input_partitions(
        &mut self,
        input_stage_id: usize,
        _input_partition_id: usize,
        executor_id: &str,
    ) -> Result<HashSet<usize>> {
        if let Some(stage_output) = self.inputs.get_mut(&input_stage_id) {
            let mut bad_map_partitions = HashSet::new();
            stage_output
                .partition_locations
                .iter_mut()
                .for_each(|(_partition, locs)| {
                    locs.iter().for_each(|loc| {
                        if loc.executor_meta.id == executor_id {
                            bad_map_partitions.insert(loc.map_partition_id);
                        }
                    });

                    locs.retain(|loc| loc.executor_meta.id != executor_id);
                });
            stage_output.complete = false;
            Ok(bad_map_partitions)
        } else {
            Err(BallistaError::Internal(format!(
                "Error remove input partition for Stage {}, {} is not a valid child stage ID",
                self.stage_id, input_stage_id
            )))
        }
    }
}

impl Debug for RunningStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);

        write!(
            f,
            "=========RunningStage[stage_id={}.{}, partitions={}, successful_tasks={}, scheduled_tasks={}, available_tasks={}]=========\n{}",
            self.stage_id,
            self.stage_attempt_num,
            self.partitions,
            self.successful_tasks(),
            self.scheduled_tasks(),
            self.available_tasks(),
            plan
        )
    }
}

impl SuccessfulStage {
    /// Change to the running state and bump the stage attempt number
    pub fn to_running(&self) -> RunningStage {
        let mut task_infos: Vec<Option<TaskInfo>> = Vec::new();
        for task in self.task_infos.iter() {
            match task {
                TaskInfo {
                    task_status: task_status::Status::Successful(_),
                    ..
                } => task_infos.push(Some(task.clone())),
                _ => task_infos.push(None),
            }
        }
        let stage_metrics = if self.stage_metrics.is_empty() {
            None
        } else {
            Some(self.stage_metrics.clone())
        };
        RunningStage {
            stage_id: self.stage_id,
            stage_attempt_num: self.stage_attempt_num + 1,
            partitions: self.partitions,
            output_links: self.output_links.clone(),
            inputs: self.inputs.clone(),
            plan: self.plan.clone(),
            task_infos,
            // It is Ok to forget the previous task failure attempts
            task_failure_numbers: vec![0; self.partitions],
            stage_metrics,
            session_config: self.session_config.clone(),
            stage_running_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        }
    }

    /// Reset the successful tasks on a given executor
    /// Returns the number of running tasks that were reset
    pub fn reset_tasks(&mut self, executor: &str) -> usize {
        let mut reset = 0;
        let failure_reason = format!("Task failure due to Executor {executor} lost");
        for task in self.task_infos.iter_mut() {
            match task {
                TaskInfo {
                    task_id,
                    scheduled_time,
                    task_status:
                        task_status::Status::Successful(SuccessfulTask {
                            executor_id, ..
                        }),
                    ..
                } if *executor == *executor_id => {
                    *task = TaskInfo {
                        task_id: *task_id,
                        executor_id: executor_id.clone(),
                        scheduled_time: *scheduled_time,
                        launch_time: 0,
                        start_exec_time: 0,
                        end_exec_time: 0,
                        finish_time: 0,
                        task_status: task_status::Status::Failed(FailedTask {
                            error: failure_reason.clone(),
                            retryable: true,
                            count_to_failures: false,
                            failed_reason: Some(FailedReason::ResultLost(ResultLost {})),
                        }),
                    };
                    reset += 1;
                }
                _ => {}
            }
        }
        reset
    }

    pub(crate) fn decode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: protobuf::SuccessfulStage,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
        session_config: Arc<SessionConfig>,
    ) -> Result<SuccessfulStage> {
        let plan = decode_plan(&stage.plan, codec, session_ctx)?;
        let inputs = decode_inputs(stage.inputs)?;
        // Reconstruct by `partition_id` (rather than trusting the on-the-wire
        // order) and validate against `partitions`, returning an error instead
        // of panicking on corrupt or version-skewed persisted state.
        let stage_id = stage.stage_id;
        let partitions = stage.partitions as usize;
        let mut slots: Vec<Option<TaskInfo>> = vec![None; partitions];
        for info in stage.task_infos {
            let partition_id = info.partition_id as usize;
            if partition_id >= partitions {
                return Err(BallistaError::Internal(format!(
                    "protobuf::SuccessfulStage {stage_id} task_info partition_id {partition_id} out of range (partitions={partitions})"
                )));
            }
            if slots[partition_id].is_some() {
                return Err(BallistaError::Internal(format!(
                    "protobuf::SuccessfulStage {stage_id} has duplicate task_info for partition {partition_id}"
                )));
            }
            slots[partition_id] = Some(decode_taskinfo(info)?);
        }
        let task_infos = slots
            .into_iter()
            .enumerate()
            .map(|(partition, info)| {
                info.ok_or_else(|| {
                    BallistaError::Internal(format!(
                        "protobuf::SuccessfulStage {stage_id} is missing task_info for partition {partition}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let stage_metrics = stage
            .stage_metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(SuccessfulStage {
            stage_id: stage.stage_id as usize,
            stage_attempt_num: stage.stage_attempt_num as usize,
            partitions: stage.partitions as usize,
            output_links: stage.output_links.into_iter().map(|l| l as usize).collect(),
            inputs,
            plan,
            task_infos,
            stage_metrics,
            session_config,
            encoded_plan: Arc::new(OnceLock::new()),
        })
    }

    pub(crate) fn encode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: SuccessfulStage,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::SuccessfulStage> {
        let stage_id = stage.stage_id;
        let plan = match stage.encoded_plan.get() {
            Some(bytes) => bytes.clone(),
            None => {
                let bytes = encode_plan(stage.plan.clone(), codec)?;
                let _ = stage.encoded_plan.set(bytes.clone());
                bytes
            }
        };
        let inputs = encode_inputs(stage.inputs)?;
        let task_infos = stage
            .task_infos
            .into_iter()
            .enumerate()
            .map(|(partition, task_info)| encode_taskinfo(task_info, partition))
            .collect();
        let stage_metrics = stage
            .stage_metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(protobuf::SuccessfulStage {
            stage_id: stage_id as u32,
            stage_attempt_num: stage.stage_attempt_num as u32,
            partitions: stage.partitions as u32,
            output_links: stage.output_links.into_iter().map(|l| l as u32).collect(),
            inputs,
            plan,
            task_infos,
            stage_metrics,
        })
    }
}

impl Debug for SuccessfulStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableBallistaExecutionPlan::new(
            self.plan.as_ref(),
            &self.stage_metrics,
        )
        .indent();

        write!(
            f,
            "=========SuccessfulStage[stage_id={}.{}, partitions={}]=========\n{}",
            self.stage_id, self.stage_attempt_num, self.partitions, plan
        )
    }
}

impl FailedStage {
    /// Returns the number of successful tasks
    pub fn successful_tasks(&self) -> usize {
        self.task_infos
            .iter()
            .filter(|info| {
                matches!(
                    info,
                    Some(TaskInfo {
                        task_status: task_status::Status::Successful(_),
                        ..
                    })
                )
            })
            .count()
    }
    /// Returns the number of scheduled tasks
    pub fn scheduled_tasks(&self) -> usize {
        self.task_infos.iter().filter(|s| s.is_some()).count()
    }

    /// Returns the number of tasks in this stage which are available for scheduling.
    /// If the stage is not yet resolved, then this will return `0`, otherwise it will
    /// return the number of tasks where the task status is not yet set.
    pub fn available_tasks(&self) -> usize {
        self.task_infos.iter().filter(|s| s.is_none()).count()
    }

    pub(crate) fn decode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: protobuf::FailedStage,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
    ) -> Result<FailedStage> {
        let plan = decode_plan(&stage.plan, codec, session_ctx)?;

        let stage_id = stage.stage_id;
        let partitions = stage.partitions as usize;
        let mut task_infos: Vec<Option<TaskInfo>> = vec![None; partitions];
        for info in stage.task_infos {
            let partition_id = info.partition_id as usize;
            if partition_id >= partitions {
                return Err(BallistaError::Internal(format!(
                    "protobuf::FailedStage {stage_id} task_info partition_id {partition_id} out of range (partitions={partitions})"
                )));
            }
            task_infos[partition_id] = Some(decode_taskinfo(info)?);
        }

        let stage_metrics = if stage.stage_metrics.is_empty() {
            None
        } else {
            let ms = stage
                .stage_metrics
                .into_iter()
                .map(|m| m.try_into())
                .collect::<Result<Vec<_>>>()?;
            Some(ms)
        };

        Ok(FailedStage {
            stage_id: stage.stage_id as usize,
            stage_attempt_num: stage.stage_attempt_num as usize,
            partitions: stage.partitions as usize,
            output_links: stage.output_links.into_iter().map(|l| l as usize).collect(),
            plan,
            task_infos,
            stage_metrics,
            error_message: stage.error_message,
        })
    }

    pub(crate) fn encode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: FailedStage,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::FailedStage> {
        let stage_id = stage.stage_id;
        let plan = encode_plan(stage.plan, codec)?;

        let task_infos: Vec<protobuf::TaskInfo> = stage
            .task_infos
            .into_iter()
            .enumerate()
            .filter_map(|(partition, task_info)| {
                task_info.map(|info| encode_taskinfo(info, partition))
            })
            .collect();

        let stage_metrics = stage
            .stage_metrics
            .unwrap_or_default()
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(protobuf::FailedStage {
            stage_id: stage_id as u32,
            stage_attempt_num: stage.stage_attempt_num as u32,
            partitions: stage.partitions as u32,
            output_links: stage.output_links.into_iter().map(|l| l as u32).collect(),
            plan,
            task_infos,
            stage_metrics,
            error_message: stage.error_message,
        })
    }
}

impl Debug for FailedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);

        write!(
            f,
            "=========FailedStage[stage_id={}.{}, partitions={}, successful_tasks={}, scheduled_tasks={}, available_tasks={}, error_message={}]=========\n{}",
            self.stage_id,
            self.stage_attempt_num,
            self.partitions,
            self.successful_tasks(),
            self.scheduled_tasks(),
            self.available_tasks(),
            self.error_message,
            plan
        )
    }
}

/// Get the total number of partitions for a stage with plan.
/// Only for shuffle writers, the input partition count and the output partition count
/// will be different. Here, we should use the input partition count.
fn get_stage_partitions(plan: Arc<dyn ExecutionPlan>) -> usize {
    // Try ShuffleWriterExec first
    if let Some(shuffle_writer) = plan.downcast_ref::<ShuffleWriterExec>() {
        return shuffle_writer.input_partition_count();
    }
    // Try SortShuffleWriterExec
    if let Some(shuffle_writer) = plan.downcast_ref::<SortShuffleWriterExec>() {
        return shuffle_writer.input_partition_count();
    }
    // Fallback to output partitioning
    plan.properties().output_partitioning().partition_count()
}

/// This data structure collects the partition locations for an `ExecutionStage`.
/// Each `ExecutionStage` will hold a `StageOutput`s for each of its child stages.
/// When all tasks for the child stage are complete, it will mark the `StageOutput`
/// as complete.
#[derive(Clone, Debug, Default)]
pub struct StageOutput {
    /// Map from partition -> partition locations
    pub partition_locations: HashMap<usize, Vec<PartitionLocation>>,
    /// Flag indicating whether all tasks are complete
    pub complete: bool,
}

impl StageOutput {
    /// Creates a new empty stage output.
    pub fn new() -> Self {
        Self {
            partition_locations: HashMap::new(),
            complete: false,
        }
    }

    /// Adds a `PartitionLocation` to this stage output.
    pub fn add_partition(&mut self, partition_location: PartitionLocation) {
        if let Some(parts) = self
            .partition_locations
            .get_mut(&partition_location.partition_id.partition_id)
        {
            parts.push(partition_location)
        } else {
            self.partition_locations.insert(
                partition_location.partition_id.partition_id,
                vec![partition_location],
            );
        }
    }

    /// Returns true if all partitions for this stage output are complete.
    pub fn is_complete(&self) -> bool {
        self.complete
    }
    /// returns vector of partition locations
    /// which is compatible with ShuffleReader vector format
    pub fn partition_locations(
        mut self,
        output_partitions: usize,
    ) -> Vec<Vec<PartitionLocation>> {
        let mut partition_locations = Vec::new();
        for i in 0..output_partitions {
            let p = self.partition_locations.remove(&i).unwrap_or_default();
            partition_locations.push(p);
        }

        partition_locations
    }

    /// Returns partition locations as a vector of vectors for broadcast shuffle read.
    pub fn partition_locations_broadcast(self) -> Vec<Vec<PartitionLocation>> {
        self.partition_locations.into_values().collect()
    }
}

fn decode_plan<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    bytes: &[u8],
    codec: &BallistaCodec<T, U>,
    session_ctx: &SessionContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan_proto = U::try_decode(bytes)?;
    let task_ctx = session_ctx.task_ctx();
    let plan =
        plan_proto.try_into_physical_plan(&task_ctx, codec.physical_extension_codec())?;
    Ok(plan)
}

fn encode_plan<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    plan: Arc<dyn ExecutionPlan>,
    codec: &BallistaCodec<T, U>,
) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = vec![];
    U::try_from_physical_plan(plan, codec.physical_extension_codec())
        .and_then(|proto| proto.try_encode(&mut buf))?;
    Ok(buf)
}

fn decode_inputs(
    stage_inputs: Vec<GraphStageInput>,
) -> Result<HashMap<usize, StageOutput>> {
    let mut inputs: HashMap<usize, StageOutput> = HashMap::new();
    for input in stage_inputs {
        let stage_id = input.stage_id as usize;

        let outputs = input
            .partition_locations
            .into_iter()
            .map(|loc| {
                let partition = loc.partition as usize;
                let locations = loc
                    .partition_location
                    .into_iter()
                    .map(|l| l.try_into())
                    .collect::<Result<Vec<_>>>()?;
                Ok((partition, locations))
            })
            .collect::<Result<HashMap<usize, Vec<PartitionLocation>>>>()?;

        inputs.insert(
            stage_id,
            StageOutput {
                partition_locations: outputs,
                complete: input.complete,
            },
        );
    }
    Ok(inputs)
}

fn encode_inputs(
    stage_inputs: HashMap<usize, StageOutput>,
) -> Result<Vec<GraphStageInput>> {
    let mut inputs: Vec<protobuf::GraphStageInput> = vec![];
    for (stage_id, output) in stage_inputs.into_iter() {
        inputs.push(protobuf::GraphStageInput {
            stage_id: stage_id as u32,
            partition_locations: output
                .partition_locations
                .into_iter()
                .map(|(partition, locations)| {
                    Ok(protobuf::TaskInputPartitions {
                        partition: partition as u32,
                        partition_location: locations
                            .into_iter()
                            .map(|l| l.try_into())
                            .collect::<Result<Vec<_>>>()?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            complete: output.complete,
        });
    }
    Ok(inputs)
}

fn decode_taskinfo(task_info: protobuf::TaskInfo) -> Result<TaskInfo> {
    // These protobufs are persisted (object store) and may come from an older
    // version or be corrupt; return an error rather than panicking so a single
    // bad graph cannot crash the scheduler during recovery.
    let task_info_status = match task_info.status {
        Some(task_info::Status::Running(running)) => {
            task_status::Status::Running(running)
        }
        Some(task_info::Status::Failed(failed)) => task_status::Status::Failed(failed),
        Some(task_info::Status::Successful(success)) => {
            task_status::Status::Successful(success)
        }
        None => {
            return Err(BallistaError::Internal(format!(
                "protobuf::TaskInfo status for task {} is missing; cannot decode persisted graph",
                task_info.task_id
            )));
        }
    };
    let executor_id = match &task_info_status {
        task_status::Status::Running(running) => running.executor_id.clone(),
        task_status::Status::Successful(success) => success.executor_id.clone(),
        task_status::Status::Failed(_) => String::new(),
    };
    Ok(TaskInfo {
        task_id: task_info.task_id as usize,
        executor_id,
        scheduled_time: task_info.scheduled_time as u128,
        launch_time: task_info.launch_time as u128,
        start_exec_time: task_info.start_exec_time as u128,
        end_exec_time: task_info.end_exec_time as u128,
        finish_time: task_info.finish_time as u128,
        task_status: task_info_status,
    })
}

fn encode_taskinfo(task_info: TaskInfo, partition_id: usize) -> protobuf::TaskInfo {
    let task_info_status = match task_info.task_status {
        task_status::Status::Running(running) => task_info::Status::Running(running),
        task_status::Status::Failed(failed) => task_info::Status::Failed(failed),
        task_status::Status::Successful(success) => {
            task_info::Status::Successful(success)
        }
    };
    protobuf::TaskInfo {
        task_id: task_info.task_id as u32,
        partition_id: partition_id as u32,
        scheduled_time: task_info.scheduled_time as u64,
        launch_time: task_info.launch_time as u64,
        start_exec_time: task_info.start_exec_time as u64,
        end_exec_time: task_info.end_exec_time as u64,
        finish_time: task_info.finish_time as u64,
        status: Some(task_info_status),
    }
}
