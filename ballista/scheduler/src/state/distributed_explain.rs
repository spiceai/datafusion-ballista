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

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use ballista_core::error::Result;
use datafusion::arrow::array::{ListArray, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::format::ExplainFormat;
use datafusion::common::{ScalarValue, UnnestOptions};
use datafusion::logical_expr::{LogicalPlan, PlanType, StringifiedPlan};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::expressions::col;
use datafusion::physical_plan::expressions::lit;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::unnest::{ListUnnest, UnnestExec};
use datafusion::prelude::SessionContext;

use crate::state::execution_graph::ExecutionStage;
use crate::{
    planner::{DefaultDistributedPlanner, DistributedPlanner},
    state::execution_graph::ExecutionStageBuilder,
};

pub(crate) async fn generate_distributed_explain_plan(
    job_id: &str,
    session_ctx: Arc<SessionContext>,
    plan: Arc<LogicalPlan>,
) -> Result<String> {
    let session_config = Arc::new(session_ctx.copied_config());

    let plan = session_ctx.state().create_physical_plan(&plan).await?;

    let mut planner = DefaultDistributedPlanner::new();
    let shuffle_stages =
        planner.plan_query_stages(job_id, plan, session_config.options())?;
    let builder = ExecutionStageBuilder::new(session_config.clone());
    let stages = builder.build(shuffle_stages)?;

    Ok(render_stages(stages))
}

pub(crate) fn extract_logical_and_physical_plans(
    plans: &[StringifiedPlan],
    format: &ExplainFormat,
) -> (String, String) {
    // For Tree format, DataFusion only provides physical_plan with tree rendering
    // and doesn't include logical plan. We match that behavior.
    match format {
        ExplainFormat::Tree => {
            // For tree format, logical plan is not available (matching DataFusion behavior)
            let logical_txt = String::new();

            let physical_txt = plans
                .iter()
                .find(|p| matches!(p.plan_type, PlanType::FinalPhysicalPlan))
                .map(|p| p.plan.to_string())
                .unwrap_or_else(|| "<physical plan not available>".to_string());

            (logical_txt, physical_txt)
        }
        ExplainFormat::Indent | ExplainFormat::PostgresJSON | ExplainFormat::Graphviz => {
            let logical_txt = plans
                .iter()
                .rev()
                .find(|p| matches!(p.plan_type, PlanType::FinalAnalyzedLogicalPlan))
                .or_else(|| {
                    // Fall back to the pre-analysis FinalLogicalPlan when the
                    // analyzed plan is not present.
                    plans
                        .iter()
                        .find(|p| matches!(p.plan_type, PlanType::FinalLogicalPlan))
                })
                .or_else(|| plans.first())
                .map(|p| p.plan.to_string())
                .unwrap_or_else(|| "logical plan not available".to_string());

            let physical_txt = plans
                .iter()
                .find(|p| matches!(p.plan_type, PlanType::FinalPhysicalPlan))
                .map(|p| p.plan.to_string())
                .unwrap_or_else(|| "<physical plan not available>".to_string());

            (logical_txt, physical_txt)
        }
    }
}

/// Build a distributed explain execution plan that produces a two-column table:
///
/// For Indent format (default):
/// | plan_type        | plan            |
/// |------------------|-----------------|
/// | logical_plan     | logical_txt     |
/// | physical_plan    | physical_txt    |
/// | distributed_plan | distributed_txt |
///
/// For Tree format (matching DataFusion behavior - only physical plan is shown):
/// | plan_type        | plan            |
/// |------------------|-----------------|
/// | physical_plan    | physical_txt    |
/// | distributed_plan | distributed_txt |
///
/// The transformed physical tree looks like:
///     CoalescePartitionsExec
///       └─ ProjectionExec: expr=[list_type -> plan_type, list_plan -> plan]
///            └─ UnnestExec
///                 └─ ProjectionExec: expr=[list_type, list_plan]
///                      └─ PlaceholderRowExec
pub(crate) fn construct_distributed_explain_exec(
    logical_txt: String,
    physical_txt: String,
    distributed_txt: String,
    format: &ExplainFormat,
) -> Result<Arc<dyn ExecutionPlan>> {
    let place_holder_row: Arc<PlaceholderRowExec> =
        Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty())));

    // Determine which rows to include based on format
    // For Tree format, DataFusion omits logical_plan
    let (plan_types, plan_texts): (Vec<&str>, Vec<&str>) = match format {
        ExplainFormat::Tree => (
            vec!["physical_plan", "distributed_plan"],
            vec![&physical_txt, &distributed_txt],
        ),
        ExplainFormat::Indent | ExplainFormat::PostgresJSON | ExplainFormat::Graphviz => {
            (
                vec!["logical_plan", "physical_plan", "distributed_plan"],
                vec![&logical_txt, &physical_txt, &distributed_txt],
            )
        }
    };

    // construct list_type from plan_types
    let mut type_list_builder = ListBuilder::new(StringBuilder::new());
    {
        let vb = type_list_builder.values();
        for plan_type in &plan_types {
            vb.append_value(plan_type);
        }
    }
    type_list_builder.append(true);
    let list_type_array: Arc<ListArray> = Arc::new(type_list_builder.finish());

    // construct list_plan from plan_texts
    let mut plan_list_builder = ListBuilder::new(StringBuilder::new());
    {
        let vb = plan_list_builder.values();
        for plan_text in &plan_texts {
            vb.append_value(plan_text);
        }
    }
    plan_list_builder.append(true);
    let list_plan_array: Arc<ListArray> = Arc::new(plan_list_builder.finish());

    // Project both List literals onto the placeholder row
    let exprs_lists: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
        (
            lit(ScalarValue::List(list_type_array)),
            "list_type".to_string(),
        ),
        (
            lit(ScalarValue::List(list_plan_array)),
            "list_plan".to_string(),
        ),
    ];
    let proj_lists = Arc::new(ProjectionExec::try_new(exprs_lists, place_holder_row)?);

    // Build the Unnest operator to expand the two List columns row-wise
    let lists = vec![
        ListUnnest {
            index_in_input_schema: 0,
            depth: 1,
        },
        ListUnnest {
            index_in_input_schema: 1,
            depth: 1,
        },
    ];
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("list_type", DataType::Utf8, true),
        Field::new("list_plan", DataType::Utf8, true),
    ]));
    let unnest = Arc::new(UnnestExec::new(
        proj_lists,
        lists,
        Vec::new(),
        out_schema,
        UnnestOptions::default(),
    )?);

    // Final projection: rename columns to (plan_type, plan)
    let proj_final = Arc::new(ProjectionExec::try_new(
        vec![
            (
                col("list_type", unnest.schema().as_ref())?,
                "plan_type".into(),
            ),
            (col("list_plan", unnest.schema().as_ref())?, "plan".into()),
        ],
        unnest,
    )?);

    // CoalescePartitionsExec → merge all partitions into one
    // ensuring deterministic single-partition output.
    Ok(Arc::new(CoalescePartitionsExec::new(proj_final)) as Arc<dyn ExecutionPlan>)
}

fn render_stages(stages: HashMap<usize, ExecutionStage>) -> String {
    let mut buf = String::new();
    let mut keys: Vec<_> = stages.keys().cloned().collect();
    keys.sort();
    for k in keys {
        let stage = &stages[&k];
        writeln!(buf, "{:#?}", stage).ok();
    }
    buf
}
