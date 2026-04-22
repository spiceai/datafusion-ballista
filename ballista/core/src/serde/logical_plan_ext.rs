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

//! Ballista-specific logical plan extension nodes.
//!
//! These extension nodes wrap DataFusion `LogicalPlan::Explain` so that
//! fields not covered by `datafusion-proto` (notably `ExplainFormat`) are
//! preserved when the plan is sent from the client to the Ballista
//! scheduler.
//!
//! The client planner wraps an `Explain` in `BallistaExplainNode` before
//! submitting it to the scheduler. The scheduler unwraps it back to the
//! native `LogicalPlan::Explain` before physical planning.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::DFSchemaRef;
use datafusion::common::format::ExplainFormat;
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};

/// Ballista wrapper for `LogicalPlan::Explain`.
///
/// Preserves fields that are lost by the default `datafusion-proto`
/// `ExplainNode` (in particular `explain_format`).
#[derive(Debug, Clone)]
pub struct BallistaExplainNode {
    /// Whether verbose output was requested.
    pub verbose: bool,
    /// The explain output format (Indent, Tree, PostgresJSON, Graphviz).
    pub explain_format: ExplainFormat,
    /// The plan being explained.
    pub plan: Arc<LogicalPlan>,
    /// Output schema for the explain (typically `plan_type`, `plan`).
    pub schema: DFSchemaRef,
}

impl BallistaExplainNode {
    /// Serialize `ExplainFormat` to a stable string identifier.
    pub fn format_as_str(format: &ExplainFormat) -> &'static str {
        match format {
            ExplainFormat::Indent => "indent",
            ExplainFormat::Tree => "tree",
            ExplainFormat::PostgresJSON => "pgjson",
            ExplainFormat::Graphviz => "graphviz",
        }
    }

    /// Parse a stable string identifier back into `ExplainFormat`.
    pub fn format_from_str(s: &str) -> Option<ExplainFormat> {
        match s {
            "indent" => Some(ExplainFormat::Indent),
            "tree" => Some(ExplainFormat::Tree),
            "pgjson" => Some(ExplainFormat::PostgresJSON),
            "graphviz" => Some(ExplainFormat::Graphviz),
            _ => None,
        }
    }
}

impl PartialEq for BallistaExplainNode {
    fn eq(&self, other: &Self) -> bool {
        self.verbose == other.verbose
            && self.explain_format == other.explain_format
            && self.plan == other.plan
    }
}

impl Eq for BallistaExplainNode {}

impl PartialOrd for BallistaExplainNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.verbose.partial_cmp(&other.verbose) {
            Some(Ordering::Equal) => {
                match Self::format_as_str(&self.explain_format)
                    .partial_cmp(Self::format_as_str(&other.explain_format))
                {
                    Some(Ordering::Equal) => self.plan.partial_cmp(&other.plan),
                    cmp => cmp,
                }
            }
            cmp => cmp,
        }
    }
}

impl Hash for BallistaExplainNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.verbose.hash(state);
        Self::format_as_str(&self.explain_format).hash(state);
        self.plan.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for BallistaExplainNode {
    fn name(&self) -> &str {
        "BallistaExplain"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.plan.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BallistaExplain: verbose={}, format={}",
            self.verbose,
            Self::format_as_str(&self.explain_format)
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        if inputs.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "BallistaExplainNode expects exactly 1 input, got {}",
                inputs.len()
            )));
        }
        Ok(BallistaExplainNode {
            verbose: self.verbose,
            explain_format: self.explain_format.clone(),
            plan: Arc::new(inputs.pop().unwrap()),
            schema: self.schema.clone(),
        })
    }
}

/// Downcast an `Extension`'s node to `BallistaExplainNode` if it is one.
pub fn as_ballista_explain(
    node: &dyn UserDefinedLogicalNode,
) -> Option<&BallistaExplainNode> {
    node.as_any().downcast_ref::<BallistaExplainNode>()
}
