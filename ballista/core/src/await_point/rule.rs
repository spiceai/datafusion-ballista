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

use super::AwaitPointExec;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Selects the leaf nodes that become await points.
pub type AwaitPointFilter = Arc<dyn Fn(&dyn ExecutionPlan) -> bool + Send + Sync>;

/// Wraps the selected leaf nodes of a stage plan in [`AwaitPointExec`], and
/// carries the settings an executor needs to suspend tasks at them.
///
/// Providing this rule to an executor (`Executor::with_await_point_rule`) is
/// the opt-in: the executor applies it to every decoded stage plan and gives
/// each task a [`VcorePermit`](super::VcorePermit), whose vcore is released to
/// other tasks while the task is suspended. The number of suspended tasks is
/// not capped. The rule runs on the executor rather than the scheduler so
/// scheduler rules still see the unwrapped leaves. Applying it twice is a
/// no-op.
#[derive(Clone)]
pub struct AwaitPointRule {
    filter: AwaitPointFilter,
    suspend_after: Duration,
}

impl AwaitPointRule {
    /// Make every leaf for which `filter` returns true an await point. Only
    /// select leaves whose `Pending` means waiting on a remote system: a leaf
    /// waiting on CPU work elsewhere would release a vcore that is still busy.
    #[must_use]
    pub fn new(filter: AwaitPointFilter) -> Self {
        Self {
            filter,
            suspend_after: Duration::from_millis(50),
        }
    }

    /// How long every await point of a task must stay suspended before the
    /// task's vcore is released. Default 50 ms, which keeps short local waits
    /// and cooperative yields from releasing it.
    #[must_use]
    pub fn with_suspend_after(mut self, suspend_after: Duration) -> Self {
        self.suspend_after = suspend_after;
        self
    }

    /// See [`with_suspend_after`](Self::with_suspend_after).
    #[must_use]
    pub fn suspend_after(&self) -> Duration {
        self.suspend_after
    }

    /// Wrap the selected leaves of `plan`, reporting whether any were wrapped.
    ///
    /// # Errors
    ///
    /// Fails if rebuilding a parent node with its wrapped child fails.
    pub fn wrap(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        plan.transform_down(|node| {
            if node.downcast_ref::<AwaitPointExec>().is_some() {
                return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
            }
            if !node.children().is_empty() || !(self.filter)(node.as_ref()) {
                return Ok(Transformed::no(node));
            }
            let wrapped: Arc<dyn ExecutionPlan> = Arc::new(AwaitPointExec::new(node));
            Ok(Transformed::new(wrapped, true, TreeNodeRecursion::Jump))
        })
    }
}

impl fmt::Debug for AwaitPointRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwaitPointRule")
            .field("suspend_after", &self.suspend_after)
            .finish_non_exhaustive()
    }
}

impl PhysicalOptimizerRule for AwaitPointRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.wrap(plan).map(|t| t.data)
    }

    fn name(&self) -> &'static str {
        "await_point"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
    use datafusion::physical_plan::union::UnionExec;

    fn plan() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::empty());
        UnionExec::try_new(vec![
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
            Arc::new(PlaceholderRowExec::new(schema)),
        ])
        .expect("union of leaves")
    }

    fn render(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(false).to_string()
    }

    fn named(name: &'static str) -> AwaitPointRule {
        AwaitPointRule::new(Arc::new(move |plan| plan.name() == name))
    }

    #[test]
    fn wraps_only_selected_leaves_once() {
        let rule = named("EmptyExec");
        let once = rule.wrap(plan()).expect("wrap");
        assert!(once.transformed);
        let twice = rule.wrap(Arc::clone(&once.data)).expect("wrap");
        assert!(!twice.transformed);
        let expected =
            "UnionExec\n  AwaitPointExec\n    EmptyExec\n  PlaceholderRowExec\n";
        assert_eq!(render(&once.data), expected);
        assert_eq!(render(&twice.data), expected);
    }

    #[test]
    fn no_selected_leaf_leaves_plan_unchanged() {
        let out = named("HttpExec").wrap(plan()).expect("wrap");
        assert!(!out.transformed);
        assert_eq!(
            render(&out.data),
            "UnionExec\n  EmptyExec\n  PlaceholderRowExec\n"
        );
    }

    #[test]
    fn non_leaves_are_never_wrapped() {
        let out = AwaitPointRule::new(Arc::new(|_| true))
            .wrap(plan())
            .expect("wrap");
        assert!(render(&out.data).starts_with("UnionExec\n"));
    }
}
