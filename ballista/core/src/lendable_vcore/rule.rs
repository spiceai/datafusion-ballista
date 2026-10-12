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

use super::LendableVcoreExec;
use crate::config::BallistaConfig;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use std::fmt;
use std::sync::Arc;

/// Decides whether a leaf gets wrapped.
pub type LeafFilter = Arc<dyn Fn(&dyn ExecutionPlan) -> bool + Send + Sync>;

/// Wraps leaf nodes in [`LendableVcoreExec`] when `ballista.executor.lend_vcore_on_pending_task`
/// is set.
///
/// Meant to run on the executor against the decoded stage plan, after every
/// planning rule: wrapping on the scheduler would hide `DataSourceExec` and
/// `ShuffleReaderExec` from rules that downcast to them, and the wrapper would
/// need a codec entry to reach the executor. Applying it twice is a no-op.
#[derive(Clone, Default)]
pub struct LendableVcoreRule {
    filter: Option<LeafFilter>,
}

impl LendableVcoreRule {
    /// Wrap only the leaves for which `filter` returns true, e.g. to skip
    /// sources known to block on CPU work done elsewhere.
    #[must_use]
    pub fn with_filter(mut self, filter: LeafFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    fn should_wrap(&self, plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.children().is_empty()
            && self.filter.as_ref().is_none_or(|f| f(plan.as_ref()))
    }
}

impl fmt::Debug for LendableVcoreRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LendableVcoreRule")
            .field("filtered", &self.filter.is_some())
            .finish()
    }
}

impl PhysicalOptimizerRule for LendableVcoreRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let enabled = config
            .extensions
            .get::<BallistaConfig>()
            .is_some_and(BallistaConfig::lend_vcore_on_pending_task);
        if !enabled {
            return Ok(plan);
        }
        plan.transform_down(|node| {
            if node.downcast_ref::<LendableVcoreExec>().is_some() {
                return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
            }
            if !self.should_wrap(&node) {
                return Ok(Transformed::no(node));
            }
            let wrapped: Arc<dyn ExecutionPlan> = Arc::new(LendableVcoreExec::new(node));
            Ok(Transformed::new(wrapped, true, TreeNodeRecursion::Jump))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &'static str {
        "lendable_vcore"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BALLISTA_LEND_VCORE_ON_PENDING_TASK;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::prelude::SessionConfig;

    fn options(enabled: bool) -> ConfigOptions {
        SessionConfig::new_with_ballista()
            .set_bool(BALLISTA_LEND_VCORE_ON_PENDING_TASK, enabled)
            .options()
            .as_ref()
            .clone()
    }

    fn plan() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::empty());
        UnionExec::try_new(vec![
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
            Arc::new(EmptyExec::new(schema)),
        ])
        .expect("union of empty inputs")
    }

    fn render(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(false).to_string()
    }

    use crate::extension::SessionConfigExt;

    #[test]
    fn wraps_every_leaf_once() {
        let rule = LendableVcoreRule::default();
        let once = rule.optimize(plan(), &options(true)).expect("optimize");
        let twice = rule
            .optimize(Arc::clone(&once), &options(true))
            .expect("optimize");
        let expected = "UnionExec\n  LendableVcoreExec\n    EmptyExec\n  LendableVcoreExec\n    EmptyExec\n";
        assert_eq!(render(&once), expected);
        assert_eq!(render(&twice), expected);
    }

    #[test]
    fn disabled_leaves_plan_unchanged() {
        let out = LendableVcoreRule::default()
            .optimize(plan(), &options(false))
            .expect("optimize");
        assert_eq!(render(&out), "UnionExec\n  EmptyExec\n  EmptyExec\n");
    }

    #[test]
    fn filter_skips_leaves() {
        let rule = LendableVcoreRule::default().with_filter(Arc::new(|_| false));
        let out = rule.optimize(plan(), &options(true)).expect("optimize");
        assert_eq!(render(&out), "UnionExec\n  EmptyExec\n  EmptyExec\n");
    }
}
