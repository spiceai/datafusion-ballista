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

use ballista_core::serde::protobuf::TaskStatus;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct ExecutorStatusStore {
    task_statuses: DashMap<String, Arc<Mutex<VecDeque<TaskStatus>>>>,
}

impl ExecutorStatusStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_task_status(&self, scheduler_id: String, status: TaskStatus) {
        let queue = self
            .task_statuses
            .entry(scheduler_id)
            .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
            .value()
            .clone();
        let lock_result = queue.lock();
        if let Ok(mut guard) = lock_result {
            guard.push_back(status);
        }
    }

    pub fn drain_task_statuses(
        &self,
        scheduler_id: &str,
        max_count: usize,
    ) -> Vec<TaskStatus> {
        let Some(entry) = self.task_statuses.get(scheduler_id) else {
            return Vec::new();
        };
        let queue = entry.value().clone();
        let Ok(mut queue) = queue.lock() else {
            return Vec::new();
        };

        let count = if max_count == 0 {
            queue.len()
        } else {
            max_count.min(queue.len())
        };

        (0..count).filter_map(|_| queue.pop_front()).collect()
    }
}
