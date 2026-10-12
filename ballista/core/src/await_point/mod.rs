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

//! Suspending a Ballista task at its await points.
//!
//! A task holds one executor vcore for its whole life, even while a source
//! waits on a slow remote system and the task uses no CPU. An executor given an
//! [`AwaitPointRule`] wraps the selected leaves of each stage plan in an
//! [`AwaitPointExec`] and runs the task with a [`VcorePermit`]. When an await
//! point returns `Pending` (not a self-wake or a cooperative yield) the task is
//! suspended; once every await point of the task has been suspended for the
//! rule's `suspend_after`, the vcore is released for other tasks, and the
//! task acquires a vcore again before it resumes. Like `.await`, but for a
//! task's claim on executor capacity.

mod exec;
mod permit;
mod rule;

pub use exec::AwaitPointExec;
pub use permit::{SuspendStats, VcorePermit, global_stats};
pub use rule::{AwaitPointFilter, AwaitPointRule};
