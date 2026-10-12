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

//! Lending a pending task's vcore back to the executor.
//!
//! A Ballista task holds one vcore for its whole life, even while its sources
//! sit on a slow remote response and use no CPU. With
//! `ballista.executor.lend_vcore_on_pending_task` the executor acquires the
//! task's vcore through a [`LendableVcore`], and [`LendableVcoreRule`] wraps
//! every leaf of the stage plan in a [`LendableVcoreExec`] whose stream reports
//! when it parks (`Poll::Pending` that is neither a self-wake nor a coop-budget
//! yield) and resumes. Once every wrapped stream of the task has been parked
//! for the pending threshold, the vcore is lent back so the poll loop can take
//! more work; the next stream to resume gets a vcore back before it polls its
//! input.
//!
//! Sources that know more than the `Pending` heuristic can mark their waits
//! with [`io_wait`] (lend immediately) or [`cpu_wait`] (never lend).

mod exec;
mod rule;
mod vcore;

pub use exec::LendableVcoreExec;
pub use rule::LendableVcoreRule;
pub use vcore::{LendStats, LendableVcore, cpu_wait, global_stats, io_wait};
