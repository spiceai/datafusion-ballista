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
//

//! Remote catalog serialization and stub providers for Ballista clients.
//!
//! This module provides functionality to serialize catalog metadata (schemas, tables, functions)
//! from the scheduler to ship to Ballista clients, as well as stub providers that allow clients
//! to perform logical planning without access to actual table data.

/// Extension trait for serializing catalog schemas and table names.
pub mod catalog_serialize_ext;
/// Extension trait for serializing user-defined functions.
pub mod remote_function_serialize_ext;
/// Stub scalar UDF implementation for remote function planning.
pub mod remote_scalar_udf;
/// Stub table provider for remote table planning.
pub mod remote_table_provider;
