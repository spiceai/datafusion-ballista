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

use crate::error::BallistaError::DataFusionError;
use crate::serde::protobuf::{
    ScalarUdfDocumentation, ScalarUdfInfo, ScalarUdfTypeSignature,
};
use datafusion::execution::FunctionRegistry;
use datafusion::prelude::SessionContext;
use datafusion_proto_common::ArrowType;

/// Used to serialize function shapes to ship to Ballista clients
pub trait FunctionSerializeExt {
    fn serialize_udfs(&self) -> Vec<ScalarUdfInfo>;
}

impl FunctionSerializeExt for SessionContext {
    fn serialize_udfs(&self) -> Vec<ScalarUdfInfo> {
        let mut udfs = vec![];

        for udf in self.udfs() {
            let f = self.udf(&udf).expect("Must find defined UDF");
            let signature = f.signature();
            let signatures = signature
                .type_signature
                .get_example_types()
                .into_iter()
                .map(|t| {
                    let arity = t
                        .iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<ArrowType>, _>>()
                        .expect("Must serialize data types");

                    let return_type = f.return_type(&t).expect("Must have return type");

                    ScalarUdfTypeSignature {
                        arity,
                        return_type: Some(
                            (&return_type)
                                .try_into()
                                .expect("Must serialize return type"),
                        ),
                    }
                })
                .collect::<Vec<_>>();

            let docs = f.documentation().map(|d| ScalarUdfDocumentation {
                description: d.description.clone(),
                syntax_example: d.syntax_example.clone(),
                sql_example: d.sql_example.clone(),
            });

            udfs.push(ScalarUdfInfo {
                name: f.name().to_string(),
                documentation: docs,
                signatures,
            });
        }

        udfs
    }
}
