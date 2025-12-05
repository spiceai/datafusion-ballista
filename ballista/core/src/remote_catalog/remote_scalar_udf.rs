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

use crate::serde::protobuf::ScalarUdfInfo;
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::common::{exec_err, plan_err, DataFusionError};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use std::any::Any;
use std::hash::Hash;

/// A stub provider to encapsulate a function that exists in the scheduler's registry,
/// in order to decouple its specific implementation from logical planning
#[derive(Debug)]
pub struct RemoteScalarUDF {
    arities: Vec<Vec<DataType>>,
    meta: ScalarUdfInfo,
    signature: Signature,
    documentation: Option<Documentation>,
}

impl Hash for RemoteScalarUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arities.hash(state);
        self.meta.name.hash(state);
        self.signature.hash(state);
    }
}

impl PartialEq for RemoteScalarUDF {
    fn eq(&self, other: &Self) -> bool {
        self.arities == other.arities
            && self.meta.name == other.meta.name
            && self.signature == other.signature
    }
}

impl Eq for RemoteScalarUDF {}

impl RemoteScalarUDF {
    pub fn new(meta: ScalarUdfInfo) -> Result<Self> {
        let mut arities = vec![];

        for signature in &meta.signatures {
            let signature_types = signature
                .arity
                .iter()
                .map(|t| t.try_into())
                .collect::<Result<Vec<DataType>, _>>()?;

            arities.push(signature_types);
        }

        let documentation = meta.documentation.clone().map(|d| Documentation {
            doc_section: Default::default(),
            description: d.description.clone(),
            syntax_example: d.syntax_example.clone(),
            sql_example: d.sql_example.clone(),
            arguments: Some(
                d.arguments
                    .iter()
                    .map(|a| (a.argument.clone(), a.description.clone()))
                    .collect(),
            ),
            alternative_syntax: None,
            related_udfs: None,
        });

        Ok(Self {
            arities,
            documentation,
            meta,
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Volatile),
        })
    }
}

impl ScalarUDFImpl for RemoteScalarUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.meta.name.as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[DataType],
    ) -> datafusion::common::Result<DataType> {
        let Some((sig_index, _)) = self
            .arities
            .iter()
            .enumerate()
            .find(|(_, s)| s == &arg_types)
        else {
            return plan_err!("Unable to determine function return type");
        };

        let arrow_type = self.meta.signatures[sig_index]
            .clone()
            .return_type
            .expect("Must define return type");

        (&arrow_type)
            .try_into()
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        exec_err!("This is a stub function and should never be called on the client")
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.documentation.as_ref()
    }
}
