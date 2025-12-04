use crate::serde::protobuf::ScalarUdfInfo;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, plan_err, DataFusionError};
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;

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
    pub fn new(meta: ScalarUdfInfo) -> Self {
        let arities = meta
            .signatures
            .iter()
            .map(|signature| {
                signature
                    .arity
                    .iter()
                    .map(|t| t.try_into())
                    .collect::<Result<Vec<DataType>, _>>()
                    .expect("Must deserialize arities")
            })
            .collect();

        let documentation = meta.documentation.clone().map(|d| Documentation {
            doc_section: Default::default(),
            description: d.description.clone(),
            syntax_example: d.syntax_example.clone(),
            sql_example: d.sql_example.clone(),
            arguments: None,
            alternative_syntax: None,
            related_udfs: None,
        });

        Self {
            arities,
            documentation,
            meta,
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Volatile),
        }
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
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        exec_err!("This is a stub function and should never be called on the client")
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.documentation.as_ref()
    }
}
