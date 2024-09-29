use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use aws_config::BehaviorVersion;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::{types::InvocationType, Client as LambdaClient};
use futures::future::join_all;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{Expr, GroupByExpr, Query, Select, SelectItem, SetExpr, Statement};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;
use std::io::Cursor;
use std::sync::Arc;

#[derive(Deserialize)]
struct Request {
    query: String,
}

#[derive(Serialize)]
struct ArrowIpcResponse {
    status_code: u16,
    headers: serde_json::Value,
    #[serde(with = "serde_bytes")]
    body: Vec<u8>,
}

struct QueryPlanner {
    lambda_client: LambdaClient,
}

#[derive(Default)]
struct DistributedPlan {
    table: String,
    group_column: Option<String>,
    agg_function: String,
    partitions: Vec<String>,
}

impl QueryPlanner {
    async fn new() -> Result<Self, Error> {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let lambda_client = LambdaClient::new(&config);
        Ok(Self { lambda_client })
    }

    async fn plan_and_execute(&self, query: &str) -> Result<ArrowIpcResponse, Error> {
        let plan = self.analyze_query(query)?;
        let results = self.execute_plan(plan).await?;
        self.create_arrow_response(results)
    }

    fn analyze_query(&self, query: &str) -> Result<DistributedPlan, Error> {
        let dialect = DuckDbDialect {};
        let ast = Parser::parse_sql(&dialect, query)?;

        if let Statement::Query(query) = &ast[0] {
            let Query { body, .. } = query.as_ref();
            if let SetExpr::Select(select) = body.as_ref() {
                let select = select.as_ref();
                let Select {
                    projection,
                    from,
                    group_by,
                    ..
                } = select;

                let table_name = &from[0].relation.to_string();

                let group_column = match group_by {
                    GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => {
                        if let Expr::Identifier(ident) = &exprs[0] {
                            ident.value.clone()
                        } else {
                            return Err("Unsupported GROUP BY expression".into());
                        }
                    }
                    GroupByExpr::All(_) => return Err("GROUP BY ALL is not supported".into()),
                    GroupByExpr::Expressions(_, _) => return Err("GROUP BY clause is empty".into()),
                };

                let agg_function =
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &projection[0] {
                        func.name.to_string()
                    } else {
                        return Err("Unsupported aggregation".into());
                    };

                // In a real scenario, determine partitions based on data distribution
                let partitions = vec![
                    "A".to_string(),
                    "B".to_string(),
                    "C".to_string(),
                    "D".to_string(),
                ];

                Ok(DistributedPlan {
                    table: table_name.clone(),
                    group_column,
                    agg_function,
                    partitions,
                })
            } else {
                Err("Unsupported query type".into())
            }
        } else {
            Err("Unsupported statement type".into())
        }
    }

    async fn execute_plan(&self, plan: DistributedPlan) -> Result<Vec<(String, i64)>, Error> {
        let mut tasks = Vec::new();

        for partition in plan.partitions {
            let payload = serde_json::json!({
                "table": plan.table,
                "group_column": plan.group_column,
                "agg_function": plan.agg_function,
                "partition": partition
            });

            let payload_string = serde_json::to_string(&payload)?;
            let payload_bytes = payload_string.into_bytes();
            let blob = Blob::new(payload_bytes);

            let req = self
                .lambda_client
                .invoke()
                .function_name("worker-lambda")
                .invocation_type(InvocationType::RequestResponse)
                .payload(blob);

            tasks.push(tokio::spawn(async move { req.send().await }));
        }

        let results = join_all(tasks).await;
        let mut final_result = Vec::new();

        for result in results {
            match result {
                Ok(Ok(output)) => {
                    if let Some(payload) = output.payload {
                        let payload_vec: Vec<u8> = payload.into_inner();
                        let partial: serde_json::Value = serde_json::from_slice(&payload_vec)?;
                        for (key, value) in partial.as_object().unwrap() {
                            final_result.push((key.clone(), value.as_i64().unwrap_or(0)));
                        }
                    }
                }
                Ok(Err(err)) => return Err(format!("Lambda invocation error: {:?}", err).into()),
                Err(err) => return Err(format!("Task join error: {:?}", err).into()),
            }
        }

        Ok(final_result)
    }

    fn create_arrow_response(
        &self,
        results: Vec<(String, i64)>,
    ) -> Result<ArrowIpcResponse, Error> {
        let schema = Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ]);

        let categories: Vec<_> = results.iter().map(|(cat, _)| cat.as_str()).collect();
        let counts: Vec<_> = results.iter().map(|(_, count)| *count).collect();

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(StringArray::from(categories)),
                Arc::new(Int64Array::from(counts)),
            ],
        )?;

        let mut buffer = Cursor::new(Vec::new());
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }

        Ok(ArrowIpcResponse {
            status_code: 200,
            headers: serde_json::json!({
                "Content-Type": "application/vnd.apache.arrow.stream",
            }),
            body: buffer.into_inner(),
        })
    }
}

async fn function_handler(event: LambdaEvent<Request>) -> Result<ArrowIpcResponse, Error> {
    let planner = QueryPlanner::new().await?;
    planner.plan_and_execute(&event.payload.query).await
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda_runtime::run(service_fn(function_handler)).await
}
