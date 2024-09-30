use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use duckdb::Connection;
use http::StatusCode;
use lambda_runtime::tracing;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::io::Cursor;

#[derive(Deserialize)]
struct Request {
    query: Option<String>,
}

#[derive(Serialize)]
struct ArrowIpcResponse {
    status_code: u16,
    headers: serde_json::Value,
    #[serde(with = "serde_bytes")]
    body: Vec<u8>,
}

fn convert_to_arrow_ipc(rbs: &[RecordBatch]) -> Result<Vec<u8>, Error> {
    let mut buffer = Cursor::new(Vec::new());
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &rbs[0].schema())?;
        for batch in rbs {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(buffer.into_inner())
}

async fn function_handler(event: LambdaEvent<Request>) -> Result<ArrowIpcResponse, Error> {
    let query = event.payload.query.unwrap_or_else(||
        "SELECT * FROM read_parquet('https://shell.duckdb.org/data/tpch/0_01/parquet/customer.parquet') LIMIT 5".to_string()
    );

    // Create an in-memory DuckDB database
    let conn = Connection::open_in_memory()?;

    // Execute the query using arrow
    let mut stmt = conn.prepare(&query)?;
    let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

    // Convert RecordBatches to Arrow IPC format
    let arrow_ipc_data = convert_to_arrow_ipc(&rbs)?;

    // Return the custom response
    Ok(ArrowIpcResponse {
        status_code: StatusCode::OK.as_u16(),
        headers: json!({
            "Content-Type": "application/vnd.apache.arrow.stream",
        }),
        body: arrow_ipc_data,
    })
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();
    run(service_fn(function_handler)).await
}
