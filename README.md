Pond is a serverless distributed query engine that runs DuckDB on AWS Lambda, enabling scalable analytical query processing across multiple Lambda functions. Built with Rust and leveraging Apache Arrow for efficient data interchange, this project brings the power of DuckDB's analytical capabilities to serverless architectures.

## Motiviation

Inspired by projects like MotherDuck, which connects DuckDB to cloud resources, DuckDB Lambda aims to provide a serverless approach to distributed analytical query processing. By utilizing AWS Lambda, we can offer a scalable solution that can handle varying workloads without the need for always-on infrastructure.

## Deployment

```bash
cargo lambda build --release
```

```bash
cargo lambda deploy
```

## Development

### Local Testing

```bash
cargo lambda watch
```

```bash
cargo lambda invoke pond-planner --data-ascii '{"query": "SELECT COUNT(*) FROM read_parquet('\''https://shell.duckdb.org/data/tpch/0_01/parquet/customer.parquet'\'') WHERE c_name LIKE '\''Customer%'\'' GROUP BY customer "}' --output-format json
```
