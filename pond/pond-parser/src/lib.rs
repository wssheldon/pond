use duckdb::{Connection, Result as DuckResult};
use lazy_static::lazy_static;
use regex::Regex;
use sha2::{Digest, Sha256};
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    JoinConstraint, JoinOperator, ObjectName, Query as SqlQuery, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;
use thiserror::Error;

#[derive(Debug, Default)]
pub struct QueryAnalysis {
    tables: HashSet<String>,
    columns: HashSet<String>,
    conditions: Vec<String>,
    aggregations: Vec<String>,
    joins: Vec<String>,
    order_by: Vec<String>,
    limit: Option<u64>,
    offset: Option<u64>,
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("SQL parsing error: {0}")]
    SqlParseError(#[from] sqlparser::parser::ParserError),
    #[error("DuckDB error: {0}")]
    DuckDbError(#[from] duckdb::Error),
    #[error("Invalid filesystem: {0}")]
    InvalidFilesystem(String),
    #[error("Other error: {0}")]
    Other(String),
}

pub struct QueryWrapper {
    sql: String,
    hashed: String,
    ast: Statement,
    list_of_prefixes: Option<Vec<String>>,
}

impl QueryWrapper {
    pub fn parse(query: &str) -> Result<Self, QueryError> {
        let unified_query = Self::unify_query(query)?;
        let ast = Parser::parse_sql(&DuckDbDialect {}, &unified_query)?;

        if ast.is_empty() {
            return Err(QueryError::Other("Empty query".to_string()));
        }

        Ok(Self {
            hashed: Self::create_hash_string(&unified_query),
            sql: unified_query,
            ast: ast[0].clone(),
            list_of_prefixes: None,
        })
    }

    pub fn analyze(&self) -> QueryAnalysis {
        let mut analysis = QueryAnalysis::default();
        self.analyze_ast(&self.ast, &mut analysis);
        analysis
    }

    fn analyze_ast(&self, statement: &Statement, analysis: &mut QueryAnalysis) {
        match statement {
            Statement::Query(query) => self.analyze_query(query.as_ref(), analysis),
            _ => {} // Handle other statement types if needed
        }
    }

    fn analyze_query(&self, query: &SqlQuery, analysis: &mut QueryAnalysis) {
        if let SetExpr::Select(select) = query.body.as_ref() {
            self.analyze_select(select, analysis);
        }

        // Analyze ORDER BY
        for order in &query.order_by {
            analysis.order_by.push(order.to_string());
        }

        // Analyze LIMIT and OFFSET
        if let Some(limit) = &query.limit {
            if let Expr::Value(sqlparser::ast::Value::Number(n, _)) = limit {
                if let Ok(limit_value) = n.parse::<u64>() {
                    analysis.limit = Some(limit_value);
                }
            }
        }
        if let Some(offset) = &query.offset {
            if let Expr::Value(sqlparser::ast::Value::Number(n, _)) = &offset.value {
                if let Ok(offset_value) = n.parse::<u64>() {
                    analysis.offset = Some(offset_value);
                }
            }
        }
    }

    fn analyze_select(&self, select: &Select, analysis: &mut QueryAnalysis) {
        // Analyze FROM clause
        for table_with_joins in &select.from {
            self.analyze_from(table_with_joins, analysis);
        }

        // Analyze SELECT items
        for item in &select.projection {
            self.analyze_select_item(item, analysis);
        }

        // Analyze WHERE clause
        if let Some(where_clause) = &select.selection {
            self.analyze_expr(where_clause, analysis);
            analysis.conditions.push(where_clause.to_string());
        }

        // Analyze GROUP BY
        match &select.group_by {
            GroupByExpr::All(modifiers) => {
                analysis.aggregations.push("GROUP BY ALL".to_string());
                for modifier in modifiers {
                    analysis.aggregations.push(modifier.to_string());
                }
            }
            GroupByExpr::Expressions(exprs, modifiers) => {
                for expr in exprs {
                    self.analyze_expr(expr, analysis);
                    analysis.aggregations.push(expr.to_string());
                }
                for modifier in modifiers {
                    analysis.aggregations.push(modifier.to_string());
                }
            }
        }

        // Analyze HAVING
        if let Some(having) = &select.having {
            self.analyze_expr(having, analysis);
            analysis.conditions.push(having.to_string());
        }
    }

    fn analyze_from(&self, table_with_joins: &TableWithJoins, analysis: &mut QueryAnalysis) {
        analysis
            .tables
            .insert(table_with_joins.relation.to_string());
        for join in &table_with_joins.joins {
            analysis.tables.insert(join.relation.to_string());
            analysis.joins.push(format!("{:?}", join.join_operator));

            match &join.join_operator {
                JoinOperator::Inner(constraint)
                | JoinOperator::LeftOuter(constraint)
                | JoinOperator::RightOuter(constraint)
                | JoinOperator::FullOuter(constraint)
                | JoinOperator::LeftSemi(constraint)
                | JoinOperator::RightSemi(constraint)
                | JoinOperator::LeftAnti(constraint)
                | JoinOperator::RightAnti(constraint) => {
                    self.analyze_join_constraint(constraint, analysis);
                }
                JoinOperator::AsOf {
                    constraint,
                    match_condition,
                } => {
                    self.analyze_join_constraint(constraint, analysis);
                    self.analyze_expr(match_condition, analysis);
                }
                JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => {
                    // These join types don't have constraints
                }
            }
        }
    }

    fn analyze_join_constraint(&self, constraint: &JoinConstraint, analysis: &mut QueryAnalysis) {
        match constraint {
            JoinConstraint::On(expr) => {
                self.analyze_expr(expr, analysis);
            }
            JoinConstraint::Using(idents) => {
                for ident in idents {
                    analysis.columns.insert(ident.value.clone());
                }
            }
            JoinConstraint::Natural => {
                // Natural join doesn't have an explicit constraint
            }
            JoinConstraint::None => {
                // No constraint
            }
        }
    }

    fn analyze_select_item(&self, item: &SelectItem, analysis: &mut QueryAnalysis) {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                self.analyze_expr(expr, analysis);
            }
            SelectItem::QualifiedWildcard(name, _) => {
                analysis.columns.insert(name.to_string());
            }
            SelectItem::Wildcard(_) => {
                analysis.columns.insert("*".to_string());
            }
        }
    }

    fn analyze_expr(&self, expr: &Expr, analysis: &mut QueryAnalysis) {
        match expr {
            Expr::Identifier(col) => {
                analysis.columns.insert(col.value.clone());
            }
            Expr::Function(Function { name, args, .. }) => {
                analysis.aggregations.push(name.to_string());
                match args {
                    FunctionArguments::None => {}
                    FunctionArguments::Subquery(query) => {
                        self.analyze_query(query, analysis);
                    }
                    FunctionArguments::List(arg_list) => {
                        for arg in &arg_list.args {
                            match arg {
                                FunctionArg::Unnamed(arg_expr) => {
                                    self.analyze_function_arg_expr(arg_expr, analysis);
                                }
                                FunctionArg::Named { arg, .. } => {
                                    self.analyze_function_arg_expr(arg, analysis);
                                }
                            }
                        }
                    }
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.analyze_expr(left, analysis);
                self.analyze_expr(right, analysis);
            }
            // Add more cases as needed for other expression types
            _ => {}
        }
    }

    fn analyze_function_arg_expr(&self, arg_expr: &FunctionArgExpr, analysis: &mut QueryAnalysis) {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => self.analyze_expr(expr, analysis),
            FunctionArgExpr::QualifiedWildcard(object_name) => {
                analysis.columns.insert(object_name.to_string() + ".*");
            }
            FunctionArgExpr::Wildcard => {
                analysis.columns.insert("*".to_string());
            }
        }
    }

    pub fn replace(&mut self, old: &str, new: &str) {
        self.sql = self.sql.replace(old, new);
    }

    pub fn list_of_prefixes(&mut self) -> Result<&Vec<String>, QueryError> {
        if self.list_of_prefixes.is_none() {
            let prefixes = self.scan_source_for_prefixes()?;
            self.list_of_prefixes = Some(prefixes);
        }
        Ok(self.list_of_prefixes.as_ref().unwrap())
    }

    pub fn tables(&self) -> Vec<&TableFactor> {
        let mut tables = Vec::new();
        if let Statement::Query(query) = &self.ast {
            if let SetExpr::Select(select) = query.body.as_ref() {
                for TableWithJoins { relation, joins } in &select.from {
                    tables.push(relation);
                    for join in joins {
                        tables.push(&join.relation);
                    }
                }
            }
        }
        tables
    }

    pub fn bucket(&self) -> Result<String, QueryError> {
        lazy_static! {
            static ref BUCKET_RE: Regex = Regex::new(r"s3://([A-Za-z0-9_-]+)").unwrap();
        }

        for table in self.tables() {
            if let Some(captures) = BUCKET_RE.captures(&table.to_string()) {
                return Ok(format!("s3://{}", &captures[1]));
            }
        }

        Err(QueryError::InvalidFilesystem(
            "Not able to locate any bucket name in query".to_string(),
        ))
    }

    pub fn source(&self) -> Result<String, QueryError> {
        for table in self.tables() {
            if let TableFactor::Table { name, .. } = table {
                // Assuming the source is always the first (and only) part of the table name
                if name.0.len() == 1 {
                    return Ok(name.0[0].value.clone());
                }
            }
        }
        Err(QueryError::Other("No source found in query".to_string()))
    }

    pub fn scan_source_for_prefixes(&self) -> Result<Vec<String>, QueryError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;

        let source = self.source()?;
        let glob_query = format!(
            "SELECT DISTINCT CONCAT(REGEXP_REPLACE(file, '/[^/]+$', ''), '/*') AS prefix FROM GLOB('{}')",
            source
        );

        let mut stmt = conn.prepare(&glob_query)?;
        let prefixes: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<DuckResult<Vec<String>>>()?;

        Ok(prefixes)
    }

    pub fn parquet_files(&self) -> Vec<String> {
        lazy_static! {
            static ref PARQUET_RE: Regex = Regex::new(r"'([^']+\.parquet)'").unwrap();
        }

        let mut files = Vec::new();
        for table in self.tables() {
            let table_str = table.to_string();
            for cap in PARQUET_RE.captures_iter(&table_str) {
                files.push(cap[1].to_string());
            }
        }
        files
    }

    fn unify_query(query: &str) -> Result<String, QueryError> {
        // For now, we'll just return the original query
        // In a real implementation, you'd want to use a SQL formatter here
        Ok(query.to_string())
    }

    fn create_hash_string(s: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(s.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_select() {
        let query = "SELECT * FROM mytable";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        assert_eq!(parsed.tables().len(), 1);
    }

    #[test]
    fn test_select_with_where() {
        let query = "SELECT id, name FROM users WHERE age > 18";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        assert_eq!(parsed.tables().len(), 1);
    }

    #[test]
    fn test_select_with_join() {
        let query = "SELECT orders.id, customers.name FROM orders JOIN customers ON orders.customer_id = customers.id";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        assert_eq!(parsed.tables().len(), 2);
    }

    #[test]
    fn test_select_with_subquery() {
        let query = "SELECT * FROM (SELECT id FROM users WHERE active = true) AS active_users";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        // The outer query sees only one table (the subquery)
        assert_eq!(parsed.tables().len(), 1);
    }

    #[test]
    fn test_select_with_cte() {
        let query = "WITH active_users AS (SELECT id FROM users WHERE active = true) SELECT * FROM active_users";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        assert_eq!(parsed.tables().len(), 1);
    }

    #[test]
    fn test_select_with_union() {
        let query = "SELECT id FROM table1 UNION SELECT id FROM table2";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        // Our current implementation might not handle UNION correctly
        // This test might need adjustment based on how we decide to handle UNION
    }

    #[test]
    fn test_select_with_group_by_having() {
        let query = "SELECT country, AVG(age) FROM users GROUP BY country HAVING AVG(age) > 30";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        assert_eq!(parsed.tables().len(), 1);
    }

    #[test]
    fn test_select_with_order_by_limit() {
        let query = "SELECT * FROM products ORDER BY price DESC LIMIT 10";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        assert_eq!(parsed.tables().len(), 1);
    }

    #[test]
    fn test_select_with_window_function() {
        let query = "SELECT id, name, ROW_NUMBER() OVER (ORDER BY salary DESC) FROM employees";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        assert_eq!(parsed.tables().len(), 1);
    }

    #[test]
    fn test_select_with_case() {
        let query = "SELECT id, CASE WHEN age < 18 THEN 'Minor' ELSE 'Adult' END AS age_category FROM users";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        assert_eq!(parsed.tables().len(), 1);
    }

    #[test]
    fn test_select_with_casting() {
        let query = "SELECT CAST(price AS DECIMAL(10,2)) FROM products";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        assert_eq!(parsed.tables().len(), 1);
    }

    #[test]
    fn test_query_bucket() {
        let query = "SELECT * FROM READ_PARQUET('s3://my-bucket/data/*.parquet')";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.bucket().unwrap(), "s3://my-bucket");
    }

    #[test]
    fn test_select_from_parquet() {
        let query = "SELECT * FROM 's3://my-bucket/data/*.parquet'";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        assert_eq!(
            parsed.parquet_files(),
            vec!["s3://my-bucket/data/*.parquet"]
        );
        assert_eq!(parsed.bucket().unwrap(), "s3://my-bucket");
    }

    #[test]
    fn test_select_from_multiple_parquet() {
        let query = "SELECT * FROM 's3://bucket1/data1.parquet', 's3://bucket2/data2.parquet'";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        assert_eq!(
            parsed.parquet_files(),
            vec!["s3://bucket1/data1.parquet", "s3://bucket2/data2.parquet"]
        );
        assert_eq!(parsed.bucket().unwrap(), "s3://bucket1");
    }

    #[test]
    fn test_select_parquet_with_join() {
        let query = "SELECT o.id, c.name FROM 's3://bucket1/orders.parquet' o JOIN 's3://bucket2/customers.parquet' c ON o.customer_id = c.id";
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        assert_eq!(
            parsed.parquet_files(),
            vec![
                "s3://bucket1/orders.parquet",
                "s3://bucket2/customers.parquet"
            ]
        );
        assert_eq!(parsed.bucket().unwrap(), "s3://bucket1");
    }

    #[test]
    fn test_source_extraction() -> Result<(), QueryError> {
        let query = "SELECT * FROM 's3://my-bucket/data/*.parquet'";
        let parsed = QueryWrapper::parse(query)?;
        assert_eq!(parsed.source()?, "s3://my-bucket/data/*.parquet");
        Ok(())
    }

    #[test]
    fn test_complex_query() {
        let query = r#"
            WITH revenue AS (
                SELECT customer_id, SUM(amount) as total_revenue
                FROM orders
                GROUP BY customer_id
            )
            SELECT c.name, r.total_revenue
            FROM customers c
            JOIN revenue r ON c.id = r.customer_id
            WHERE r.total_revenue > 1000
            ORDER BY r.total_revenue DESC
            LIMIT 10
        "#
        .trim();
        let parsed = QueryWrapper::parse(query).unwrap();
        assert_eq!(parsed.sql, query);
        assert_eq!(parsed.tables().len(), 2);
    }
}
