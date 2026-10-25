//! SQL front-end: ``headers.`x` `` becomes `header(headers, 'x')`, and only read-only
//! statements are accepted.
//!
//! User SQL is parsed with the MySQL dialect (backtick identifiers, and `offset` /
//! `partition` / `timestamp` usable as plain column names). Unquoted column identifiers
//! are emitted double-quoted and lower-cased, so DataFusion's own parser reads them the same.

use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{Expr, Statement, visit_expressions_mut};
use datafusion::sql::sqlparser::dialect::{GenericDialect, MySqlDialect};
use datafusion::sql::sqlparser::parser::Parser;

use crate::error::{Error, Result};

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn header_call(qualifier: &[String], key: &str) -> Result<Expr> {
    let column = if qualifier.is_empty() {
        "headers".to_owned()
    } else {
        format!("{}.headers", qualifier.join("."))
    };
    let text = format!("header({column}, {})", quote_literal(key));
    Parser::new(&GenericDialect {})
        .try_with_sql(&text)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e| Error::Sql(e.to_string()))
}

/// Rewrites the user's SQL into plain DataFusion SQL.
pub fn rewrite(sql: &str) -> Result<String> {
    let mut statements =
        Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| Error::Sql(e.to_string()))?;
    if statements.len() != 1 {
        return Err(Error::Sql("exactly one statement expected".into()));
    }
    match &statements[0] {
        Statement::Query(_) | Statement::Explain { .. } => {}
        _ => return Err(Error::Sql("only SELECT queries are supported".into())),
    }
    let mut failure = None;
    let _ = visit_expressions_mut(&mut statements, |expr| {
        if let Expr::Identifier(ident) = expr
            && ident.quote_style.is_none()
        {
            ident.value = ident.value.to_lowercase();
            ident.quote_style = Some('"');
        }
        if let Expr::CompoundIdentifier(parts) = expr {
            let n = parts.len();
            if n >= 2 && parts[n - 2].value.eq_ignore_ascii_case("headers") {
                let qualifier: Vec<String> = parts[..n - 2].iter().map(|p| p.to_string()).collect();
                match header_call(&qualifier, &parts[n - 1].value) {
                    Ok(call) => *expr = call,
                    Err(e) => {
                        failure = Some(e);
                        return ControlFlow::Break(());
                    }
                }
            } else {
                for p in parts.iter_mut().filter(|p| p.quote_style.is_none()) {
                    p.value = p.value.to_lowercase();
                    p.quote_style = Some('"');
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
    if let Some(e) = failure {
        return Err(e);
    }
    Ok(statements[0].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_header_access() {
        let out = rewrite(
            "SELECT topic, headers.`correlation-id` FROM topics \
             WHERE headers.`correlation-id` = '632543-gdsf2' AND t.headers.source IN ('a', 'b')",
        )
        .unwrap();
        assert_eq!(
            out,
            "SELECT \"topic\", header(headers, 'correlation-id') FROM topics \
             WHERE header(headers, 'correlation-id') = '632543-gdsf2' \
             AND header(t.headers, 'source') IN ('a', 'b')"
        );
        assert_eq!(
            rewrite("SELECT topic, Offset FROM topics ORDER BY offset LIMIT 1 OFFSET 2").unwrap(),
            "SELECT \"topic\", \"offset\" FROM topics ORDER BY \"offset\" LIMIT 1 OFFSET 2"
        );
        assert!(
            rewrite("SELECT headers.`it''s` FROM topics")
                .unwrap()
                .contains("'it''s'")
        );
    }

    #[test]
    fn rejects_writes() {
        assert!(rewrite("DROP TABLE topics").is_err());
        assert!(rewrite("INSERT INTO topics VALUES (1)").is_err());
        assert!(rewrite("SELECT 1; SELECT 2").is_err());
    }
}
