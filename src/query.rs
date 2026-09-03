use std::path::Path;

use anyhow::{Context, Result, bail};
use base64::Engine;
use rusqlite::{Connection, OpenFlags, types::ValueRef};
use serde::Serialize;
use serde_json::Value;

use crate::db::{STATES_DB, TIMELINE_DB};

#[derive(Debug, Serialize)]
pub struct QueryOutput {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

pub fn query_database(path: &Path, description: &str, sql: &str) -> Result<QueryOutput> {
    if !path.is_file() {
        bail!("{description} database `{}` does not exist", path.display());
    }
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("failed to open `{}` read-only", path.display()))?;
    connection.pragma_update(None, "query_only", true)?;
    execute(&connection, sql)
}

pub fn query_system(root: &Path, sql: &str) -> Result<QueryOutput> {
    let timeline = root.join(TIMELINE_DB);
    let states = root.join(STATES_DB);
    for (description, path) in [("timeline", &timeline), ("states", &states)] {
        if !path.is_file() {
            bail!("{description} database `{}` does not exist", path.display());
        }
    }

    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_URI;
    let connection = Connection::open_with_flags(":memory:", flags)?;
    connection.execute("ATTACH DATABASE ?1 AS timeline", [readonly_uri(&timeline)?])?;
    connection.execute("ATTACH DATABASE ?1 AS states", [readonly_uri(&states)?])?;
    connection.pragma_update(None, "query_only", true)?;
    execute(&connection, sql)
}

fn readonly_uri(path: &Path) -> Result<String> {
    let path = path.canonicalize()?;
    let mut encoded = String::new();
    for byte in path.to_string_lossy().bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    Ok(format!("file:{encoded}?mode=ro"))
}

fn execute(connection: &Connection, sql: &str) -> Result<QueryOutput> {
    let mut statement = connection.prepare(sql).context("invalid query")?;
    let columns = statement
        .column_names()
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    let column_count = statement.column_count();
    let rows = statement
        .query_map([], |row| {
            (0..column_count)
                .map(|index| match row.get_ref(index)? {
                    ValueRef::Null => Ok(Value::Null),
                    ValueRef::Integer(value) => Ok(Value::from(value)),
                    ValueRef::Real(value) => Ok(Value::from(value)),
                    ValueRef::Text(value) => {
                        Ok(Value::String(String::from_utf8_lossy(value).into_owned()))
                    }
                    ValueRef::Blob(value) => Ok(serde_json::json!({
                        "base64": base64::engine::general_purpose::STANDARD.encode(value)
                    })),
                })
                .collect::<rusqlite::Result<Vec<_>>>()
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(QueryOutput { columns, rows })
}
