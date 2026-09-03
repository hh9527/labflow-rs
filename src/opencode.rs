use std::path::Path;

use reqwest::Client;
use serde_json::Value;

pub async fn turn_messages(
    client: &Client,
    backend_url: &str,
    root: &Path,
    session_id: &str,
    parent_id: &str,
    limit: Option<usize>,
) -> anyhow::Result<Vec<Value>> {
    let mut request = client
        .get(format!("{backend_url}/session/{session_id}/message"))
        .query(&[("directory", root.to_string_lossy().as_ref())]);
    if let Some(limit) = limit {
        request = request.query(&[("limit", limit)]);
    }
    let mut messages = request
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<Value>>()
        .await?;
    messages.retain(|message| {
        message["info"]["role"] == "assistant"
            && message["info"]["parentID"].as_str() == Some(parent_id)
    });
    messages.sort_by_key(|message| message["info"]["time"]["created"].as_i64());
    Ok(messages)
}

pub async fn turn_parts(
    client: &Client,
    backend_url: &str,
    root: &Path,
    session_id: &str,
    response: &Value,
    limit: Option<usize>,
) -> Vec<Value> {
    let fallback = response["parts"].as_array().cloned().unwrap_or_default();
    let Some(parent_id) = response["info"]["parentID"].as_str() else {
        return fallback;
    };

    let Ok(messages) = turn_messages(client, backend_url, root, session_id, parent_id, limit).await
    else {
        return fallback;
    };
    let parts = messages
        .into_iter()
        .flat_map(|message| message["parts"].as_array().cloned().unwrap_or_default())
        .collect::<Vec<_>>();
    if parts.is_empty() { fallback } else { parts }
}
