use std::path::Path;

use reqwest::Client;
use serde_json::Value;

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

    let mut request = client
        .get(format!("{backend_url}/session/{session_id}/message"))
        .query(&[("directory", root.to_string_lossy().as_ref())]);
    if let Some(limit) = limit {
        request = request.query(&[("limit", limit)]);
    }
    let Ok(response) = request.send().await else {
        return fallback;
    };
    let Ok(response) = response.error_for_status() else {
        return fallback;
    };
    let Ok(mut messages) = response.json::<Vec<Value>>().await else {
        return fallback;
    };
    messages.retain(|message| {
        message["info"]["role"] == "assistant"
            && message["info"]["parentID"].as_str() == Some(parent_id)
    });
    messages.sort_by_key(|message| message["info"]["time"]["created"].as_i64());
    let parts = messages
        .into_iter()
        .flat_map(|message| message["parts"].as_array().cloned().unwrap_or_default())
        .collect::<Vec<_>>();
    if parts.is_empty() { fallback } else { parts }
}
