use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub const CONFIG_FILE: &str = ".labflow/config";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub port: u16,
}

impl Config {
    pub fn load(root: &Path) -> Result<Self> {
        let path = root.join(CONFIG_FILE);
        let source = fs::read_to_string(&path)
            .with_context(|| format!("failed to read `{}`", path.display()))?;
        let config: Self =
            toml::from_str(&source).with_context(|| format!("invalid `{}`", path.display()))?;
        anyhow::ensure!(config.port != 0, "port must be non-zero");
        Ok(config)
    }
}
