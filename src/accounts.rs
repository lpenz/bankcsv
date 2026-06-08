// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

use color_eyre::eyre::Result;
use regex::Regex;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct Config {
    #[serde(rename = "AccountFromDescription")]
    account_from_description: Vec<AccountFromDescription>,
}

#[derive(Debug, Deserialize)]
struct AccountFromDescription {
    #[serde(rename = "Account")]
    account: String,
    #[serde(rename = "Regex")]
    regex: String,
}

pub struct Accounts {
    pub rules: Vec<(Regex, String)>,
}

impl Accounts {
    pub fn from_file(path: &Path) -> Result<Self> {
        let config_content = std::fs::read_to_string(path)?;
        Self::from_str(&config_content)
    }

    pub fn from_str(content: &str) -> Result<Self> {
        let config: Config = serde_json::from_str(content)?;

        let mut rules = Vec::new();
        for rule in config.account_from_description {
            let re = Regex::new(&rule.regex)?;
            rules.push((re, rule.account));
        }
        Ok(Self { rules })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accounts_from_str() -> Result<()> {
        let content = r#"{
            "AccountFromDescription": [
                {
                    "Account": "Account1",
                    "Regex": "Regex1"
                },
                {
                    "Account": "Account2",
                    "Regex": "Regex2"
                }
            ]
        }"#;
        let accounts = Accounts::from_str(content)?;
        assert_eq!(accounts.rules.len(), 2);
        assert_eq!(accounts.rules[0].1, "Account1");
        assert!(accounts.rules[0].0.is_match("Regex1"));
        assert_eq!(accounts.rules[1].1, "Account2");
        assert!(accounts.rules[1].0.is_match("Regex2"));
        Ok(())
    }

    #[test]
    fn test_accounts_from_str_invalid_json() {
        let content = r#"{"invalid": "json"}"#;
        let result = Accounts::from_str(content);
        assert!(result.is_err());
    }

    #[test]
    fn test_accounts_from_str_invalid_regex() {
        let content = r#"{
            "AccountFromDescription": [
                {
                    "Account": "Account1",
                    "Regex": "["
                }
            ]
        }"#;
        let result = Accounts::from_str(content);
        assert!(result.is_err());
    }
}
