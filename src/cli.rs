// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

//! Command-line interface definition.

use clap::{Parser, ValueHint};
use std::path::PathBuf;

/// Convert bank CSV files to GnuCash-compatible format using JSON-based rules
#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about,
    long_about = "bankcsv processes bank transaction exports and generates a CSV format suitable for GnuCash import. It leverages a JSON configuration file to map transaction descriptions to specific accounts using regular expressions, enabling automated categorization of your financial data."
)]
pub struct Args {
    /// Source account
    pub src_account: String,

    /// JSON config file
    #[arg(value_hint = ValueHint::FilePath)]
    pub config_file: PathBuf,

    /// Input bank CSV files
    #[arg(value_hint = ValueHint::FilePath)]
    pub inputs: Vec<PathBuf>,

    /// Output file
    #[arg(short, long, default_value = "-", value_hint = ValueHint::FilePath)]
    pub output: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_args() {
        let args =
            Args::try_parse_from(["bankcsv", "src_acc", "config.json", "in1.csv", "in2.csv"])
                .unwrap();
        assert_eq!(args.src_account, "src_acc");
        assert_eq!(args.config_file, PathBuf::from("config.json"));
        assert_eq!(
            args.inputs,
            vec![PathBuf::from("in1.csv"), PathBuf::from("in2.csv")]
        );
        assert_eq!(args.output, "-");
    }

    #[test]
    fn test_parse_args_with_output() {
        let args = Args::try_parse_from([
            "bankcsv",
            "--output",
            "out.csv",
            "src_acc",
            "config.json",
            "in1.csv",
        ])
        .unwrap();
        assert_eq!(args.output, "out.csv");
    }

    #[test]
    fn test_parse_args_missing_required() {
        let result = Args::try_parse_from(["bankcsv", "src_acc"]);
        assert!(result.is_err());
    }
}
