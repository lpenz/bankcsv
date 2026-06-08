// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

//! Main library module for bankcsv.

mod accounts;
mod cli;
mod input;
mod output;
mod transaction;

use clap::Parser;
use color_eyre::eyre::Result;
use std::fs::File;
use std::io::{self, Write};

/// Entry point for the bankcsv application.
/// Initializes error handling, parses CLI arguments, and starts the processing.
pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    color_eyre::install()?;
    let args = cli::Args::parse();
    bankcsv(args)?;
    Ok(())
}

/// Core logic of bankcsv.
/// Processes input CSV files using the provided configuration and writes the output.
pub fn bankcsv(args: cli::Args) -> Result<()> {
    // Read config
    let accounts = accounts::Accounts::from_file(&args.config_file)?;

    // Prepare output
    let writer: Box<dyn Write> = if args.output == "-" {
        Box::new(io::stdout())
    } else {
        Box::new(File::create(&args.output)?)
    };

    let mut output = output::Output::new(writer)?;

    let parser = input::Parser::new(args.src_account.clone());

    for input_path in args.inputs {
        let content = std::fs::read_to_string(&input_path)?;
        for result in parser.parse_str(&content)? {
            let mut t = result?;

            for (re, account) in &accounts.rules {
                if re.is_match(&t.description) {
                    t.account = Some(account.clone());
                    break;
                }
            }

            if t.account.is_some() {
                output.write_transaction(&t)?;
            } else {
                eprintln!("could not assign account to {}", t.description);
            }
        }
    }

    output.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_bankcsv_integration() -> Result<(), Box<dyn std::error::Error>> {
        let dir = std::env::temp_dir().join("bankcsv_test_integration");
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
        }
        fs::create_dir_all(&dir)?;

        let config_file = dir.join("config.json");
        fs::write(
            &config_file,
            r#"{
            "AccountFromDescription": [
                { "Account": "Assets:Checking", "Regex": "MERCHANT 1" }
            ]
        }"#,
        )?;

        let input_file = dir.join("input.csv");
        fs::write(
            &input_file,
            "Masked Card Number, Transaction date/time, Processed, Description, Paid out, Paid in, Posted Currency, Transaction Type, Category
0000 **** **** 0000,\"12:10, 08/01/2026\",\"09/01/2026\",\"MERCHANT 1\",\"9.95\",\"\",\"EUR\",\"Purchase\",\"Leisure & Entertainment\"",
        )?;

        let output_file = dir.join("output.csv");

        let args = cli::Args {
            src_account: "Liabilities:CreditCard".to_string(),
            config_file: config_file.clone(),
            inputs: vec![input_file.clone()],
            output: output_file.to_str().unwrap().to_string(),
        };

        bankcsv(args)?;

        let output_content = fs::read_to_string(&output_file)?;
        assert!(output_content.contains(
            "\"2026010901\",\"2026-01-09\",\"MERCHANT 1\",\"9.95\",\"9.95\",\"Liabilities:CreditCard\""
        ));
        assert!(output_content.contains(
            "\"2026010901\",\"2026-01-09\",\"MERCHANT 1\",\"-9.95\",\"-9.95\",\"Assets:Checking\""
        ));

        fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
