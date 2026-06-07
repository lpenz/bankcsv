// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

mod csvparser;
mod transaction;

use clap::Parser;
use color_eyre::eyre::Result;
use csv::WriterBuilder;
use regex::Regex;
use serde::Deserialize;
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Source account
    src_account: String,

    /// JSON config file
    config_file: PathBuf,

    /// Input bank CSV files
    inputs: Vec<PathBuf>,

    /// Output file
    #[arg(short, long, default_value = "-")]
    output: String,
}

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

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    color_eyre::install()?;
    let args = Args::parse();

    // Read config
    let config_content = std::fs::read_to_string(&args.config_file)?;
    let config: Config = serde_json::from_str(&config_content)?;

    // Compile regexes
    let mut rules = Vec::new();
    for rule in config.account_from_description {
        let re = Regex::new(&rule.regex)?;
        rules.push((re, rule.account));
    }

    // Prepare output
    let mut writer: Box<dyn Write> = if args.output == "-" {
        Box::new(io::stdout())
    } else {
        Box::new(File::create(&args.output)?)
    };

    let mut csv_writer = WriterBuilder::new()
        .quote_style(csv::QuoteStyle::Always)
        .from_writer(writer.as_mut());

    // Write header
    csv_writer.write_record(["id", "date", "description", "amount", "value", "account"])?;

    let parser = csvparser::Parser::new(args.src_account.clone());

    for input_path in args.inputs {
        let content = std::fs::read_to_string(&input_path)?;
        for result in parser.parse_str(&content)? {
            let mut t = result?;

            for (re, account) in &rules {
                if re.is_match(&t.description) {
                    t.account = Some(account.clone());
                    break;
                }
            }

            if let Some(ref account) = t.account {
                let date_fmt = t.date.format("%Y-%m-%d").to_string();

                // Add src record
                csv_writer.write_record([
                    &t.id,
                    &date_fmt,
                    &t.description,
                    &t.value,
                    &t.value,
                    &t.src_account,
                ])?;

                // Add dst record
                let dst_value = if t.value.starts_with('-') {
                    t.value[1..].to_string()
                } else {
                    format!("-{}", t.value)
                };

                csv_writer.write_record([
                    &t.id,
                    &date_fmt,
                    &t.description,
                    &dst_value,
                    &dst_value,
                    account,
                ])?;
            } else {
                eprintln!("could not assign account to {}", t.description);
            }
        }
    }

    csv_writer.flush()?;
    Ok(())
}
