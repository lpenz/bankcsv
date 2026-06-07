// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

use anyhow::{Result, anyhow};
use chrono::{Datelike, NaiveDate};
use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use regex::Regex;
use serde::Deserialize;
use std::fs::File;
use std::io::{self, BufReader, Write};
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

struct Transaction {
    id: String,
    date: NaiveDate,
    description: String,
    value: String,
    src_account: String,
    account: Option<String>,
}

fn value_parse(record: &csv::StringRecord, is_credit: bool) -> String {
    let val = if is_credit {
        record.get(4).unwrap_or("").trim()
    } else {
        record.get(5).unwrap_or("").trim()
    };

    if val == "0.00" || val.is_empty() {
        if is_credit {
            format!("-{}", record.get(5).unwrap_or("").trim())
        } else {
            format!("-{}", record.get(6).unwrap_or("").trim())
        }
    } else {
        val.to_string()
    }
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    let mut last_date: Option<NaiveDate> = None;
    let mut counter = 1;

    for input_path in args.inputs {
        let file = File::open(&input_path)?;
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(BufReader::new(file));

        let mut is_credit = false;
        let mut first_line = true;

        for result in rdr.records() {
            let record = result?;
            if first_line {
                first_line = false;
                match record.get(0) {
                    Some("Masked Card Number") => is_credit = true,
                    Some("Posted Account") => is_credit = false,
                    _ => return Err(anyhow!("unknown input format for {:?}", input_path).into()),
                }
                continue;
            }

            let offset = if is_credit { 1 } else { 0 };
            let date_str = record
                .get(1 + offset)
                .ok_or_else(|| anyhow!("missing date field"))?;

            // Go's time.Parse("02/01/2006", line)
            // But wait, credit-cur.csv has "13:43, 28/08/2025" in column 1.
            // Go code uses line[1+offset].
            // For credit, offset=1, so line[2].
            // line[2] in credit-cur.csv is "Processed" date: "29/08/2025".
            // For debit, offset=0, so line[1].
            // line[1] in debit-cur.csv is "Posted Transactions Date": "01/07/2025".

            let date = NaiveDate::parse_from_str(date_str, "%d/%m/%Y")?;

            if Some(date) != last_date {
                counter = 1;
                last_date = Some(date);
            }

            let id = format!(
                "{}{:02}{:02}{:02}",
                date.year(),
                date.month(),
                date.day(),
                counter
            );
            let description = record.get(2 + offset).unwrap_or("").to_string();
            let value = value_parse(&record, is_credit);

            let mut t = Transaction {
                id,
                date,
                description,
                value,
                src_account: args.src_account.clone(),
                account: None,
            };

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

            counter += 1;
        }
    }

    csv_writer.flush()?;
    Ok(())
}
