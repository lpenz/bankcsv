// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

use crate::transaction::Transaction;
use chrono::{Datelike, NaiveDate};
use color_eyre::eyre::{Result, eyre};
use csv::ReaderBuilder;

pub struct Parser {
    src_account: String,
}

impl Parser {
    pub fn new(src_account: String) -> Self {
        Self { src_account }
    }

    pub fn parse_str<'a>(
        &'a self,
        content: &'a str,
    ) -> Result<impl Iterator<Item = Result<Transaction>> + 'a> {
        let rdr = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(content.as_bytes());

        let mut records = rdr.into_records();

        let first_record = records.next().ok_or_else(|| eyre!("empty input"))??;

        let is_credit = match first_record.get(0) {
            Some("Masked Card Number") => true,
            Some("Posted Account") => false,
            _ => return Err(eyre!("unknown input format")),
        };

        let mut last_date: Option<NaiveDate> = None;
        let mut counter = 1;

        Ok(records.map(move |result| {
            let record = result?;
            let offset = if is_credit { 1 } else { 0 };
            let date_str = record
                .get(1 + offset)
                .ok_or_else(|| eyre!("missing date field"))?;

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

            counter += 1;

            Ok(Transaction {
                id,
                date,
                description,
                value,
                src_account: self.src_account.clone(),
                account: None,
            })
        }))
    }
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
