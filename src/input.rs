// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

//! Bank CSV file parser.

use crate::transaction::Transaction;
use chrono::{Datelike, NaiveDate};
use color_eyre::eyre::{Result, eyre};
use csv::ReaderBuilder;

/// Parser for bank CSV files.
pub struct Parser {
    src_account: String,
}

impl Parser {
    /// Creates a new Parser for the given source account.
    pub fn new(src_account: String) -> Self {
        Self { src_account }
    }

    /// Parses the given CSV content into an iterator of Transactions.
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

/// Helper function to parse the transaction value from a CSV record.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_credit() -> Result<()> {
        let content = "Masked Card Number, Transaction date/time, Processed, Description, Paid out, Paid in, Posted Currency, Transaction Type, Category
0000 **** **** 0000,\"00:00, 11/01/2026\",\"12/01/2026\",\"DIRECT DEBIT\",\"\",\"2113.05\",\"EUR\",\"Bill Payment\",\"\"
0000 **** **** 0000,\"12:10, 08/01/2026\",\"09/01/2026\",\"MERCHANT 1\",\"9.95\",\"\",\"EUR\",\"Purchase\",\"Leisure & Entertainment\"";
        let parser = Parser::new("test_account".to_string());
        let transactions: Vec<_> = parser.parse_str(content)?.collect::<Result<Vec<_>>>()?;

        assert_eq!(transactions.len(), 2);

        assert_eq!(
            transactions[0].date,
            NaiveDate::from_ymd_opt(2026, 1, 12).unwrap()
        );
        assert_eq!(transactions[0].description, "DIRECT DEBIT");
        assert_eq!(transactions[0].value, "-2113.05");

        assert_eq!(
            transactions[1].date,
            NaiveDate::from_ymd_opt(2026, 1, 9).unwrap()
        );
        assert_eq!(transactions[1].description, "MERCHANT 1");
        assert_eq!(transactions[1].value, "9.95");

        Ok(())
    }

    #[test]
    fn test_parse_debit() -> Result<()> {
        let content = "Posted Account, Posted Transactions Date, Description1, Description2, Description3, Debit Amount, Credit Amount,Balance,Posted Currency,Transaction Type,Local Currency Amount,Local Currency
\"000000 - 00000000\",\"03/03/2025\",\"MERCHANT 2\",\"\",\"\",\"50.00\",,\"20302.92\",EUR,\"Debit\",\" 50.00\",EUR
\"000000 - 00000000\",\"26/03/2025\",\"ZZ00000000000000\",\"PERSON NAME\",\"\",,\"14,649.77\",\"19519.71\",EUR,\"Credit\",\" 14,649.77\",EUR";
        let parser = Parser::new("test_account".to_string());
        let transactions: Vec<_> = parser.parse_str(content)?.collect::<Result<Vec<_>>>()?;

        assert_eq!(transactions.len(), 2);

        assert_eq!(
            transactions[0].date,
            NaiveDate::from_ymd_opt(2025, 3, 3).unwrap()
        );
        assert_eq!(transactions[0].description, "MERCHANT 2");
        assert_eq!(transactions[0].value, "50.00");

        assert_eq!(
            transactions[1].date,
            NaiveDate::from_ymd_opt(2025, 3, 26).unwrap()
        );
        assert_eq!(transactions[1].description, "ZZ00000000000000");
        assert_eq!(transactions[1].value, "-14,649.77");

        Ok(())
    }
}
