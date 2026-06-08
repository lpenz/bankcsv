// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

use crate::transaction::Transaction;
use color_eyre::eyre::Result;
use csv::Writer;
use std::io::Write;

pub struct Output<W: Write> {
    csv_writer: Writer<W>,
}

impl<W: Write> Output<W> {
    pub fn new(writer: W) -> Result<Self> {
        let mut csv_writer = csv::WriterBuilder::new()
            .quote_style(csv::QuoteStyle::Always)
            .from_writer(writer);
        csv_writer.write_record(["id", "date", "description", "amount", "value", "account"])?;
        Ok(Self { csv_writer })
    }

    pub fn write_transaction(&mut self, t: &Transaction) -> Result<()> {
        let account = t
            .account
            .as_ref()
            .ok_or_else(|| color_eyre::eyre::eyre!("transaction has no account"))?;
        let date_fmt = t.date.format("%Y-%m-%d").to_string();

        // Add src record
        self.csv_writer.write_record([
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

        self.csv_writer.write_record([
            &t.id,
            &date_fmt,
            &t.description,
            &dst_value,
            &dst_value,
            account,
        ])?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.csv_writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::Transaction;
    use chrono::NaiveDate;

    fn check(t: Transaction, expected: &str) -> Result<()> {
        let mut buffer = Vec::new();
        {
            let mut output = Output::new(&mut buffer)?;
            output.write_transaction(&t)?;
            output.flush()?;
        }
        let result = String::from_utf8(buffer)?;
        let mut full_expected =
            String::from("\"id\",\"date\",\"description\",\"amount\",\"value\",\"account\"\n");
        full_expected.push_str(expected);
        assert_eq!(result, full_expected);
        Ok(())
    }

    #[test]
    fn test_output_write_transaction() -> Result<()> {
        let t = Transaction {
            id: "2026011201".to_string(),
            date: NaiveDate::from_ymd_opt(2026, 1, 12).unwrap(),
            description: "TEST TRANSACTION".to_string(),
            value: "100.00".to_string(),
            src_account: "SRC_ACC".to_string(),
            account: Some("DST_ACC".to_string()),
        };
        check(
            t,
            "\
\"2026011201\",\"2026-01-12\",\"TEST TRANSACTION\",\"100.00\",\"100.00\",\"SRC_ACC\"\n\
\"2026011201\",\"2026-01-12\",\"TEST TRANSACTION\",\"-100.00\",\"-100.00\",\"DST_ACC\"\n",
        )
    }

    #[test]
    fn test_output_write_transaction_negative() -> Result<()> {
        let t = Transaction {
            id: "2026011201".to_string(),
            date: NaiveDate::from_ymd_opt(2026, 1, 12).unwrap(),
            description: "TEST TRANSACTION".to_string(),
            value: "-50.00".to_string(),
            src_account: "SRC_ACC".to_string(),
            account: Some("DST_ACC".to_string()),
        };
        check(
            t,
            "\
\"2026011201\",\"2026-01-12\",\"TEST TRANSACTION\",\"-50.00\",\"-50.00\",\"SRC_ACC\"\n\
\"2026011201\",\"2026-01-12\",\"TEST TRANSACTION\",\"50.00\",\"50.00\",\"DST_ACC\"\n",
        )
    }
}
