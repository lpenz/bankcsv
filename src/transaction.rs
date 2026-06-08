// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

//! Transaction data structures.

use chrono::NaiveDate;

/// A single bank transaction.
#[derive(Debug)]
pub struct Transaction {
    /// Unique identifier for the transaction.
    pub id: String,
    /// The date the transaction occurred.
    pub date: NaiveDate,
    /// Description of the transaction from the bank.
    pub description: String,
    /// The value of the transaction as a string (to preserve precision).
    pub value: String,
    /// The source account (e.g., the bank account the CSV came from).
    pub src_account: String,
    /// The destination account, if identified.
    pub account: Option<String>,
}
