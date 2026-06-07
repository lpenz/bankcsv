// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

use chrono::NaiveDate;

#[derive(Debug)]
pub struct Transaction {
    pub id: String,
    pub date: NaiveDate,
    pub description: String,
    pub value: String,
    pub src_account: String,
    pub account: Option<String>,
}
