// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Source account
    pub src_account: String,

    /// JSON config file
    pub config_file: PathBuf,

    /// Input bank CSV files
    pub inputs: Vec<PathBuf>,

    /// Output file
    #[arg(short, long, default_value = "-")]
    pub output: String,
}
