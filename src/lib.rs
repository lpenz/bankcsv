// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

mod accounts;
mod cli;
mod csvparser;
mod output;
mod transaction;

use clap::Parser;
use color_eyre::eyre::Result;
use std::fs::File;
use std::io::{self, Write};

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    color_eyre::install()?;
    let args = cli::Args::parse();

    // Read config
    let accounts = accounts::Accounts::from_file(&args.config_file)?;

    // Prepare output
    let writer: Box<dyn Write> = if args.output == "-" {
        Box::new(io::stdout())
    } else {
        Box::new(File::create(&args.output)?)
    };

    let mut output = output::Output::new(writer)?;

    let parser = csvparser::Parser::new(args.src_account.clone());

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
