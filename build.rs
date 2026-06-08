// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

use clap::CommandFactory;
use clap_complete::generate_to;
use clap_complete::shells::Bash;
use clap_complete::shells::Fish;
use clap_complete::shells::Zsh;
use color_eyre::{Result, eyre::eyre};
use man::prelude::*;
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::Write;
use std::path;

include!("src/cli.rs");

fn generate_man_page<P: AsRef<path::Path>>(outdir: P) -> Result<()> {
    let outdir = outdir.as_ref();
    let man_path = outdir.join("bankcsv.1");
    let cmd = Args::command();
    let manpage: Manual = clap2man::Manual::try_from(&cmd)
        .map_err(|e| eyre!(e))?
        .into();
    let manpage = manpage
        .example(
            Example::new()
                .text("Convert a bank CSV file")
                .command("bankcsv Liabilities:CreditCard config.json input.csv"),
        )
        .render();
    File::create(man_path)?.write_all(manpage.as_bytes())?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    color_eyre::install()?;
    let mut outdir =
        path::PathBuf::from(env::var_os("OUT_DIR").ok_or_else(|| eyre!("error getting OUT_DIR"))?);
    fs::create_dir_all(&outdir)?;
    generate_man_page(&outdir)?;
    // build/bankcsv-*/out
    outdir.pop();
    // build/bankcsv-*
    outdir.pop();
    // build
    outdir.pop();
    // .
    generate_man_page(&outdir)?;
    // Generate shell completions:
    let mut cmd = Args::command();
    generate_to(Bash, &mut cmd, "bankcsv", &outdir)?;
    generate_to(Fish, &mut cmd, "bankcsv", &outdir)?;
    generate_to(Zsh, &mut cmd, "bankcsv", &outdir)?;
    Ok(())
}
