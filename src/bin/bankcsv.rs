// Copyright (C) 2026 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

//! Binary entry point for bankcsv.

/// Main function for the bankcsv binary.
/// Delegates execution to the library's main function.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    bankcsv::main()
}
