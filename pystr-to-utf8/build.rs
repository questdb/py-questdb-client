#[cfg(feature = "cbindgen")]
extern crate cbindgen;

#[cfg(feature = "cbindgen")]
const BAD_PXD: &str = "
cdef extern from *:
  ctypedef bint bool
  ctypedef struct va_list";

#[cfg(feature = "cbindgen")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let bindings = cbindgen::generate(&crate_dir)?;
    bindings.write_to_file("include/pystr_to_utf8.h");

    let config = cbindgen::Config {
        language: cbindgen::Language::Cython,
        documentation: true,
        cython: cbindgen::CythonConfig {
            header: Some("\"pystr_to_utf8.h\"".to_owned()),
            cimports: std::collections::BTreeMap::new()},
        usize_is_size_t: true,
        ..Default::default()
    };

    let bindings = cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(config)
        .generate()?;

    // Instead of just writing out the file:
    //     bindings.write_to_file("include/pystr_to_utf8.pxd");
    // We need to do some post-processing to make it work our code.
    // The default output is too opinionated and has unwanted typedefs.
    let mut pxd = Vec::new();
    bindings.write(&mut pxd);
    let pxd = String::from_utf8(pxd)?;
    if !pxd.contains(BAD_PXD) {
        panic!("cbindgen generated unexpected pxd: {}", pxd);
    }
    let pxd = pxd.replace(BAD_PXD, "");
    let pxd = pxd.replace("bool", "bint");
    let pxd = pxd.replace(";", "");
    // println!("{}", &pxd);
    std::fs::write("../src/questdb/pystr_to_utf8.pxd", &pxd)?;
    Ok(())
}

#[cfg(not(feature = "cbindgen"))]
fn main() {}

