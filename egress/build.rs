use std::env;

fn main() {
    let target = env::var("TARGET").unwrap();
    if target.contains("apple-darwin") {
        println!("cargo:rustc-link-arg=-Wl,-undefined");
        println!("cargo:rustc-link-arg=-Wl,dynamic_lookup");
    }
}