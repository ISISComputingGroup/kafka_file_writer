use std::process::Command;

fn main() {
    println!(
        "cargo:rustc-env=BUILD_HOST={}",
        std::env::var("HOST").unwrap_or_default()
    );
    println!("cargo:rerun-if-env-changed=HOST");

    println!(
        "cargo:rustc-env=BUILD_TARGET={}",
        std::env::var("TARGET").unwrap_or_default()
    );
    println!("cargo:rerun-if-env-changed=TARGET");

    println!(
        "cargo:rustc-env=BUILD_DEBUG={}",
        std::env::var("DEBUG").unwrap_or_default()
    );
    println!("cargo:rerun-if-env-changed=DEBUG");

    println!(
        "cargo:rustc-env=BUILD_OPT_LEVEL={}",
        std::env::var("OPT_LEVEL").unwrap_or_default()
    );
    println!("cargo:rerun-if-env-changed=OPT_LEVEL");

    let version = std::env::var("RUSTC")
        .ok()
        .and_then(|rustc| Command::new(rustc).args(["--version"]).output().ok())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or("unknown".to_string());
    println!("cargo:rustc-env=BUILD_RUSTC={}", version.trim());
    println!("cargo:rerun-if-env-changed=RUSTC");

    let git_description = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .filter(|s| !s.is_empty())
        .unwrap_or("unknown".to_owned());
    println!("cargo:rustc-env=GIT_VERSION={}", git_description.trim());
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
}
