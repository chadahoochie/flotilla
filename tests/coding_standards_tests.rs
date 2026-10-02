//! Coding Standards Enforcement Tests.
//!
//! Enforces:
//! 1. No Private Helper Methods:
//!    Inherent `impl` blocks must NEVER contain private methods (`fn helper(&self)`).
//!    All algorithmic routines must be decomposed into crate-visible standalone pure functions,
//!    traits, or dedicated types for 100% isolated unit testability.
//! 2. Type-per-File Decomposition:
//!    Every non-exempt file must define at most one primary public type (`pub struct`, `pub enum`, `pub trait`).
//! 3. Zero-Allocation Hot Path Test Verification:
//!    Confirms the tracking allocator test suite exists and enforces 0 heap allocations.

use std::fs;
use std::path::{Path, PathBuf};

const EXEMPT_FILES: &[&str] = &[
    "mod.rs",
    "lib.rs",
    "rules.rs",
    "evaluator.rs",
    "framing.rs",
    "packets.rs",
    "sinks.rs",
    "channel.rs",
    "commit.rs",
    "flotilla-server.rs",
];

fn collect_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_rs_files(&path, files);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path);
            }
        }
    }
}

#[test]
fn test_no_private_helper_methods_in_impl_blocks() {
    let src_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rs_files(&src_dir, &mut files);
    assert!(!files.is_empty(), "src directory must contain .rs files");

    let mut violations = Vec::new();

    for path in &files {
        let content = fs::read_to_string(path).expect("Failed to read file");
        let lines: Vec<&str> = content.lines().collect();

        let mut in_impl = false;
        let mut is_trait_impl = false;
        let mut brace_level = 0;
        let mut impl_type = String::new();

        for (line_idx, line) in lines.iter().enumerate() {
            let clean = line.split("//").next().unwrap_or("").trim();
            if clean.is_empty() {
                continue;
            }

            // Detect impl block start
            if clean.starts_with("impl") && clean.contains('{') && !in_impl {
                in_impl = true;
                brace_level = 0;
                is_trait_impl = clean.contains(" for ");
                impl_type = clean.to_string();
            }

            if in_impl {
                brace_level += clean.matches('{').count() as i32;
                brace_level -= clean.matches('}').count() as i32;

                if brace_level <= 0 {
                    in_impl = false;
                    is_trait_impl = false;
                    impl_type.clear();
                    continue;
                }

                // Inside inherent impl block (brace_level == 1)
                if !is_trait_impl && brace_level == 1 {
                    // Check for fn definition that does not start with pub
                    let is_fn_def = clean.starts_with("fn ")
                        || clean.starts_with("async fn ")
                        || clean.starts_with("const fn ")
                        || clean.starts_with("unsafe fn ")
                        || clean.starts_with("extern fn ");

                    let is_pub = clean.starts_with("pub ")
                        || clean.starts_with("pub(")
                        || clean.starts_with("pub(crate)")
                        || clean.starts_with("pub(super)");

                    if is_fn_def && !is_pub {
                        violations.push(format!(
                            "{}:{}: private method found in '{}': '{}'. Flotilla strictly prohibits private helper methods. Decompose into a standalone crate-visible pure function or dedicated type.",
                            path.display(),
                            line_idx + 1,
                            impl_type,
                            clean
                        ));
                    }
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "Violations of 'No Private Helper Methods' detected:\n{}",
        violations.join("\n")
    );
}

#[test]
fn test_type_per_file_decomposition() {
    let src_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rs_files(&src_dir, &mut files);
    assert!(!files.is_empty(), "src directory must contain .rs files");

    let mut violations = Vec::new();

    for path in &files {
        let file_name = path.file_name().unwrap().to_str().unwrap();
        if EXEMPT_FILES.contains(&file_name) {
            continue;
        }

        let content = fs::read_to_string(path).expect("Failed to read file");
        let mut public_types = Vec::new();

        for line in content.lines() {
            let clean = line.trim();
            if clean.starts_with("pub struct ")
                || clean.starts_with("pub enum ")
                || clean.starts_with("pub trait ")
            {
                let parts: Vec<&str> = clean.split_whitespace().collect();
                if parts.len() >= 3 {
                    let type_name = parts[2].split('<').next().unwrap();
                    public_types.push(type_name.to_string());
                }
            }
        }

        if public_types.len() > 1 {
            violations.push(format!(
                "{}: Multiple primary public types defined in a single file: {:?}. Each type must reside in its own dedicated file.",
                path.display(),
                public_types
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "Violations of 'Type-per-File' detected:\n{}",
        violations.join("\n")
    );
}

#[test]
fn test_zero_allocation_verification_suite_intact() {
    let zero_alloc_test_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("zero_alloc_tests.rs");

    assert!(
        zero_alloc_test_path.exists(),
        "zero_alloc_tests.rs must exist to guarantee hot path allocation enforcement"
    );

    let content =
        fs::read_to_string(&zero_alloc_test_path).expect("Failed to read zero_alloc_tests.rs");

    assert!(
        content.contains("TrackingAllocator"),
        "zero_alloc_tests.rs must define TrackingAllocator"
    );
    assert!(
        content.contains("GlobalAlloc"),
        "TrackingAllocator must implement GlobalAlloc"
    );
    assert!(
        content.contains("storage_allocs, 0") || content.contains("assert_eq!(storage_allocs, 0"),
        "zero_alloc_tests.rs must assert 0 storage allocations"
    );
    assert!(
        content.contains("codec_allocs, 0") || content.contains("assert_eq!(codec_allocs, 0"),
        "zero_alloc_tests.rs must assert 0 codec allocations"
    );
}
