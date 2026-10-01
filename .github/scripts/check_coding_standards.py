#!/usr/bin/env python3
"""
Flotilla Coding Standards Verification Script.

Enforces:
1. No Private Helper Methods:
   Inherent `impl` blocks must not contain private functions (fn without pub/pub(crate)).
   Logic must be decomposed into crate-visible standalone pure functions or dedicated types.
2. Type-per-File Architecture:
   Every non-exempt file must define at most one primary public type (struct, enum, trait)
   whose name matches the snake_case filename.
3. Zero-Allocation Test Verification:
   Ensures the TrackingAllocator consensus hot path verification test suite is present.
"""

import os
import re
import sys

# Files that contain pure functions or crate/module infrastructure
EXEMPT_FILES = {
    "mod.rs",
    "lib.rs",
    "rules.rs",
    "evaluator.rs",
    "framing.rs",
    "packets.rs",
    "sinks.rs",
    "channel.rs",
    "commit.rs",
}

# Subsystem-specific type-to-file mappings
TYPE_FILE_MAP = {
    "CosmosError": "error.rs",
    "CosmosConfig": "config.rs",
    "CosmosDocument": "document.rs",
    "CosmosAuth": "auth.rs",
    "CosmosClient": "client.rs",
    "CosmosOffloader": "offloader.rs",
    "OffloaderCommand": "offloader_command.rs",
    "CosmosArchiveSink": "sink.rs",
}


def to_snake_case(name: str) -> str:
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s).lower()


def check_no_private_helper_methods(src_dir: str):
    violations = []
    files_scanned = 0

    # Pattern matching fn definitions: fn name(...), async fn, const fn, unsafe fn
    fn_pattern = re.compile(
        r"^(?:(?:const|async|unsafe|extern(?:\s+\"[^\"]+\")?)\s+)*fn\s+([a-zA-Z0-9_]+)"
    )

    for root, _, files in os.walk(src_dir):
        for f in files:
            if not f.endswith(".rs"):
                continue
            files_scanned += 1
            path = os.path.join(root, f)

            with open(path, "r", encoding="utf-8") as fp:
                lines = fp.readlines()

            in_impl = False
            is_trait_impl = False
            brace_level = 0
            impl_target = ""

            for line_no, raw_line in enumerate(lines, 1):
                clean_line = raw_line.split("//")[0].strip()
                if not clean_line:
                    continue

                # Check for impl block opening
                impl_match = re.match(
                    r"^impl(?:\s*<[^>]*>)?\s+(?:.*?\s+for\s+)?([A-Za-z0-9_]+)",
                    clean_line,
                )
                if impl_match and not in_impl and "{" in clean_line:
                    in_impl = True
                    brace_level = 0
                    is_trait_impl = " for " in clean_line
                    impl_target = impl_match.group(1)

                if in_impl:
                    brace_level += clean_line.count("{") - clean_line.count("}")
                    if brace_level <= 0:
                        in_impl = False
                        is_trait_impl = False
                        impl_target = ""
                        continue

                    # Inside inherent impl block at top method level
                    if not is_trait_impl and brace_level == 1:
                        fn_match = fn_pattern.match(clean_line)
                        if fn_match:
                            fn_name = fn_match.group(1)
                            violations.append(
                                (
                                    path,
                                    line_no,
                                    impl_target,
                                    fn_name,
                                    clean_line,
                                )
                            )

    return violations, files_scanned


def check_type_per_file(src_dir: str):
    violations = []
    files_scanned = 0

    type_pattern = re.compile(
        r"^pub\s+(?:struct|enum|trait)\s+([A-Za-z0-9_]+)", re.MULTILINE
    )

    for root, _, files in os.walk(src_dir):
        for f in files:
            if not f.endswith(".rs"):
                continue
            if f in EXEMPT_FILES:
                continue

            files_scanned += 1
            path = os.path.join(root, f)

            with open(path, "r", encoding="utf-8") as fp:
                content = fp.read()

            types = type_pattern.findall(content)

            if len(types) > 1:
                violations.append(
                    (
                        path,
                        f"Multiple primary types defined in single file: {types}. "
                        f"Each type must have its own dedicated file.",
                    )
                )
            elif len(types) == 1:
                tname = types[0]
                expected_filename = to_snake_case(tname) + ".rs"
                mapped_filename = TYPE_FILE_MAP.get(tname, expected_filename)
                if f != expected_filename and f != mapped_filename:
                    violations.append(
                        (
                            path,
                            f"Type '{tname}' in file '{f}' does not match expected "
                            f"filename '{expected_filename}' (or mapped '{mapped_filename}').",
                        )
                    )

    return violations, files_scanned


def check_zero_alloc_tests(tests_dir: str):
    zero_alloc_path = os.path.join(tests_dir, "zero_alloc_tests.rs")
    if not os.path.isfile(zero_alloc_path):
        return [f"Missing required zero-allocation verification test file: {zero_alloc_path}"]

    with open(zero_alloc_path, "r", encoding="utf-8") as fp:
        content = fp.read()

    errors = []
    if "GlobalAlloc" not in content or "TrackingAllocator" not in content:
        errors.append(f"{zero_alloc_path} must implement TrackingAllocator using GlobalAlloc.")
    if "assert_eq!(storage_allocs, 0" not in content and "ALLOC_COUNT" not in content:
        errors.append(f"{zero_alloc_path} must assert 0 heap allocations along consensus hot paths.")

    return errors


def main():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    src_dir = os.path.join(repo_root, "src")
    tests_dir = os.path.join(repo_root, "tests")

    print("=" * 75)
    print("FLOTILLA CODING STANDARDS ENFORCEMENT AUDIT")
    print("=" * 75)

    all_passed = True

    # 1. Audit: No Private Helper Methods
    helper_violations, scanned_helpers = check_no_private_helper_methods(src_dir)
    print(f"\n[1/3] Auditing Inherent 'impl' Blocks for Private Helpers ({scanned_helpers} files)...")
    if helper_violations:
        all_passed = False
        print(f"❌ FAIL: Found {len(helper_violations)} private helper method(s):")
        for path, line, target, fn, snippet in helper_violations:
            relpath = os.path.relpath(path, repo_root)
            print(f"  - {relpath}:{line} in 'impl {target}': method '{fn}' is private.")
            print(f"    Line: {snippet}")
            print("    Guideline: Decompose into a standalone crate-visible pure function or dedicated type.")
            print(f"::error file={relpath},line={line}::Private helper method '{fn}' in 'impl {target}'. Flotilla prohibits private helper methods.")
    else:
        print("✅ PASS: Zero private helper methods found across all inherent impl blocks.")

    # 2. Audit: Type-per-File Architecture
    type_violations, scanned_types = check_type_per_file(src_dir)
    print(f"\n[2/3] Auditing Type-per-File Decomposition ({scanned_types} files)...")
    if type_violations:
        all_passed = False
        print(f"❌ FAIL: Found {len(type_violations)} type-per-file violation(s):")
        for path, err in type_violations:
            relpath = os.path.relpath(path, repo_root)
            print(f"  - {relpath}: {err}")
            print(f"::error file={relpath}::{err}")
    else:
        print("✅ PASS: Every type is strictly decomposed into its own dedicated source file.")

    # 3. Audit: Zero-Allocation Test Invariant
    print("\n[3/3] Auditing Zero-Allocation Verification Test Suite...")
    zero_alloc_errors = check_zero_alloc_tests(tests_dir)
    if zero_alloc_errors:
        all_passed = False
        print(f"❌ FAIL: Zero-allocation test suite check failed:")
        for err in zero_alloc_errors:
            print(f"  - {err}")
            print(f"::error::{err}")
    else:
        print("✅ PASS: Zero-allocation TrackingAllocator test suite is fully present and asserting 0 allocations.")

    print("\n" + "=" * 75)
    if all_passed:
        print("🎉 ALL FLOTILLA CODING STANDARDS SATISFIED!")
        print("=" * 75)
        sys.exit(0)
    else:
        print("⚠️ CODING STANDARDS VIOLATIONS DETECTED. Review errors above.")
        print("=" * 75)
        sys.exit(1)


if __name__ == "__main__":
    main()
