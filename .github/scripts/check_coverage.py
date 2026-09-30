#!/usr/bin/env python3
"""
Coverage gate verification script for flotilla.
Enforces minimum line and branch coverage thresholds from cargo-llvm-cov JSON report.
Generates GitHub Actions step summaries and workflow annotations.
"""

import argparse
import json
import os
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Verify code coverage gates for lines and branches."
    )
    parser.add_argument(
        "coverage_file",
        nargs="?",
        default="coverage.json",
        help="Path to cargo-llvm-cov summary JSON file (default: coverage.json)",
    )
    parser.add_argument(
        "--min-line",
        type=float,
        default=85.0,
        help="Minimum required line coverage percentage (default: 85.0)",
    )
    parser.add_argument(
        "--min-branch",
        type=float,
        default=90.0,
        help="Minimum required branch coverage percentage (default: 90.0)",
    )
    parser.add_argument(
        "--markdown-output",
        type=str,
        default=None,
        help="Path to write markdown summary file for PR comments",
    )
    return parser.parse_args()


def format_status(actual: float, required: float) -> str:
    return "✅ PASS" if actual >= required else "❌ FAIL"


def main():
    args = parse_args()

    if not os.path.isfile(args.coverage_file):
        print(f"::error::Coverage report file not found: {args.coverage_file}")
        sys.exit(1)

    with open(args.coverage_file, "r", encoding="utf-8") as f:
        try:
            report = json.load(f)
        except json.JSONDecodeError as e:
            print(f"::error::Failed to parse JSON coverage report: {e}")
            sys.exit(1)

    try:
        data_block = report["data"][0]
        totals = data_block["totals"]
        files = data_block.get("files", [])
    except (KeyError, IndexError) as e:
        print(f"::error::Invalid coverage JSON schema: missing {e}")
        sys.exit(1)

    lines_total = totals.get("lines", {})
    branches_total = totals.get("branches", {})

    line_cov = lines_total.get("percent", 0.0)
    line_covered = lines_total.get("covered", 0)
    line_count = lines_total.get("count", 0)

    branch_cov = branches_total.get("percent", 0.0)
    branch_covered = branches_total.get("covered", 0)
    branch_count = branches_total.get("count", 0)

    line_pass = line_cov >= args.min_line
    branch_pass = branch_cov >= args.min_branch
    all_passed = line_pass and branch_pass

    print("=" * 70)
    print("CODE COVERAGE GATE RESULTS")
    print("=" * 70)
    print(
        f"Line Coverage:   {line_cov:6.2f}% ({line_covered}/{line_count}) "
        f"[Required: {args.min_line:5.2f}%] -> {format_status(line_cov, args.min_line)}"
    )
    print(
        f"Branch Coverage: {branch_cov:6.2f}% ({branch_covered}/{branch_count}) "
        f"[Required: {args.min_branch:5.2f}%] -> {format_status(branch_cov, args.min_branch)}"
    )
    print("=" * 70)

    # Print breakdown per file
    print(f"\n{'File':<42} {'Lines':<18} {'Branches':<18}")
    print("-" * 80)
    repo_root = os.getcwd()
    file_rows = []
    for f in sorted(files, key=lambda x: x.get("filename", "")):
        fname = f.get("filename", "")
        if fname.startswith(repo_root):
            fname = os.path.relpath(fname, repo_root)

        f_summary = f.get("summary", {})
        f_lines = f_summary.get("lines", {})
        f_branches = f_summary.get("branches", {})

        f_lp = f_lines.get("percent", 0.0)
        f_lc = f_lines.get("covered", 0)
        f_lt = f_lines.get("count", 0)

        f_bp = f_branches.get("percent", 0.0)
        f_bc = f_branches.get("covered", 0)
        f_bt = f_branches.get("count", 0)

        line_str = f"{f_lp:5.1f}% ({f_lc}/{f_lt})"
        branch_str = f"{f_bp:5.1f}% ({f_bc}/{f_bt})" if f_bt > 0 else "    N/A"

        print(f"{fname:<42} {line_str:<18} {branch_str:<18}")
        file_rows.append((fname, f_lp, f_lc, f_lt, f_bp, f_bc, f_bt))

    # Build Markdown Summary Report
    md_lines = []
    md_lines.append("## 🎯 Code Coverage Gate Summary\n\n")
    if all_passed:
        md_lines.append(
            "> **Status: PASSED** 🎉 Both line and branch coverage requirements are satisfied.\n\n"
        )
    else:
        md_lines.append(
            "> **Status: FAILED** ⚠️ One or more coverage gates fell below required thresholds.\n\n"
        )

    md_lines.append("| Metric | Coverage | Threshold | Status |\n")
    md_lines.append("| :--- | :---: | :---: | :---: |\n")
    md_lines.append(
        f"| **Line Coverage** | `{line_cov:.2f}%` ({line_covered}/{line_count}) | `{args.min_line:.2f}%` | {format_status(line_cov, args.min_line)} |\n"
    )
    md_lines.append(
        f"| **Branch Coverage** | `{branch_cov:.2f}%` ({branch_covered}/{branch_count}) | `{args.min_branch:.2f}%` | {format_status(branch_cov, args.min_branch)} |\n\n"
    )

    md_lines.append("<details><summary><b>Detailed File Breakdown</b></summary>\n\n")
    md_lines.append("| File | Line Coverage | Branch Coverage |\n")
    md_lines.append("| :--- | :---: | :---: |\n")
    for fname, flp, flc, flt, fbp, fbc, fbt in file_rows:
        b_str = f"`{fbp:.1f}%` ({fbc}/{fbt})" if fbt > 0 else "—"
        md_lines.append(
            f"| `{fname}` | `{flp:.1f}%` ({flc}/{flt}) | {b_str} |\n"
        )
    md_lines.append("\n</details>\n")
    md_content = "".join(md_lines)

    # GitHub Step Summary output
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as sf:
            sf.write(md_content)

    # Markdown file output (for PR comments)
    if args.markdown_output:
        with open(args.markdown_output, "w", encoding="utf-8") as mf:
            mf.write(md_content)

    # GitHub workflow error annotations
    if not line_pass:
        print(
            f"::error title=Coverage Gate Failed::Line coverage is {line_cov:.2f}%, "
            f"which is below the required threshold of {args.min_line:.2f}%."
        )

    if not branch_pass:
        print(
            f"::error title=Coverage Gate Failed::Branch coverage is {branch_cov:.2f}%, "
            f"which is below the required threshold of {args.min_branch:.2f}%."
        )

    if not all_passed:
        print("\n❌ Coverage gate failed! See details above.")
        sys.exit(1)

    print("\n✅ All coverage gates passed successfully!")
    sys.exit(0)


if __name__ == "__main__":
    main()
