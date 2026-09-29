---
name: rust-tdd-refactoring
description: Enforces test-driven development (Red -> Green -> Refactor) and modular public decomposition with zero private helper methods.
---

# Rust TDD & Refactoring Standards

## Invariants
1. **Red -> Green -> Refactor Cycle**:
   - **Red**: Write a failing unit test asserting exact requirements before touching production logic.
   - **Green**: Write the minimal code necessary to make the test pass.
   - **Refactor**: Clean up the design, ensure cache alignment, remove allocations, and eliminate clippy warnings.
2. **No Private Helper Methods**:
   - Prohibit `fn helper(&self)` hidden inside struct `impl` blocks.
   - Every behavior must be decomposed into a separate crate-visible (`pub` or `pub(crate)`) standalone type, pure function, or trait.
   - This ensures full unit-testability, clean composability, and transparent architecture.
