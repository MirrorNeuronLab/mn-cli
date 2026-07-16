# AGENTS.md

Guidance for future coding agents working in this repository.

## Issue Fixing Policy

- Unless the user explicitly asks for a temporary workaround, fix the root cause in the intended layer or contract.
- Avoid adding fallback paths, compatibility shims, feature flags, or temp solutions that mask a broken primary path.
- If fallback behavior is already product-specified, keep it narrow, documented, and tested; do not use it to avoid fixing the primary path.

## CLI Output Standards

- Keep presentation changes separate from command behavior, data contracts, and machine-readable output.
- Use the shared UI helpers in `mn_cli.libs.ui` for user-facing status messages:
  - `✓` for successful completed actions.
  - `→` for progress or informative lifecycle updates.
  - `! Warning:` for non-fatal conditions.
  - `× Error:` for actionable failures; include an error code when one is available.
- Routine confirmations should be compact: a status line followed by a borderless key/value summary. Do not add decorative panels for ordinary mutations.
- Reserve rounded panels for rich lifecycle results such as submitted jobs, detached runs, and final job summaries. Use the shared result-panel anatomy and detail table so those views remain consistent.
- Prefer concise sentence-case messages and stable field labels. Do not introduce legacy `=>` progress lines, ad-hoc red error strings, or ad-hoc yellow warning strings.
- Preserve plain-mode output (`MN_CLI_OUTPUT=plain`) as predictable, unadorned text for automation.
- In the interactive monitor, indicate selection with the cursor marker and text weight; do not use reverse-video row backgrounds.
