# Performance Smoke Baseline

This repository now includes smoke-level performance validation in:

- `Tests/PerformanceSmokeTests.cpp`

Scope:

- bulk import through `ExecuteImport(...)`
- indexed equality query on persisted SQLite data
- relation-query-shaped lookup through indexed relation columns

Current purpose:

- verify that the repository contains executable performance scenarios
- ensure performance-sensitive code paths exist in regression coverage

What this is not:

- a formal product SLA benchmark
- a statistically rigorous benchmark suite
- a replacement for environment-specific profiling

Recommended next step when validating on a real machine:

1. record import elapsed time
2. record equality-query elapsed time
3. record relation-query elapsed time
4. compare against project-specific acceptance thresholds
