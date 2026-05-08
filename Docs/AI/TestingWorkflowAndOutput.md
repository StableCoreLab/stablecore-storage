# Testing Workflow And Output

## Testing Rules

- GoogleTest is mandatory
- New behavior requires tests
- Bug fixes require regression tests

Priority test areas:

- rollback recovery
- deleted-record access
- interrupted import
- migration failure
- undo/redo consistency
- transaction recovery
- readonly open
- failed upgrade recovery
- journal recovery

AI may modify tests but must not execute them.

## Documentation Rules

Documentation updates are required only when:

- architecture changes
- public APIs change
- storage semantics change
- migration workflow changes
- replay / journal behavior changes

Avoid unrelated documentation rewrites.

## Task Workflow

Every task should follow this order:

1. Analyze scope and boundaries
2. Understand storage semantic constraints
3. Implement minimal localized changes
4. Add or update tests
5. Perform static self-review
6. Summarize risks and remaining gaps

## Required Output

Task summaries should include:

### Change Summary

- modified modules
- implemented behavior
- semantic impact

### Test Coverage

- affected scenarios
- boundary conditions covered
- regression coverage

### Risk Assessment

- storage semantic risks
- migration risks
- rollback risks
- SQLite persistence risks
- uncovered paths

### Suggested Next Steps

- most reasonable follow-up tasks
