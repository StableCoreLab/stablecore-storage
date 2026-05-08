# Execution Boundary And Scope

## AI Execution Boundary (P0)

AI operates in:

> Offline Code Generation Mode

AI may:

- modify source code
- modify tests
- modify CMake
- update documentation
- perform static reasoning and analysis

AI must NOT:

- compile
- run executables
- run tests
- execute scripts
- invoke build systems
- modify generated build artifacts
- validate behavior through execution

Code correctness must be determined through:

- static reasoning
- semantic consistency
- architecture constraints
- existing code patterns

## Modification Scope Rules (P0)

Changes must remain strictly localized to the requested scope.

AI must NOT:

- refactor unrelated modules
- rename symbols without necessity
- rewrite stable implementations
- move files across modules
- perform repository-wide formatting
- modify unrelated behavior
- introduce speculative abstractions

Prefer:

- extension over rewrite
- local fixes over global refactors
- adapters over duplication
- incremental evolution over redesign

Large-scale redesign is forbidden unless explicitly requested.

## Common Failure Patterns

Avoid the following architectural violations:

- accessing SQLite directly from upper layers
- embedding business rules inside backend code
- bypassing transaction boundaries
- implicit writes during reads
- hidden upgrade paths
- using cache state as persistence truth
- leaking SQLite concepts into public interfaces
- mixing project lifecycle with UI workflow
