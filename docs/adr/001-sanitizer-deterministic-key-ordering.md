# ADR-001: Deterministic key ordering in sanitizer for seeded reproducibility

## Status

Accepted

## Context

The sanitizer uses `gofakeit` with a seeded PRNG to produce deterministic fake data when a non-zero `--seed` is provided. Each call to `generate()` advances the PRNG state, so the order in which fields are processed determines which fake value each field receives.

Go's `map` type randomizes iteration order across runs. This means:

- In `sanitizeRecord`, iterating `map[string]any` keys in random order causes different fields to receive different fake values across runs, even with the same seed.
- In `sanitizeCSVFile`, iterating `map[int]string` (column index to faker type) in random order causes the same issue.

This was discovered when `TestSanitizer_SeedDeterminism` began flaking after adding multiple fields to the test data.

## Decision

Sort keys/indices before iterating in both `sanitizeRecord` (sort map keys with `sort.Strings`) and `sanitizeCSVFile` (sort column indices with `sort.Ints`) to ensure consistent RNG consumption order.

## Consequences

- Deterministic output is guaranteed for the same seed, input data, and config, regardless of Go's map iteration randomization.
- Small performance overhead from sorting keys on every record — negligible for typical document sizes.
- Any future code that consumes values from the seeded faker in a loop over a map must also sort keys first, or determinism will break.
