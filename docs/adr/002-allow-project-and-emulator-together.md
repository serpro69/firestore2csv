# ADR-002: Allow --project and --emulator flags together

## Status

Accepted

## Context

The `--project` and `--emulator` flags were mutually exclusive. When `--emulator` was used, the project ID was hardcoded to `"emulator-project"`. This caused problems with Firebase emulators running in single-project mode (the default), which reject requests whose project ID doesn't match the one configured in `firebase.json`.

Users had no way to specify the correct project ID when connecting to an emulator, making the tool incompatible with single-project mode emulators unless the emulator was reconfigured with `"singleProjectMode": false`.

## Decision

Allow `--project` and `--emulator` to be used together. When both are provided, the given `--project` value is used as the project ID for the emulator connection. When only `--emulator` is given, the project defaults to `"emulator-project"` for backward compatibility. At least one of the two flags must still be provided.

## Consequences

- Users can now connect to emulators running in single-project mode by passing both flags (e.g. `-e localhost:8686 -p my-project`).
- Existing scripts using only `--emulator` continue to work unchanged (project defaults to `"emulator-project"`).
- The validation error message changed from "exactly one of" to "at least one of", which could affect scripts that parse stderr.
