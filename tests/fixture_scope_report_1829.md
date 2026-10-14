# Fixture prerequisite report for kyil-io/tools#1829

## Scope correction

- Removed synthetic final-registration `Pool.connect` integration assertions from
  fixture scaffolding.
- Removed manual loser close and direct private `Pool` registry mutation that
  manufactured winner/loser expectations.
- Removed `SameIdentifierConnectRace(boundary="final_registration")` mode and
  narrowed race scaffolding to its real transport-return boundary behavior.
- Removed winner/loser observation assertion helpers that could not honestly
  observe mainline arbitration state.
- Preserved and contract-tested only reusable prerequisites:
  - loopback READY-hold websocket handler behavior,
  - `FinalRegistrationGenerationBarrier` scheduling semantics.

## Accounting

- Base commit: `37a321ac2e6db36c5a3d117431f2d447f294002e`
- Full base-to-head charged LOC (net): `+469 / -8` (net `+461`)
- Per-commit churn sum (additions + deletions across each commit): `+946 / -485`
  => churn `1431`

Notes:
- Base-to-head charged LOC is the net line delta between base and current head.
- Per-commit churn counts every touched line in every commit and is not a net
  measurement.
