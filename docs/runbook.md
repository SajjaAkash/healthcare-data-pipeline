# Clinical Release Runbook

## Purpose

This runbook exists for the part of healthcare analytics work that tends to become production-critical:
deciding whether a batch is safe to publish when encounter and claim coverage are not perfectly aligned.

## Release Gate Logic

- If quality rules fail, publication should remain blocked.
- If missing-claim coverage exceeds the configured tolerance, publication should remain blocked.
- If orphan-claim coverage exceeds tolerance, publication should remain blocked.
- Protected patient outputs should be used for non-authorized consumers instead of raw identifiers.

## Typical Response Flow

1. Review the reconciliation audit artifact.
2. Inspect unmatched encounters and orphan claims.
3. Confirm whether the issue is expected lag or a structural mapping problem.
4. Reprocess the affected slice after the source issue is resolved.
5. Re-open publication only when the release gate returns `publish_allowed=true`.
