# TODOS

## Release reproducibility

### Document the Grok Bot 0.36 candidate acquisition path

**What:** Record a legally authorized, checksum-pinned acquisition or reconstruction path for the proprietary Grok Bot 0.36 candidate.

**Why:** Local bundle checks are repeatable only when another authorized operator can obtain the same candidate bytes.

**Context:** The candidate package and extracted application remain local, ignored artifacts. The repository currently records the expected version and bundle checks but does not distribute the proprietary package.

**Effort:** M
**Priority:** P1
**Depends on:** An authorized source for the Grok Bot 0.36 package

## Runtime verification

### Add a repeatable visible-GUI smoke harness

**What:** Automate visible roster, channel creation, per-member reply, deletion, and reload-readback checks.

**Why:** Backend transcripts and accessibility readback do not by themselves prove stable rendered pixels across restarts.

**Context:** The current release process uses a fresh manual GUI canary after the local verifier. A dedicated harness should preserve the same runtime-proof boundaries and avoid writing production or external-provider state.

**Effort:** M
**Priority:** P1
**Depends on:** A stable, authorized Grok Bot 0.36 candidate

## Completed

### Grok Bot 0.36 local implementation candidate

**What:** Add the local gateway, channel/group contracts, Codex Responses binding, local verifier, and manual-only workflow policy.

**Why:** Restore local Bot/channel operation while keeping provider, GitHub, and runtime evidence as separate gates.

**Context:** Candidate code and local validation are complete. Merge and runtime acceptance remain separately evidenced lifecycle steps.

**Effort:** L
**Priority:** P0
**Depends on:** None

**Completed:** v0.36.0.0 (2026-09-03)
