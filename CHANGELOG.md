# Changelog

All notable changes to grok-bridge are documented in this file.

## [0.36.0.0] - 2026-09-03

### Added

- Grok Bot 0.36 local coordinator launch, routing, renderer patch, and verification tooling.
- Durable local Bot and group-channel lifecycle, member fan-out, transcript identity, replay, and acceptance contracts.
- A safe model-binding layer that mirrors the active Codex Responses provider/model without storing or printing credentials.
- Staged and explicit-write-set secret scanning for release preparation.

### Changed

- The Grok Bot 0.36 backend now defaults to the active Codex model binding; Ollama remains an explicit opt-in backend.
- Automatic push/pull-request workflow triggers are removed; `workflow_dispatch` remains, with the Windows local verifier as the acceptance gate.

### Fixed

- Channel creation, group membership, per-member replies, renderer body visibility, reconnect state, and terminal transcript cursors.
- Launcher drift between a healthy backend process and the requested model binding.

### Security

- Candidate packages, runtime profiles, logs, reconstructed ASAR files, and credentials remain excluded from Git.
- Provider errors fail closed without cross-wire or cross-provider fallback.
- Non-loopback HTTP providers are rejected before an Authorization header is constructed or a provider request is sent unless the explicit risk override is set; safe summaries expose only authentication availability.
- Backend restart ownership requires the exact Python executable and exact `backend_server.py` command line.
