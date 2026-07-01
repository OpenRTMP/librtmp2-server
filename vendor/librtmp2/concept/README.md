# Project Concept

This directory holds the design concept for `librtmp2`, written before (and now alongside) the implementation in `src/` and `include/`. It describes *why* the library exists, what belongs in scope, and the phased plan used to build it.

## Files

- [`librtmp2-core.md`](librtmp2-core.md) — the full concept: purpose, goals, non-goals, architecture principles, repository layout, public API sketch, protocol modules (handshake, chunking, AMF, session/commands), Enhanced RTMP v1/v2, state machine, memory/security rules, error classes, testing strategy, build system, versioning, phase plan, and an implementation-status tracker that is kept in sync with the actual code.

A previous `librtmp2-server.md` concept (a product/server layer built on top of the core) was removed deliberately: `librtmp2` itself stays a protocol library, not a media server. Server/product concerns belong in a separate, downstream project.
