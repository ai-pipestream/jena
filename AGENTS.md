# Apache Jena Protobuf and gRPC Workspace (`AGENTS.md`)

Guidance for coding agents working in `/work/worktrees/jena-workspace` (also reachable via `/work/woktrees/jena-workspace`).
Last refreshed: 2026-10-30.

## 1. Start Here

This directory is a multi-worktree workspace for developing, reviewing, and graduating Apache Jena Protobuf and gRPC contributions. It is not a single Git repository at the root; always `cd` into the specific checkout or worktree you intend to inspect, build, test, or commit.

There are two canonical locations for Apache Jena on this machine:

- **Pristine reference clone**: `/work/reference-code/jena`
  Read-only reference checkout on `main`. Do not create feature branches or worktrees from `/work/reference-code/jena`; create all branches and worktrees from `fork/` inside this workspace.
- **Active development workspace**: `/work/worktrees/jena-workspace`
  Contains `fork/` (the `main` checkout of `ai-pipestream/jena`), the 3 dedicated PR worktrees, the 4-agent signoff ledger under `reviews/`, and workspace helper scripts.

---

## 2. Tracked Tickets and Context

| Ticket / Issue | Link | Role in This Workspace |
| :--- | :--- | :--- |
| **GH-4364** (`apache/jena#4364`) | https://github.com/apache/jena/issues/4364 | **Primary tracking issue:** *Completing Protobuf / gRPC support (JENA-2167): message-level StreamRDF/RowSet adapters, Protobuf RDFPatch, and RDFLink over gRPC*. All upstream PR titles and commit messages must start with `GH-4364: `. |
| **JENA-2167** (ASF JIRA) | https://issues.apache.org/jira/browse/JENA-2167 | **Historical JIRA context:** *Provide an RDF Binary format using Protobuf* ("To go along side the RDF Thrift encoding. Sometimes, apps want protobuf encoded RDF, e.g. for use with gRPC."). Introduced `binary-rdf.proto` and `org.apache.jena.riot.protobuf` in `jena-arq`, stopping at `InputStream`/`OutputStream` helpers. |
| **GH-4363** (`apache/jena#4363`) | https://github.com/apache/jena/issues/4363 | **Related upstream discussion:** *[Discussion] Support for RDF Messages in Jena RIOT* (streaming sequences of atomic RDF datasets with preserved message boundaries). Useful context for how `RDF_Stream` and `RDF_Patch_Row` frame dataset/message boundaries. |

### Downstream Motivation (ProtoMolt)

ProtoMolt coordinates services and agents over gRPC and needs an eventually consistent RDF/SKOS and document-link graph store backed by `jena-tdb2` and `jena-arq` **without pulling Jetty or Servlet dependencies into the runtime**. Completing `GH-4364` in Apache Jena allows both in-process `DatasetGraph` access and remote gRPC graph hops through Jena's standard `RDFLink` / `RDFConnection` SPI and `PB_RDF` wire messages.

---

## 3. Workspace Layout: Fork and Worktrees

| Path | Branch | Remotes (`fork/`) | Purpose |
| :--- | :--- | :--- | :--- |
| `fork/` | `main` | `origin` = `https://github.com/ai-pipestream/jena.git`<br>`upstream` = `https://github.com/apache/jena.git` | Clean checkout of `main` tracking `origin/main` and synced with `upstream/main`. Parent repository for all git worktrees in this directory. Never develop features directly on `fork/main`. |
| `pr1-GH-4364-protobuf-stream-adapters/` | `GH-4364-protobuf-stream-adapters` | Inherited from `fork/` | **PR 1 (`jena-arq`):** Expose in-memory Protobuf message streaming adapters (`StreamRDF` <-> `RDF_StreamRow` / `RDF_Stream` and `RowSet` / `Binding` <-> `RDF_VarTuple` / `RDF_DataTuple`) in `org.apache.jena.riot.protobuf` without `InputStream`/`OutputStream` copy overhead. Zero new dependencies. |
| `pr2-GH-4364-protobuf-rdfpatch/` | `GH-4364-protobuf-rdfpatch` | Inherited from `fork/` | **PR 2 (`jena-arq` + `jena-rdfpatch`):** Bring `binary-rdf.proto` to parity with `BinaryRDF.thrift` by adding `RDF_Patch_Row` (`Patch_Header`, `Patch_Data_Add`, `Patch_Data_Del`, `Patch_Prefix_Add`, `Patch_Prefix_Del`, `PatchTxn`) and adding Protobuf binary reader/writer support in `jena-rdfpatch`. |
| `pr3-GH-4364-jena-grpc/` | `GH-4364-jena-grpc` | Inherited from `fork/` | **PR 3 (`jena-grpc` / `jena-rdfconnection`):** Lightweight, Jetty-free gRPC client and server (`RDFLinkGrpc`, `RDFConnectionGrpc`, and `DatasetGraph` gRPC service) supporting streaming quad/triple pattern lookups (`RDF_ANY` wildcards), bulk `StreamRDF` ingest, transactional `RDFPatch` replication, and streaming SPARQL Query/Update. |
| `reviews/` | (Workspace files) | N/A | Contains the mandatory 4-agent signoff ledgers (`PR1-SIGNOFF.md`, `PR2-SIGNOFF.md`, `PR3-SIGNOFF.md`) and review artifacts for `fable`, `antigravity`, `codex`, and `cursor`. |
| `sync.sh` | (Workspace script) | N/A | Fetches `origin` and `upstream`, reports branch/worktree states, and checks the 4-agent signoff status for each PR worktree. |

---

## 4. Detailed Scope of the 3 PRs (Standalone Value at Every Step)

Each PR is designed to deliver immediate standalone value without waiting for the subsequent PRs to land:

### PR 1: In-Memory Protobuf Message Adapters (`pr1-GH-4364-protobuf-stream-adapters/`)
- **Target module:** `jena-arq` (`org.apache.jena.riot.protobuf`)
- **Problem:** `StreamRDF2Protobuf` already uses `private final Consumer<RDF_StreamRow> rowHandler` internally, and `Protobuf2StreamRDF` already implements `VisitorStreamRowProtoRDF`, but `ProtobufRDF` only exposes `InputStream` and `OutputStream` methods (`streamToOutputStream`, `inputStreamToStreamRDF`, `writeRowSet`, `readRowSet`). Callers using gRPC `StreamObserver` or Kafka must serialize to a `ByteArrayOutputStream` and re-parse `PB_RDF` objects.
- **Deliverables:**
  1. Expose public factory/adapter methods on `ProtobufRDF` (and package components `StreamRDF2Protobuf`, `Protobuf2StreamRDF`, `PBufRDF`) for:
     - `StreamRDF` -> `Consumer<RDF_StreamRow>` and batched `Consumer<RDF_Stream>` (configurable batch size and `withValues` encoding).
     - `RDF_StreamRow` and `RDF_Stream` -> `StreamRDF` sink (with optional `FactoryRDFCaching` node cache).
  2. Expose message-level `RowSet` / `Binding` conversion (`RDF_VarTuple` header + `RDF_DataTuple` row stream/iterator) in `ProtobufRDF`, `Binding2Protobuf`, and `Protobuf2Binding`.
  3. Comprehensive unit tests in `TS_RDFProtobuf` (`TestProtobufStreamRDF`, `TestProtobufResultSet`).
- **Standalone value:** Zero new Maven dependencies in `jena-arq`. Immediately usable by any gRPC, Kafka, or actor-based application that frames `PB_RDF` messages.

### PR 2: Protobuf Support for `RDFPatch` (`pr2-GH-4364-protobuf-rdfpatch/`)
- **Target modules:** `jena-arq` (`Grammar/RDF-Protobuf/binary-rdf.proto` + `org.apache.jena.riot.protobuf.wire.PB_RDF`) and `jena-rdfpatch`
- **Problem:** `BinaryRDF.thrift` defines `RDF_Patch_Row` (`header`, `dataAdd`, `dataDel`, `prefixAdd`, `prefixDel`, `txn: TX | TC | TA | Segment`), which backs Jena's binary `RDFPatch` log and replication format. `binary-rdf.proto` stops at `RDF_VarTuple`/`RDF_DataTuple` and omits `RDF_Patch_Row`.
- **Deliverables:**
  1. Add the `RDF_Patch_Row` messages and `PatchTxn` enum to `jena-arq/Grammar/RDF-Protobuf/binary-rdf.proto` (matching `BinaryRDF.thrift`) and regenerate `PB_RDF.java` via `./gen-protobuf`.
  2. Implement Protobuf binary `RDFPatch` reader/writer and message-level `RDFChanges` <-> `Consumer<RDF_Patch_Row>` adapters in `jena-rdfpatch` alongside the existing Thrift implementation (`RDFPatchReaderBinary`, `StreamPatch`, `RDFPatchOps`).
  3. Extend `AbstractTestPatchIO` and `TS_RDFPatch` to cover Protobuf patches (streamed and delimited, including `TX`/`TC`/`TA` boundaries, prefixes, quads, triples, and RDF-star triple terms).
- **Standalone value:** Completes `JENA-2167` schema parity with Thrift and enables Protobuf-native transactional CDC outbox logs and patch streams over Kafka, files, or custom RPCs.

### PR 3: Jetty-Free gRPC Transport (`pr3-GH-4364-jena-grpc/`)
- **Target module:** `jena-grpc` (wired into root `pom.xml`, depending on `jena-arq`, `jena-rdfconnection`, `jena-rdfpatch`, and `io.grpc`; optional `jena-tdb2` for integration tests; **zero Jetty or Servlet dependencies**)
- **Problem:** Remote access in Jena (`RDFLinkHTTP` / `RDFLinkFuseki`) requires HTTP and Fuseki/Jetty on the server, and even `RDFLinkFuseki` has commented-out wishlist stubs for `findStream(g, s, p, o)` and `Send Patch` that HTTP/1.1 could not cleanly express.
- **Deliverables:**
  1. Define the gRPC service contract over `PB_RDF` messages for:
     - Streaming quad/triple pattern match (`findStream` using `RDF_Quad` with `RDF_ANY` wildcards -> `stream RDF_StreamRow` / `RDF_Stream`).
     - Streaming dataset/graph ingest (`load` / `put` via client-streamed `RDF_StreamRow` / `RDF_Stream`).
     - Transactional `RDFPatch` application and streaming patch subscription (`applyPatch` via `RDF_Patch_Row`).
     - SPARQL Query (`SELECT` streaming `RDF_VarTuple` + `RDF_DataTuple`, `CONSTRUCT`/`DESCRIBE` streaming `RDF_StreamRow`, `ASK` returning boolean) and SPARQL Update.
  2. Implement `RDFLinkGrpc` (and `RDFConnectionGrpc` adapter) conforming to Jena's `RDFLink` (`LinkSparqlQuery`, `LinkSparqlUpdate`, `LinkDatasetGraph`) SPI so local (`RDFLinkDataset`) and remote (`RDFLinkGrpc`) connections are interchangeable.
  3. Implement the server-side gRPC bindable service backed by any `DatasetGraph` (including `TDB2` and `DatasetGraphTxnMem`), tested with `InProcessServerBuilder` and Netty transport against `AbstractTestRDFLink`.

---

## 5. Mandatory 4-Agent Signoff Gate (`fable`, `antigravity`, `codex`, `cursor`)

**No PR worktree is eligible for upstream submission or integration until all four agents have reviewed the exact HEAD commit SHA and recorded an explicit `APPROVED` signoff:**

1. **`fable`**
2. **`antigravity`**
3. **`codex`**
4. **`cursor`**

### Signoff Rules and Workflow

- **Signoff ledger files:**
  - PR 1: `reviews/PR1-SIGNOFF.md`
  - PR 2: `reviews/PR2-SIGNOFF.md`
  - PR 3: `reviews/PR3-SIGNOFF.md`
- **SHA pinning:** Every agent signoff must record the exact `git rev-parse HEAD` commit SHA it reviewed, the date/time, the test command executed, and its findings (Blocking vs. Minor, following `~/.claude/skills/krickert-review/SKILL.md`).
- **Invalidation on commit:** If a worktree's `HEAD` SHA changes for any reason (bug fix, review adjustment, or rebase onto `upstream/main`), any prior signoff for an older SHA becomes stale and all 4 agents (`fable`, `antigravity`, `codex`, `cursor`) must re-verify and sign off on the new `HEAD` SHA.
- **Independence:** Each agent must inspect the actual diff (`git diff upstream/main...HEAD`), verify that tests fail without the production change (or pinpoint the new behavior), check Javadoc/visibility/allocations, and verify the build passes before marking `APPROVED`.

---

## 6. Non-Negotiable Ground Rules and Code Standards

1. **Apache Committing and Upstream Write Rules:**
   - **Never push to `upstream` (`apache/jena`)** and never take any GitHub write action on `apache/jena` (opening PRs, posting issue/PR comments, editing descriptions) unless Kristian explicitly authorizes that exact single action.
   - Fork pushes go to `origin` (`ai-pipestream/jena`) when requested. When opening review PRs inside the fork, always pass `--repo ai-pipestream/jena` so `gh` never defaults to `apache/jena`.
   - Force pushes (`--force` or `--force-with-lease`) from agent sessions are blocked; if a branch rebase requires a force-with-lease push to `origin`, give Kristian the exact one-liner prefixed with `!`.
2. **No Em Dashes Anywhere:**
   - Never use em dashes or en dashes in commit messages, Javadoc, comments, PR descriptions, or markdown docs. Use commas, colons, parentheses, or ASCII hyphens (`-` / `--`).
3. **Apache Jena Code Style (`CONTRIBUTING.md`) and `krickert-review` Discipline:**
   - Read `/work/worktrees/jena-workspace/fork/CONTRIBUTING.md` and `/home/krickert/.claude/skills/krickert-review/SKILL.md` before writing or reviewing code.
   - **License header:** Include the standard ASF 2.0 header at the top of every new file.
   - **Formatting:** K&R "Egyptian" braces, 4 spaces for Java indentation, 2 spaces for XML indentation, one statement per line, **no `@author` tags**, and never mix unrelated reformatting with functional changes.
   - **Visibility and Modifiers:** Restrict helpers to the minimum required visibility (`private` or package-private). Do not mark helper methods `static` if they do not need to be. Validate public API arguments at the boundary (`Objects.requireNonNull` / `IllegalArgumentException` matching the surrounding Jena package convention).
   - **Javadoc:** Concise, concrete Javadoc on all added/changed methods without LLM-flavored filler or migration history narratives. Preserve all existing comments and Javadoc on untouched code.
   - **Zero Warnings:** Code must compile with zero warnings on Java 25 (`@Override` on overrides, proper generics, no unused imports or dead code).
4. **Test-First Workflow:**
   - Write or extend the JUnit 5 test in `src/test/java` first to demonstrate the missing capability or bug (red), then implement the minimal, clean production change so the suite passes (green).
5. **Commit Message and PR Naming Convention:**
   - Key commit messages and PR titles must start with `GH-4364: <concise user-visible summary>`.
   - Before Kristian lands a PR upstream, follow Section 11 of `/home/krickert/.claude/skills/krickert-review/SKILL.md` (single squashed commit, ASF generative tooling disclosure trailers as directed by Kristian, `git commit -s`).

---

## 7. Build and Test Commands

Apache Jena's CI (`.github/workflows/pr.yml`) builds with **Java 25 (Temurin)** using Maven:

```bash
# Fast targeted build and test for PR 1 (jena-arq Protobuf suite):
mvn -B test -pl jena-arq -Dtest=TS_RDFProtobuf

# Fast targeted build and test for PR 2 (jena-arq + jena-rdfpatch):
mvn -B test -pl jena-arq,jena-rdfpatch -Dtest=TS_RDFProtobuf,TS_RDFPatch

# Full module install without javadoc (matches .github/workflows/pr.yml):
mvn -B --file pom.xml -Dmaven.javadoc.skip=true install
```

When regenerating `PB_RDF.java` in PR 2 from `jena-arq/Grammar/RDF-Protobuf`:
```bash
cd jena-arq/Grammar/RDF-Protobuf
./gen-protobuf
```
Verify that `PB_RDF.java` retains the ASF 2.0 license header and `@SuppressWarnings("all")` annotation added by `./gen-protobuf`.
