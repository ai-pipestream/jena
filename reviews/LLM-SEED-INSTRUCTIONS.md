# Agent Seeding Instructions: Apache Jena Protobuf & gRPC Review (`GH-4364` / `JENA-2167`)

> **Note:** This `LLM-SEED-INSTRUCTIONS.md` file is a fork-only review guide on `ai-pipestream/jena` to seed multi-agent code reviews. It is not part of the upstream Apache Jena submission and should be excluded when squashing for upstream.

## 1. Mission & Role

You are reviewing (and verifying) a 3-part stacked implementation for Apache Jena ticket **GH-4364** (*Completing Protobuf / gRPC support (`JENA-2167`): message-level `StreamRDF`/`RowSet` adapters, Protobuf `RDFPatch`, and `RDFLink` over gRPC*).

Before taking any action, read:
1. `/work/worktrees/jena-workspace/AGENTS.md` (if working in the local multi-worktree workspace)
2. `CONTRIBUTING.md` at the repository root
3. `/home/krickert/.claude/skills/krickert-review/SKILL.md` (if present on the local machine)

### Strict Safety Guardrails (Non-Negotiable)
- **Never push or write to `upstream` (`apache/jena`).**
- **Do NOT open any GitHub Pull Requests** (neither on `apache/jena` nor on `ai-pipestream/jena`) unless explicitly instructed by the user.
- **Do NOT tag or mention any GitHub users** (never use `@username` in any commit, comment, or document).
- **Never use em dashes or en dashes** anywhere in code, Javadoc, comments, commit messages, or review markdown. Use ASCII hyphens (`-` or `--`), commas, colons, or parentheses.
- **Do NOT modify `fork/main`** (`/work/worktrees/jena-workspace/fork`).
- **Preserve SHA-pinned signoffs:** Only modify code in a PR worktree if you find a genuine **Blocking** bug. Any new commit invalidates prior agent signoffs for that worktree.

---

## 2. Branch Topology & Implementation Commits (Option A)

All three branches are pushed to the fork `origin` (`https://github.com/ai-pipestream/jena.git`) and checked out in dedicated worktrees under `/work/worktrees/jena-workspace`:

| PR | Worktree Path | Branch Name | Base | Implementation Commit SHA |
| :--- | :--- | :--- | :--- | :--- |
| **PR 1** | `/work/worktrees/jena-workspace/pr1-GH-4364-protobuf-stream-adapters` | `GH-4364-protobuf-stream-adapters` | `upstream/main` (`73713dcc54`) | `03c2037d3d4041fdf694fc88228d68d802cc6af6` |
| **PR 2** | `/work/worktrees/jena-workspace/pr2-GH-4364-protobuf-rdfpatch` | `GH-4364-protobuf-rdfpatch` | `upstream/main` (`73713dcc54`) | `451eeb0cdfed2ab907b19930d893a96e15ef2059` |
| **PR 3** | `/work/worktrees/jena-workspace/pr3-GH-4364-jena-grpc` | `GH-4364-jena-grpc` | Merge of PR 1 + PR 2 (`c9607ac2da`) | `aa929c2a075028ab4a11b623ebac7976f35eaea2` |

### Fork Inspection Links (No PRs Opened)
- **PR 1 branch on fork:** `https://github.com/ai-pipestream/jena/tree/GH-4364-protobuf-stream-adapters`
- **PR 1 diff on fork:** `https://github.com/ai-pipestream/jena/compare/main...GH-4364-protobuf-stream-adapters`
- **PR 2 branch on fork:** `https://github.com/ai-pipestream/jena/tree/GH-4364-protobuf-rdfpatch`
- **PR 2 diff on fork:** `https://github.com/ai-pipestream/jena/compare/main...GH-4364-protobuf-rdfpatch`
- **PR 3 branch on fork:** `https://github.com/ai-pipestream/jena/tree/GH-4364-jena-grpc`
- **PR 3 implementation commit on fork:** `https://github.com/ai-pipestream/jena/commit/aa929c2a075028ab4a11b623ebac7976f35eaea2`

To check workspace status at any time, run:
```bash
/work/worktrees/jena-workspace/sync.sh
```

---

## 3. Detailed Review Scope per PR

### PR 1: In-Memory Protobuf `StreamRDF` and `RowSet` Message Adapters
- **Worktree:** `/work/worktrees/jena-workspace/pr1-GH-4364-protobuf-stream-adapters`
- **Module:** `jena-arq` (`org.apache.jena.riot.protobuf`)
- **Diff Command:**
  ```bash
  cd /work/worktrees/jena-workspace/pr1-GH-4364-protobuf-stream-adapters
  git diff upstream/main...HEAD -- ':!LLM-SEED-INSTRUCTIONS.md'
  ```
- **Test & Verification Command:**
  ```bash
  cd /work/worktrees/jena-workspace/pr1-GH-4364-protobuf-stream-adapters
  mvn -B test -pl jena-arq -Dtest=TS_RDFProtobuf && ! git diff upstream/main...HEAD | grep -P '\xe2\x80[\x93\x94]'
  ```
- **Changed / Added Files:**
  - `jena-arq/src/main/java/org/apache/jena/riot/protobuf/PBufRDF.java`
  - `jena-arq/src/main/java/org/apache/jena/riot/protobuf/StreamRDF2Protobuf.java`
  - `jena-arq/src/main/java/org/apache/jena/riot/protobuf/Protobuf2StreamRDF.java`
  - `jena-arq/src/main/java/org/apache/jena/riot/protobuf/Binding2Protobuf.java`
  - `jena-arq/src/main/java/org/apache/jena/riot/protobuf/Protobuf2Binding.java`
  - `jena-arq/src/main/java/org/apache/jena/riot/protobuf/ProtobufRDF.java`
  - `jena-arq/src/test/java/org/apache/jena/riot/protobuf/TS_RDFProtobuf.java`
  - `jena-arq/src/test/java/org/apache/jena/riot/protobuf/TestProtobufStreamRDF.java`
  - `jena-arq/src/test/java/org/apache/jena/riot/protobuf/TestProtobufResultSet.java`
- **What to Verify:**
  1. Zero new Maven dependencies in `jena-arq`.
  2. `StreamRDF2Protobuf` and `Binding2Protobuf` reuse `RDF_StreamRow.Builder`, `RDF_Triple.Builder`, `RDF_Quad.Builder`, `RDF_Term.Builder`, and `RDF_DataTuple.Builder` without intermediate `ByteArrayOutputStream` copies.
  3. Batched `StreamRDF` -> `Consumer<RDF_Stream>` flushes partial batches on `finish()`.
  4. Blank node labels, RDF 1.2 triple terms (`Node_Triple`), base IRIs, prefix directives, and value-encoded literals round-trip losslessly and produce datasets isomorphic to `StreamRDFWriter` output.
  5. Boundary null/argument checks throw `IllegalArgumentException` consistently.

---

### PR 2: Protobuf `RDF_Patch_Row` Schema & Binary `RDFPatch` Support
- **Worktree:** `/work/worktrees/jena-workspace/pr2-GH-4364-protobuf-rdfpatch`
- **Modules:** `jena-arq` and `jena-rdfpatch`
- **Diff Commands:**
  ```bash
  cd /work/worktrees/jena-workspace/pr2-GH-4364-protobuf-rdfpatch
  # Inspect hand-written changes (excluding generated PB_RDF.java):
  git diff upstream/main...HEAD -- ':!jena-arq/src/main/java/org/apache/jena/riot/protobuf/wire/PB_RDF.java' ':!LLM-SEED-INSTRUCTIONS.md'
  ```
- **Test & Verification Command:**
  ```bash
  cd /work/worktrees/jena-workspace/pr2-GH-4364-protobuf-rdfpatch
  mvn -B test -pl jena-arq,jena-rdfpatch -Dtest=TS_RDFProtobuf,TS_RDFPatch && ! git diff upstream/main...HEAD | grep -P '\xe2\x80[\x93\x94]'
  ```
- **Changed / Added Files:**
  - `jena-arq/Grammar/RDF-Protobuf/binary-rdf.proto`
  - `jena-arq/Grammar/RDF-Protobuf/gen-protobuf`
  - `jena-arq/src/main/java/org/apache/jena/riot/protobuf/wire/PB_RDF.java` (regenerated via `./gen-protobuf` with `protoc 4.34.1`)
  - `jena-rdfpatch/src/main/java/org/apache/jena/rdfpatch/RDFPatchOps.java`
  - `jena-rdfpatch/src/main/java/org/apache/jena/rdfpatch/binary/RDFChangesWriterProtobuf.java`
  - `jena-rdfpatch/src/main/java/org/apache/jena/rdfpatch/binary/RDFPatchReaderProtobuf.java`
  - `jena-rdfpatch/src/test/java/org/apache/jena/rdfpatch/TS_RDFPatch.java`
  - `jena-rdfpatch/src/test/java/org/apache/jena/rdfpatch/TestPatchIO_Protobuf.java`
- **What to Verify:**
  1. `binary-rdf.proto` defines `Patch_Header`, `Patch_Data_Add`, `Patch_Data_Del`, `Patch_Prefix_Add`, `Patch_Prefix_Del`, `PatchTxn` (`TX`, `TC`, `TA`, `Segment`), `RDF_Patch_Row`, and `RDF_Patch` matching `BinaryRDF.thrift` semantics.
  2. `PB_RDF.java` retains the commented ASF 2.0 license header and `@SuppressWarnings("all")` annotation produced by `./gen-protobuf`.
  3. `RDFChangesWriterProtobuf` and `RDFPatchReaderProtobuf` support both length-delimited `InputStream`/`OutputStream` and zero-copy in-memory `Consumer<RDF_Patch_Row>` / `Iterator<RDF_Patch_Row>` / `RDF_Patch` adapters.
  4. `TestPatchIO_Protobuf` extends `AbstractTestPatchIO` and also tests message-level streaming, batch `RDF_Patch` conversion, RDF-star triple terms, and boundary error handling.

---

### PR 3: Jetty-Free `jena-grpc` Transport (`RDFLinkGrpc`, `RDFConnectionGrpc`, `RDFDatasetGrpcService`)
- **Worktree:** `/work/worktrees/jena-workspace/pr3-GH-4364-jena-grpc`
- **Module:** `jena-grpc` (plus root `pom.xml` and `jena-bom/pom.xml`)
- **Diff Commands:**
  ```bash
  cd /work/worktrees/jena-workspace/pr3-GH-4364-jena-grpc
  # Inspect PR 3 specific implementation changes on top of the PR 1 + PR 2 merge commit:
  git show aa929c2a075028ab4a11b623ebac7976f35eaea2
  # Or full diff against upstream/main:
  git diff upstream/main...HEAD -- ':!jena-arq/src/main/java/org/apache/jena/riot/protobuf/wire/PB_RDF.java' ':!LLM-SEED-INSTRUCTIONS.md'
  ```
- **Test & Verification Commands:**
  ```bash
  cd /work/worktrees/jena-workspace/pr3-GH-4364-jena-grpc
  mvn -B test -pl jena-arq,jena-rdfpatch,jena-grpc -Dtest=TS_RDFProtobuf,TS_RDFPatch,TS_JenaGrpc && ! git diff upstream/main...HEAD | grep -P '\xe2\x80[\x93\x94]'
  mvn -B dependency:tree -pl jena-grpc | grep -E 'jetty|servlet' || echo "Zero Jetty/Servlet dependencies verified"
  ```
- **Changed / Added Files in PR 3 Commit (`aa929c2a07`):**
  - `pom.xml`, `jena-bom/pom.xml`, `jena-grpc/pom.xml`
  - `jena-grpc/src/main/java/org/apache/jena/grpc/RDFSelectMessage.java`
  - `jena-grpc/src/main/java/org/apache/jena/grpc/RDFGrpcProtocol.java`
  - `jena-grpc/src/main/java/org/apache/jena/grpc/RDFDatasetGrpcService.java`
  - `jena-grpc/src/main/java/org/apache/jena/grpc/RDFGrpcServer.java`
  - `jena-grpc/src/main/java/org/apache/jena/grpc/QueryExecGrpc.java`
  - `jena-grpc/src/main/java/org/apache/jena/grpc/QueryExecGrpcBuilder.java`
  - `jena-grpc/src/main/java/org/apache/jena/grpc/UpdateExecGrpc.java`
  - `jena-grpc/src/main/java/org/apache/jena/grpc/UpdateExecGrpcBuilder.java`
  - `jena-grpc/src/main/java/org/apache/jena/grpc/RDFLinkGrpc.java`
  - `jena-grpc/src/main/java/org/apache/jena/grpc/RDFLinkGrpcBuilder.java`
  - `jena-grpc/src/main/java/org/apache/jena/grpc/RDFConnectionGrpc.java`
  - `jena-grpc/src/test/java/org/apache/jena/grpc/TS_JenaGrpc.java`
  - `jena-grpc/src/test/java/org/apache/jena/grpc/TestRDFLinkGrpcInProcess.java`
  - `jena-grpc/src/test/java/org/apache/jena/grpc/TestRDFLinkGrpcNetty.java`
  - `jena-grpc/src/test/java/org/apache/jena/grpc/TestRDFConnectionGrpc.java`
- **What to Verify:**
  1. Zero `org.eclipse.jetty` or `jakarta.servlet` dependencies in `jena-grpc`, and Maven Enforcer `DependencyConvergence` passes cleanly.
  2. `RDFLinkGrpc` passes all inherited `AbstractTestRDFLink` tests over both `InProcessServerBuilder` (`TestRDFLinkGrpcInProcess`) and Netty TCP (`TestRDFLinkGrpcNetty`), with `supportsAbort() == true`.
  3. `RDFConnectionGrpc` passes all inherited `AbstractTestRDFConnection` tests (`TestRDFConnectionGrpc`).
  4. Thread-bound transactions (`DatasetGraphTxnMem` and `TDB2`) work across multi-RPC client transactions (`begin` -> `load`/`update`/`query` -> `commit`/`abort` -> `end`) via `ServerTxnSession` single-thread pinning in `RDFDatasetGrpcService`.
  5. Streaming quad pattern lookup (`findStream` / `findStreamBatched` with `RDF_ANY` wildcards), streaming dataset ingest (`streamLoadDataset` / `streamLoadDatasetBatched`), streaming/batched `RDFPatch` application (`applyPatch` / `applyPatchBatch`), and live patch subscription (`subscribePatches`) work end-to-end.

---

## 4. How to Record Your Signoff

After independently inspecting the diffs and running the verification commands for all 3 PRs:

1. Check the checklist items in `/work/worktrees/jena-workspace/reviews/PR1-SIGNOFF.md`, `/work/worktrees/jena-workspace/reviews/PR2-SIGNOFF.md`, and `/work/worktrees/jena-workspace/reviews/PR3-SIGNOFF.md`.
2. Update your agent row (`fable`, `codex`, or `cursor`) in each of the 3 ledger files with:
   - Status: `` `APPROVED` `` (or `` `CHANGES_REQUESTED` `` if a blocking bug is found)
   - Reviewed Commit SHA: the exact 40-character `git rev-parse HEAD` SHA of each worktree
   - Date (UTC) in ISO-8601 format
   - Exact verification command run
   - Brief test summary / notes
3. Run `/work/worktrees/jena-workspace/sync.sh` to confirm all worktrees remain clean (`dirty=0`) and your signoffs are recorded.
