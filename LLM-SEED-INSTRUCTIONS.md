# Agent Seeding Instructions: Apache Jena Protobuf & gRPC Review (`GH-4364` / `JENA-2167`)

## 1. Mission & Role

You are an independent review agent (`fable`, `codex`, or `cursor`) reviewing and verifying a 3-part stacked implementation for Apache Jena ticket **GH-4364** (*Completing Protobuf / gRPC support (`JENA-2167`): message-level `StreamRDF`/`RowSet` adapters, Protobuf `RDFPatch`, and `RDFLink` over gRPC*).

All branches and the signoff ledgers are hosted on the fork **`https://github.com/ai-pipestream/jena.git`**.

### Strict Safety Guardrails (Non-Negotiable)
- **Never push or write to `upstream` (`apache/jena`).**
- **Do NOT open any GitHub Pull Requests** (neither on `apache/jena` nor on `ai-pipestream/jena`) unless explicitly instructed by the user.
- **Do NOT tag or mention any GitHub users** (never use `@username` in any commit, comment, or document).
- **Never use em dashes or en dashes** anywhere in code, Javadoc, comments, commit messages, or review markdown. Use ASCII hyphens (`-` or `--`), commas, colons, or parentheses.
- **Do NOT modify `fork/main`**.
- **Keep the 3 PR branches untouched unless fixing a Blocking bug:** Record your signoffs on the dedicated **`GH-4364-reviews`** branch so the 3 PR branch SHAs remain stable across all 4 agent reviews.

---

## 2. Step-by-Step Bootstrap on a Fresh Machine (Start Here)

If you are starting on a fresh machine that does not yet have the workspace set up, run the following commands to clone the fork, configure `upstream`, check out the 3 PR worktrees plus the `GH-4364-reviews` ledger worktree, and populate the top-level workspace files:

```bash
export WORKSPACE_DIR="${WORKSPACE_DIR:-/work/worktrees/jena-workspace}"
mkdir -p "${WORKSPACE_DIR}"

# 1. Clone the fork (origin = ai-pipestream/jena) into fork/ and add upstream (apache/jena)
git clone https://github.com/ai-pipestream/jena.git "${WORKSPACE_DIR}/fork"
git -C "${WORKSPACE_DIR}/fork" remote add upstream https://github.com/apache/jena.git
git -C "${WORKSPACE_DIR}/fork" fetch origin --prune
git -C "${WORKSPACE_DIR}/fork" fetch upstream --prune

# 2. Create the 3 PR worktrees tracking the fork branches
git -C "${WORKSPACE_DIR}/fork" worktree add -B GH-4364-protobuf-stream-adapters \
  "${WORKSPACE_DIR}/pr1-GH-4364-protobuf-stream-adapters" origin/GH-4364-protobuf-stream-adapters

git -C "${WORKSPACE_DIR}/fork" worktree add -B GH-4364-protobuf-rdfpatch \
  "${WORKSPACE_DIR}/pr2-GH-4364-protobuf-rdfpatch" origin/GH-4364-protobuf-rdfpatch

git -C "${WORKSPACE_DIR}/fork" worktree add -B GH-4364-jena-grpc \
  "${WORKSPACE_DIR}/pr3-GH-4364-jena-grpc" origin/GH-4364-jena-grpc

# 3. Check out the GH-4364-reviews branch (contains AGENTS.md, sync.sh, and reviews/PR*-SIGNOFF.md)
git -C "${WORKSPACE_DIR}/fork" worktree add -B GH-4364-reviews \
  "${WORKSPACE_DIR}/reviews-branch" origin/GH-4364-reviews

cp "${WORKSPACE_DIR}/reviews-branch/AGENTS.md" "${WORKSPACE_DIR}/AGENTS.md"
cp "${WORKSPACE_DIR}/reviews-branch/sync.sh" "${WORKSPACE_DIR}/sync.sh"
chmod +x "${WORKSPACE_DIR}/sync.sh"
mkdir -p "${WORKSPACE_DIR}/reviews"
cp "${WORKSPACE_DIR}/reviews-branch/reviews/"*.md "${WORKSPACE_DIR}/reviews/"
```

### One-Time Local Maven Snapshot Warmup (Required on a Fresh Machine)

Because targeted test runs (`mvn test -pl jena-arq`) depend on sibling `6.3.0-SNAPSHOT` reactor jars (`jena-base`, `jena-core`, `jena-iri`, `jena-tdb2`, `jena-rdfconnection`), install the base reactor artifacts into your local `~/.m2/repository` once before running targeted module tests:

```bash
cd "${WORKSPACE_DIR}/pr3-GH-4364-jena-grpc"
mvn -B -DskipTests -Dmaven.javadoc.skip=true install -pl jena-tdb2,jena-rdfconnection -am
```

---

## 3. Branch Topology & Pinned Commit SHAs (Option A)

All branches live on `origin` (`https://github.com/ai-pipestream/jena.git`):

| Worktree / Role | Branch Name | Base | Branch HEAD SHA | Code Implementation Commit SHA |
| :--- | :--- | :--- | :--- | :--- |
| **PR 1** (`pr1-GH-4364-protobuf-stream-adapters`) | `GH-4364-protobuf-stream-adapters` | `upstream/main` (`73713dcc54`) | `c24db5cc3533e4eeda1127c563a0c1e98719b335` | `03c2037d3d4041fdf694fc88228d68d802cc6af6` |
| **PR 2** (`pr2-GH-4364-protobuf-rdfpatch`) | `GH-4364-protobuf-rdfpatch` | `upstream/main` (`73713dcc54`) | `a3c165fc8a274953ec6900e9ff4141be1dc7b09e` | `451eeb0cdfed2ab907b19930d893a96e15ef2059` |
| **PR 3** (`pr3-GH-4364-jena-grpc`) | `GH-4364-jena-grpc` | Merge of PR 1 + PR 2 (`c9607ac2da`) | `a112eb95072d4369dcadee832e3fa2f799e47f1f` | `aa929c2a075028ab4a11b623ebac7976f35eaea2` |
| **Reviews & Ledgers** (`reviews-branch`) | `GH-4364-reviews` | `upstream/main` (`73713dcc54`) | (Updated as agents sign off) | Holds `AGENTS.md`, `sync.sh`, `reviews/PR1-SIGNOFF.md`, `reviews/PR2-SIGNOFF.md`, `reviews/PR3-SIGNOFF.md` |

Before reviewing code, read:
1. `${WORKSPACE_DIR}/AGENTS.md`
2. `${WORKSPACE_DIR}/fork/CONTRIBUTING.md`
3. `/home/krickert/.claude/skills/krickert-review/SKILL.md` (if present on your machine)

---

## 4. Detailed Review Scope & Verification Commands per PR

### PR 1: In-Memory Protobuf `StreamRDF` and `RowSet` Message Adapters
- **Worktree:** `${WORKSPACE_DIR}/pr1-GH-4364-protobuf-stream-adapters`
- **Module:** `jena-arq` (`org.apache.jena.riot.protobuf`)
- **Diff Command:**
  ```bash
  cd "${WORKSPACE_DIR}/pr1-GH-4364-protobuf-stream-adapters"
  git show 03c2037d3d4041fdf694fc88228d68d802cc6af6
  ```
- **Test & Verification Command:**
  ```bash
  cd "${WORKSPACE_DIR}/pr1-GH-4364-protobuf-stream-adapters"
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
- **Worktree:** `${WORKSPACE_DIR}/pr2-GH-4364-protobuf-rdfpatch`
- **Modules:** `jena-arq` and `jena-rdfpatch`
- **Diff Commands:**
  ```bash
  cd "${WORKSPACE_DIR}/pr2-GH-4364-protobuf-rdfpatch"
  # Inspect hand-written changes (excluding generated PB_RDF.java):
  git show 451eeb0cdfed2ab907b19930d893a96e15ef2059 -- ':!jena-arq/src/main/java/org/apache/jena/riot/protobuf/wire/PB_RDF.java'
  ```
- **Test & Verification Command:**
  ```bash
  cd "${WORKSPACE_DIR}/pr2-GH-4364-protobuf-rdfpatch"
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
- **Worktree:** `${WORKSPACE_DIR}/pr3-GH-4364-jena-grpc`
- **Module:** `jena-grpc` (plus root `pom.xml` and `jena-bom/pom.xml`)
- **Diff Commands:**
  ```bash
  cd "${WORKSPACE_DIR}/pr3-GH-4364-jena-grpc"
  # Inspect PR 3 specific implementation changes on top of the PR 1 + PR 2 merge commit:
  git show aa929c2a075028ab4a11b623ebac7976f35eaea2
  ```
- **Test & Verification Commands:**
  ```bash
  cd "${WORKSPACE_DIR}/pr3-GH-4364-jena-grpc"
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

## 5. How to Record & Push Your Signoff (`GH-4364-reviews`)

After independently inspecting the diffs and running the verification commands for all 3 PRs:

1. Pull the latest `GH-4364-reviews` branch in `${WORKSPACE_DIR}/reviews-branch`:
   ```bash
   git -C "${WORKSPACE_DIR}/reviews-branch" pull --rebase origin GH-4364-reviews
   ```
2. Update your agent row (`fable`, `codex`, or `cursor`) in:
   - `${WORKSPACE_DIR}/reviews-branch/reviews/PR1-SIGNOFF.md`
   - `${WORKSPACE_DIR}/reviews-branch/reviews/PR2-SIGNOFF.md`
   - `${WORKSPACE_DIR}/reviews-branch/reviews/PR3-SIGNOFF.md`
   with:
   - Status: `` `APPROVED` `` (or `` `CHANGES_REQUESTED` `` if a blocking bug is found)
   - Reviewed Commit SHA: the 40-character SHA of each PR worktree (`c24db5cc3533e4eeda1127c563a0c1e98719b335` / `03c2037d3d4041fdf694fc88228d68d802cc6af6` for PR 1; `a3c165fc8a274953ec6900e9ff4141be1dc7b09e` / `451eeb0cdfed2ab907b19930d893a96e15ef2059` for PR 2; `a112eb95072d4369dcadee832e3fa2f799e47f1f` / `aa929c2a075028ab4a11b623ebac7976f35eaea2` for PR 3)
   - Date (UTC) in ISO-8601 format
   - Exact verification command run
   - Brief test summary / notes
3. Copy the updated ledger files to `${WORKSPACE_DIR}/reviews/`, commit them to `GH-4364-reviews`, and push `GH-4364-reviews` to `origin` (`ai-pipestream/jena`):
   ```bash
   cp "${WORKSPACE_DIR}/reviews-branch/reviews/"*.md "${WORKSPACE_DIR}/reviews/"
   git -C "${WORKSPACE_DIR}/reviews-branch" add reviews/
   git -C "${WORKSPACE_DIR}/reviews-branch" commit -s -m "GH-4364: Record <agent-name> review signoff"
   git -C "${WORKSPACE_DIR}/reviews-branch" push origin GH-4364-reviews
   "${WORKSPACE_DIR}/sync.sh"
   ```
