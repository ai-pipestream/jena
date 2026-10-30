# GH-4364 Review: claude

- Reviewer: `claude` (Claude Code, run on krick-1)
- Date (UTC): 2026-10-30
- Method: read `git diff upstream/main...HEAD` for each worktree (PR 2 excluding the generated `PB_RDF.java`, which was spot-checked), the `krickert-review` checklist, and `AGENTS.md`.
- Pinned SHAs reviewed: PR 1 `c24db5cc3533e4eeda1127c563a0c1e98719b335`, PR 2 `a3c165fc8a274953ec6900e9ff4141be1dc7b09e`, PR 3 `a112eb95072d4369dcadee832e3fa2f799e47f1f`. No PR branch was changed.

## Test status

Maven did not run in this session. The local auto-mode permission check blocked every `mvn` command (warmup and the three verification commands) in the fresh clone. The em/en dash check was run with `git diff upstream/main...HEAD | grep -cP '\xe2\x80[\x93\x94]'` and returned 0 for all three PRs. No `@author` tags were added. All new Java files in `jena-grpc` carry the ASF header.

Because the build was not run, no row below is `APPROVED`. The rows should be updated once the verification commands have been run.

## Applies to all three PRs

- `LLM-SEED-INSTRUCTIONS.md` (repo root): every PR branch HEAD (`c24db5cc35`, `a3c165fc8a`, `a112eb9507`) adds this file. It must not land upstream. Fix: drop the commit (or the file) when the landing commit is squashed.

## PR 1: `GH-4364-protobuf-stream-adapters`

Blocking: none found.

Minor:
- `StreamRDF2Protobuf.java:246`: `prefix()` now adds to the encoder `PrefixMap`, so triples and quads are emitted with `RDF_PrefixName` terms after a prefix row. This matches `StreamRDF2Thrift`, but it is a wire-level behavior change: rows can no longer be decoded independently or out of order, and a stateless `ProtobufConvert.convert(term)` with no prefix map fails on them. Fix: state in the Javadoc of `streamToRowConsumer` / `streamToBatchConsumer` that rows must be decoded in order by one decoder, and call it out in the PR description.
- Bug fixes folded in with no separate note: `streamToFile(filename, withValues)` and `writeRowSet(out, rowSet, withValues)` previously ignored `withValues`, and `StreamRDF2Protobuf.triple/quad` ignored `encodeValues`. Good fixes, but they change output bytes for existing callers. Fix: mention them in the PR description.
- `Binding2Protobuf.java:77`: the header-less `(Consumer<RDF_DataTuple>, Collection<Var>, boolean)` constructor does not reject `vars == null` while both sibling constructors do. Fix: validate `vars` the same way, or document why `null` is allowed here.
- `Protobuf2Binding.java:42`: the class goes from package-private to public with four public constructors plus `decode`, `getVars`, `getVarNames`. That is a large frozen API surface when `ProtobufRDF.readRowSet(...)` already covers the use case. Fix: keep the class package-private and expose only the `ProtobufRDF` facade plus `decodeVars`, or justify each public member.
- Null-message constants are used in some classes (`ERR_*`) but string literals in others (`Protobuf2Binding`, `ProtobufRDF`, `ProtobufRDF_Blk`). Fix: pick one style for the package.
- `ProtobufRDF_Blk.java` is changed but is not in the seed file's changed-file list. Fix: update the list.

## PR 2: `GH-4364-protobuf-rdfpatch`

Blocking: none found.

Minor:
- `RDFPatchReaderProtobuf.java:179`: `readHeader(InputStream)` reads and discards the first non-header row and, through `IO.ensureBuffered`, may read ahead past it, so the stream cannot be reused for the body. Same pattern as the Thrift reader, but worth one line of Javadoc ("consumes the input").
- `RDFChangesWriterProtobuf.java:49`: "Thread safety is implementation specific" is the interface phrasing; on a concrete class it reads oddly. Fix: "Instances are not thread safe."
- `PB_RDF.java` header says `Protobuf Java Version: 4.28.2`, while the seed instructions say it was regenerated with `protoc 4.34.1`. The file is consistent with upstream (4.28.2, runtime 4.36.2 is newer, so the gencode check passes). Fix: correct the seed/PR text.
- `gen-protobuf` now wraps `build-files/license-header` in `/* ... */`, which matches the checked-in file. Good, but it is a tooling change worth one line in the PR description.
- `PatchTxn` uses `TX = 0`. Inside a `oneof` presence is tracked, so this is safe, but a short comment in the `.proto` would stop a future editor from moving the enum out of the `oneof`.

## PR 3: `GH-4364-jena-grpc`

Blocking:
1. `RDFDatasetGrpcService.java:611-617` (`broadcastPatchRows`, also reached from `publishPatch` at line 209 and `subscribePatches` at line 608): `onNext` is called on each subscriber `StreamObserver` from whichever gRPC thread finished an `applyPatch`, with no synchronization. Two concurrent `applyPatch` calls (or a broadcast racing the ready marker) call `onNext` on the same observer concurrently, which the gRPC `StreamObserver` contract forbids, and rows from two patches can interleave (TX, TX, A, B, TC, TC) or arrive out of commit order. Since the broadcast happens after `Txn.executeWrite` returns, commit order is not preserved even without interleaving. For a replication feed this gives replicas a wrong history. Fix: serialize broadcasts (one lock, or a single-threaded publisher) and publish inside the write critical section, or after commit while holding an ordering lock. Add a test with concurrent `applyPatch` calls and a subscriber.
2. `RDFDatasetGrpcService.java:590-600` (`applyPatchRows`): when the call carries a remote transaction id, the rows are applied inside the open session transaction and broadcast immediately, before `CommitTxn`. If the client later aborts, subscribers have already received changes that never committed. Fix: buffer rows per `ServerTxnSession` and broadcast on commit (drop on abort); add a test that aborts and checks the subscriber saw nothing.
3. `RDFDatasetGrpcService.java:631, 794-796` (`beginTxn` / `ServerTxnSession`): a remote transaction lives until the client calls `EndTxn`. There is no idle timeout, no limit on open sessions, and no tie to the client's call or channel lifetime. A client that crashes after `BeginTxn(WRITE)` holds the dataset's writer lock forever (on TDB2 every other writer blocks), and every `BeginTxn` starts a new platform thread. The Netty server has no authentication, so any client that can connect can do this. Fix: add an idle timeout (and a max-sessions limit) that aborts and ends the session, document the security model, and add a test for the reaper.

Minor:
- No `.proto` service definition. The service is hand-built `MethodDescriptor`s, and `RDFSelectMessage` uses a custom marshaller that is wire-compatible with a `oneof { RDF_VarTuple header = 1; RDF_DataTuple row = 2; }` but is not declared anywhere. Non-Java gRPC clients cannot generate stubs. Fix: add a `jena-grpc` `.proto` (service plus the select message) even if the Java side keeps the hand-built descriptors, or declare `RDF_SelectRow` in `binary-rdf.proto`.
- No flow control on server streams: `findStream`, `getDataset`, `querySelect` and the others call `onNext` in a loop without checking `ServerCallStreamObserver.isReady()`, so a large result is buffered fully in server memory while the read transaction stays open. Client-streaming ingest (`collectRowsObserver` at line 713, `applyPatch`, `loadDatasetBatched`) buffers every row in a `List` before applying. "Streaming" is true on the wire but not in memory. Fix: use `isReady`/`setOnReadyHandler`, apply ingest batches incrementally inside one write txn (or document the memory bound and set `maxInboundMessageSize`).
- `QueryExecGrpc.java:271,277`: `abort()` and `close()` only set a flag. They do not cancel the in-flight call, so a caller that stops iterating a large `SELECT` leaves the blocking stream open and the server streaming. Fix: keep the `ClientCall`/`Context.CancellableContext` and cancel it in `abort()`/`close()`.
- `RDFLinkGrpc.java:749`: `promote()` only promotes the local lifecycle tracker. The server session stays in its original mode, so `isInTransaction()`/`transactionMode()` report WRITE while the server still holds a READ transaction. Fix: add a `PromoteTxn` RPC or return `false` for remote promotion.
- `RDFLinkGrpc.java` `commit()`/`abort()`: the local lifecycle is committed before the remote call. If `CommitTxn` fails, the local state says committed while the server state is unknown. Fix: call the server first.
- `RDFLinkGrpc.java:404`: `subscribePatches` swallows `onError`, so the target never learns the feed died. Fix: surface it (callback, or a `PatchSubscription.error()`).
- `RDFGrpcProtocol.java:400`: `Status.INTERNAL.withDescription(ex.getMessage())` sends server exception messages to remote clients. Fix: a generic description for INTERNAL, and log the detail server-side.
- `ServerTxnSession.ended` (line 792) is read and written from different gRPC threads without `volatile`.
- `QueryExecGrpc.java:160`: wrong indentation on the `return`.
- Builders use `Objects.requireNonNull` (NPE) while the rest of the module throws `IllegalArgumentException`. Pick one.
- `pom.xml`: `grpc-netty-shaded` is a compile dependency, so every `jena-grpc` user pulls in the shaded Netty jar even for in-process use. `grpc-context`, `grpc-util` and `grpc-protobuf-lite` are added to `dependencyManagement` but not used. Fix: consider making Netty `optional` or `runtime`, and drop the unused entries.
- `jena-grpc/testing/RDFConnection` and `testing/RDFLink` copy fixtures from `jena-rdfconnection`. Fix: load them from the `jena-rdfconnection` test jar or a shared path.
- No `package-info.java` for `org.apache.jena.grpc`.

Verified OK: `jena-grpc` declares no Jetty or Servlet dependency in its POM (the `dependency:tree` check itself was not run), `RDFLinkGrpc` and `RDFConnectionGrpc` extend the standard `AbstractTestRDFLink` / `AbstractTestRDFConnection` suites, and server-side operations run inside `Txn.executeRead` / `Txn.executeWrite`, or on the session's pinned thread when a transaction id is present.
