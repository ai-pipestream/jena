# GH-2549 Signoff: Custom `NodeFormatter` and Blank Node Serialization in `RDFWriter`

- **Branch**: `GH-2549-rdfwriter-bnode-formatter`
- **Base**: `main`
- **HEAD Commit**: `adb2281a950d8a3c02da62def82646fc98a908bf`
- **Upstream Issue**: `apache/jena#2549`
- **Fork PR**: `ai-pipestream/jena#6`

## Summary of Changes

1. **`RDFWriterBuilder` & `RIOT` (`jena-arq`)**:
   - Implements `RDFWriterBuilder.labels(NodeToLabel)` and `RDFWriterBuilder.formatter(NodeFormatter)` backed by `RIOT.symNodeToLabel` and `RIOT.symNodeFormatter`, preserving settings across `clone()` and `context(Context)` calls.
2. **Batch and Streaming Writer Integration**:
   - Updates `NodeFormatterNT`, `NTriplesWriter`, `NQuadsWriter`, `TurtleShell`, `WriterStreamRDFBase`, `WriterStreamRDFPlain`, and `StreamRDFWriter` to honor custom `NodeFormatter` and `NodeToLabel` policies.
   - Skips `findBNodesSyntax1()` in `TurtleShell` when custom node output is active so explicit blank node labels are emitted instead of `[]` abbreviations.
3. **Tests**:
   - Added 14 tests in `TestRDFWriter` covering N-Triples, N-Quads, Turtle, TriG, and `StreamRDFWriter`.
   - Verified locally (`1,379` tests, `0` failures) and on GitHub Actions CI ([Run 41200903336](https://github.com/ai-pipestream/jena/actions/runs/41200903336): **SUCCESS**).

## Agent Signoffs

- [x] **antigravity** (`argon`): Signed off at `adb2281a950d8a3c02da62def82646fc98a908bf` (2026-10-31).
- [ ] **fable**: Pending review.
- [ ] **codex**: Pending review.
- [ ] **cursor**: Pending review.
