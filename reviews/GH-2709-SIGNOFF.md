# GH-2709 Signoff: Configurable Indentation Width in Turtle and TriG Writers

- **Branch**: `GH-2709-turtle-indent-width`
- **Base**: `main`
- **HEAD Commit**: `9afab823be02457670945bb406a5b7b89a99d495`
- **Upstream Issue**: `apache/jena#2709`
- **Fork PR**: `ai-pipestream/jena#5`

## Summary of Changes

1. **`RIOT.symTurtleIndentWidth` & `WriterLib.indentWidth` (`jena-arq`)**:
   - Adds `RIOT.symTurtleIndentWidth` (`http://jena.apache.org/riot/turtle#indentWidth`) and `WriterConst.INDENT_LONG = 2`.
   - Adds `WriterLib.indentWidth(Context, int)` accepting `Number` or numeric `String` values `>= 0` and throwing `RiotException` on negative or non-numeric values.
2. **Writer Integration (`TurtleShell`, `WriterStreamRDFBlocks`, `RDFWriterRegistry`)**:
   - Applies configured indentation width across `ShellGraphWide`, `ShellGraphLong`, and `WriterStreamRDFBlocks` (`TURTLE_PRETTY`, `TURTLE_LONG`, `TURTLE_BLOCKS`, `TRIG_PRETTY`, `TRIG_LONG`, `TRIG_BLOCKS`).
   - Registers `RDFFormat.TRIG_LONG` in `RDFWriterRegistry.registerWriterDataset`.
3. **Tests**:
   - Added unit tests in `TestTurtleWriter`, `TestRiotWriterDataset`, and `TestWriterRegistration`.
   - Verified locally (`1,001` tests, `0` failures) and on GitHub Actions CI ([Run 41200976922](https://github.com/ai-pipestream/jena/actions/runs/41200976922): **SUCCESS**).

## Agent Signoffs

- [x] **antigravity** (`argon`): Signed off at `9afab823be02457670945bb406a5b7b89a99d495` (2026-10-31).
- [ ] **fable**: Pending review.
- [ ] **codex**: Pending review.
- [ ] **cursor**: Pending review.
