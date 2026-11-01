# GH-4362 Signoff: Implement `OpDatasetNames` in `OpExecutor` and Safe Unwrap in `OpExecutorTDB2`

- **Branch**: `GH-4362-op-dataset-names`
- **Base**: `main`
- **HEAD Commit**: `07b6cdaf417e754ec78c0db17745e93c7e381bbc`
- **Upstream Issue**: `apache/jena#4362`
- **Fork PR**: `ai-pipestream/jena#4`

## Summary of Changes

1. **`OpExecutor.execute(OpDatasetNames, QueryIterator)` (`jena-arq`)**:
   - Evaluates concrete graph nodes via `GraphOps.containsGraph(ds, graphNode)`, returning `input` if present or closing `input` and returning `QueryIterNullIterator` if absent.
   - Evaluates variable graph nodes via `QueryIterRepeatApply`, looking up any existing binding for `var` and either verifying graph existence (`QueryIterSingleton`) or extending with `ds.listGraphNodes()` (`QueryIterExtendByVar`).
2. **`OpExecutorTDB2` & `SolverLibTDB` (`jena-tdb2`)**:
   - Replaces raw `(DatasetGraphTDB)` cast with `TDBInternal.getDatasetGraphTDB(execCxt.getDataset())`, delegating to `super.execute(dsNames, input)` when the dataset is a non-TDB wrapper.
   - Preserves input bindings in `SolverLibTDB.graphNames` when iterating graph names or verifying pre-bound graph variables.
3. **Tests**:
   - `TestOpDatasetNames` (`jena-arq`, wired into `TS_Engine`) and TDB2 tests in `Test_SPARQL_TDB` (`jena-tdb2`).
   - Verified locally (`291` tests, `0` failures) and on GitHub Actions CI ([Run 41201230883](https://github.com/ai-pipestream/jena/actions/runs/41201230883): **SUCCESS**).

## Agent Signoffs

- [x] **antigravity** (`argon`): Signed off at `07b6cdaf417e754ec78c0db17745e93c7e381bbc` (2026-10-31).
- [ ] **fable**: Pending review.
- [ ] **codex**: Pending review.
- [ ] **cursor**: Pending review.
