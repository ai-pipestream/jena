#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FORK_DIR="${WORKSPACE_DIR}/fork"

echo "=== Fetching remotes in ${FORK_DIR} ==="
git -C "${FORK_DIR}" fetch origin --prune
git -C "${FORK_DIR}" fetch upstream --prune

echo ""
echo "=== Worktree Status ==="
git -C "${FORK_DIR}" worktree list

echo ""
echo "=== Branch Ahead/Behind vs upstream/main ==="
for wt in \
    "fork:main" \
    "pr1-GH-4364-protobuf-stream-adapters:GH-4364-protobuf-stream-adapters" \
    "pr2-GH-4364-protobuf-rdfpatch:GH-4364-protobuf-rdfpatch" \
    "pr3-GH-4364-jena-grpc:GH-4364-jena-grpc"; do
    dir="${wt%%:*}"
    branch="${wt##*:}"
    sha="$(git -C "${WORKSPACE_DIR}/${dir}" rev-parse --short HEAD)"
    counts="$(git -C "${WORKSPACE_DIR}/${dir}" rev-list --left-right --count "upstream/main...${branch}")"
    behind="$(echo "${counts}" | awk '{print $1}')"
    ahead="$(echo "${counts}" | awk '{print $2}')"
    dirty="$(git -C "${WORKSPACE_DIR}/${dir}" status --porcelain | wc -l)"
    printf "  %-42s branch=%-36s sha=%s ahead=%s behind=%s dirty=%s\n" \
        "${dir}" "${branch}" "${sha}" "${ahead}" "${behind}" "${dirty}"
done

echo ""
echo "=== 4-Agent Signoff Status (fable, antigravity, codex, cursor) ==="
for ledger in \
    "${WORKSPACE_DIR}/reviews/PR1-SIGNOFF.md" \
    "${WORKSPACE_DIR}/reviews/PR2-SIGNOFF.md" \
    "${WORKSPACE_DIR}/reviews/PR3-SIGNOFF.md"; do
    echo "  $(basename "${ledger}"):"
    grep -E '^\| \*\*(fable|antigravity|codex|cursor)\*\*' "${ledger}" | sed 's/^/    /'
done
