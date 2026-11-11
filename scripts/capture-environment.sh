#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
output="${1:-results/environment.txt}"
mkdir -p -- "$(dirname -- "$output")"
{
  date -u +'%Y-%m-%dT%H:%M:%SZ'
  uname -a
  git rev-parse HEAD 2>/dev/null || true
  git status --short 2>/dev/null || true
  java -version 2>&1
  mvn -version
  if command -v docker >/dev/null 2>&1; then
    docker version 2>&1 || true
  fi
} > "$output"
echo "Environment written to $output"
