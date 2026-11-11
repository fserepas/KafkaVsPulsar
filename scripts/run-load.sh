#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
jar="$repo_root/modules/mqtt-load-generator/target/MqttLoadGenerator-1.0-SNAPSHOT.jar"
if [[ ! -f "$jar" ]]; then
  echo "Build first: ./scripts/build.sh" >&2
  exit 1
fi
exec java -cp "$jar" org.example.MqttLoadGenerator "$@"
