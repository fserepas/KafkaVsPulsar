#!/usr/bin/env python3
"""Validate every scenario's merged Compose configuration without starting containers."""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess

ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--compose-command', default='docker compose')
    args = parser.parse_args()
    runs = json.loads((ROOT / 'experiments/runs.json').read_text())['runs']
    assets = [x['path'] for x in json.loads((ROOT / 'assets/runtime-manifest.json').read_text())['files']]
    checked = 0
    for run in runs:
        for monitoring in [False, True]:
            cmd = shlex.split(args.compose_command) + ['--env-file', str(ROOT / '.env.example'), '-p', 'validation', '-f', str(ROOT / 'deployment' / run['platform'] / 'compose.yaml')]
            if monitoring:
                cmd += ['-f', str(ROOT / 'deployment/monitoring.yaml'), '--profile', 'monitoring']
            cmd += ['--profile', run['database'], '--profile', 'load']
            if run['processing'] in ['ksqldb', 'streams']:
                cmd += ['--profile', run['processing']]
            result = subprocess.run(cmd + ['config', '--format', 'json'], env=dict(os.environ, RUN_INSTANCE='validation'), capture_output=True, text=True, check=True)
            config = json.loads(result.stdout)
            services = config['services']
            assert run['database'] in services
            assert ('mysql' not in services) == (run['database'] == 'postgres')
            assert ('kafka-streams' in services) == (run['processing'] == 'streams')
            assert ('ksqldb-server' in services) == (run['processing'] == 'ksqldb')
            assert ('prometheus' in services) == monitoring
            for service in services.values():
                for mount in service.get('volumes', []):
                    if mount['type'] != 'bind':
                        continue
                    path = Path(mount['source'])
                    if not path.is_relative_to(ROOT):
                        continue  # Host monitoring mounts belong to the Docker daemon host.
                    relative = str(path.relative_to(ROOT))
                    if relative.startswith(('.generated/', 'modules/')):
                        continue  # Rendered trial config or generated application JAR.
                    assert path.exists() or any(x == relative or x.startswith(relative + '/') for x in assets), relative
            checked += 1
    print(f'Validated {checked} Compose configurations for all 19 scenarios; no containers started.')

if __name__ == '__main__':
    main()
