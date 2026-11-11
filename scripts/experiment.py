#!/usr/bin/env python3
"""Plan and operate one isolated experiment from the manuscript's R1-R19 matrix."""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / '.state/active.json'
RUNS = {r['id']: r for r in json.loads((ROOT / 'experiments/runs.json').read_text())['runs']}
PIPELINE_SERVICES = 'kafka|zookeeper|schema-registry|mqtt-broker|kafka-connect|ksqldb-server|kafka-streams|pulsar|postgres|mysql|load-generator'

def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()

def read_env():
    if not (ROOT / '.env').exists():
        raise RuntimeError('Copy .env.example to .env before executing a trial.')
    env = {}
    for line in (ROOT / '.env').read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            env[key.strip()] = value.strip().strip('\"\'')
    if not env.get('DB_PASSWORD'):
        raise RuntimeError('DB_PASSWORD is missing from .env')
    return env

def save(state):
    STATE.parent.mkdir(exist_ok=True)
    STATE.write_text(json.dumps(state, indent=2) + '\n')
    (ROOT / state['output'] / 'manifest.json').write_text(json.dumps(state, indent=2) + '\n')

def active(run_id):
    if not STATE.exists():
        raise RuntimeError('No active trial. Run the up action first.')
    s = json.loads(STATE.read_text())
    if s['run']['id'] != run_id:
        raise RuntimeError(f"{s['run']['id']} is active. Collect and stop it before selecting {run_id}.")
    return s

def process(cmd, state, capture=False, log=None, check=True):
    print('+ ' + shlex.join(str(x) for x in cmd), flush=True)
    env = dict(os.environ)
    env.update(read_env())
    env.update(COMPOSE_PROJECT_NAME=state['project'], RUN_INSTANCE=state['instance'])
    if log is not None:
        with Path(log).open('w') as out:
            result = subprocess.run(cmd, cwd=ROOT, env=env, stdout=out, stderr=subprocess.STDOUT, text=True)
    else:
        result = subprocess.run(cmd, cwd=ROOT, env=env, text=True, capture_output=capture)
    if check and result.returncode:
        message = (result.stderr or result.stdout or '') if capture else ''
        raise RuntimeError(f'Command exited with {result.returncode}: {message[-1500:]}')
    return result

def compose(state, *args):
    cmd = ['docker', 'compose', '--env-file', str(ROOT / '.env'), '-p', state['project'], '-f', str(ROOT / 'deployment' / state['run']['platform'] / 'compose.yaml')]
    if state['monitoring']:
        cmd += ['-f', str(ROOT / 'deployment/monitoring.yaml'), '--profile', 'monitoring']
    cmd += ['--profile', state['run']['database'], '--profile', 'load']
    if state['run']['processing'] in ['ksqldb', 'streams']:
        cmd += ['--profile', state['run']['processing']]
    return cmd + list(args)

def request(url, data=None, method=None):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method=method, headers={'Content-Type': 'application/json'})
    # These APIs are always localhost; never send local API requests through a proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(req, timeout=10) as res:
        return json.load(res)

def wait_until(description, fn, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = fn()
            if value:
                return value
        except (OSError, RuntimeError, ValueError, urllib.error.URLError) as exc:
            last = str(exc)
        time.sleep(2)
    raise RuntimeError(f'Timed out waiting for {description}. {last or "Inspect service logs."}')

def db_command(state, sql, csv=False):
    if state['run']['database'] == 'postgres':
        args = ['psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'user1', '-d', 'mqtt_db']
        args += ['--csv'] if csv else ['-t', '-A']
        return compose(state, 'exec', '-T', 'postgres', *args, '-c', sql)
    args = ['--batch', '--raw']
    if not csv:
        args += ['--skip-column-names']
    return compose(state, 'exec', '-T', 'mysql', 'sh', '-c', 'MYSQL_PWD="$MYSQL_PASSWORD" exec mysql --user="$MYSQL_USER" --database="$MYSQL_DATABASE" "$@"', '--', *args, '-e', sql)

def table(state):
    return state['run']['platform'] + ('_to_db_temp' if state['run']['processing'] == 'raw' else '_processed_to_db_temp')

def count_rows(state):
    return int(process(db_command(state, f'SELECT COUNT(*) FROM {table(state)};'), state, capture=True).stdout.strip())

def check_assets():
    manifest = json.loads((ROOT / 'assets/runtime-manifest.json').read_text())
    missing = [x['path'] for x in manifest['files'] if not (ROOT / x['path']).is_file()]
    if missing:
        raise RuntimeError('Install runtime-assets.zip first with scripts/import-runtime-assets.py')
    for module, jar in [('mqtt-load-generator', 'MqttLoadGenerator'), ('pulsar-functions', 'PulsarFunction'), ('kafka-streams', 'KafkaStreamsTemperatureProcessor')]:
        if not (ROOT / f'modules/{module}/target/{jar}-1.0-SNAPSHOT.jar').is_file():
            raise RuntimeError('Build the Java modules first with scripts/build.sh')

def up(run, smoke=False, monitoring=True):
    if STATE.exists():
        raise RuntimeError('A trial is active. Collect and stop it before creating a fresh isolated trial.')
    if not shutil.which('docker'):
        raise RuntimeError('Docker Engine/Desktop and the Compose v2 plugin are required.')
    check_assets()
    read_env()
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%f')
    actual = dict(run)
    if smoke:
        actual.update(producer_count=2, messages_per_producer=10, delay_between_messages_ms=100, nominal_messages_per_second=20, attempted_total=20)
    instance = run['id'].lower() + '-' + stamp.lower()
    output = ROOT / 'results' / run['id'] / stamp
    output.mkdir(parents=True)
    state = {'run': actual, 'paper_settings': run, 'smoke_test': smoke, 'project': 'iot-' + instance, 'instance': instance, 'output': str(output.relative_to(ROOT)), 'monitoring': monitoring, 'created_utc': now(), 'configured': False, 'workload_started_utc': None, 'workload_finished_utc': None}
    # Only the selected sink is rendered and deployed. JSON is valid YAML for Pulsar CLI.
    generated = ROOT / '.generated' / instance / 'pulsar'
    generated.mkdir(parents=True, mode=0o755)
    if run['platform'] == 'pulsar':
        fname = 'jdbc_sink_pulsar_' + ('functions_' if run['processing'] == 'functions' else '') + 'to_' + run['database'] + '_temp.json'
        config = json.loads((ROOT / 'deployment/pulsar/sinks' / fname).read_text())
        config['configs']['password'] = read_env()['DB_PASSWORD']
        (generated / 'sink.yaml').write_text(json.dumps(config, indent=2) + '\n')
    save(state)
    try:
        process(compose(state, 'config', '--quiet'), state)
        process(compose(state, 'config'), state, log=output / 'compose.resolved.local.yaml')
        # Resolved configuration contains local credentials and remains in ignored results.
        (output / 'compose.resolved.local.yaml').chmod(0o600)
        process(compose(state, 'build', 'load-generator', *(['kafka-streams'] if run['processing'] == 'streams' else [])), state)
        services = (['zookeeper', 'kafka', 'schema-registry', 'mqtt-broker', 'kafka-connect'] if run['platform'] == 'kafka' else ['pulsar']) + [run['database']]
        if monitoring:
            services += ['node-exporter', 'cadvisor', 'prometheus', 'grafana']
        process(compose(state, 'up', '-d', *services), state)
        print('Infrastructure started. Next: configure, run, collect, down. Output:', output)
    except Exception:
        print('Trial state retained for diagnostics. Use the down action when finished.', file=sys.stderr)
        raise

def kafka_connector(state, name, config):
    request('http://127.0.0.1:8083/connectors/' + name + '/config', config, 'PUT')
    def running():
        status = request('http://127.0.0.1:8083/connectors/' + name + '/status')
        return status.get('connector', {}).get('state') == 'RUNNING' and bool(status.get('tasks')) and all(t['state'] == 'RUNNING' for t in status['tasks'])
    wait_until(name, running)

def configure(state):
    if state['configured']:
        raise RuntimeError('Trial already configured; run or collect it. Use a fresh up/down cycle for another trial.')
    if state['workload_started_utc']:
        raise RuntimeError('A workload was already started in this trial.')
    run = state['run']
    wait_until('database initialization', lambda: count_rows(state) == 0)
    if run['platform'] == 'kafka':
        wait_until('Kafka Connect', lambda: request('http://127.0.0.1:8083/connector-plugins'))
        wait_until('Schema Registry', lambda: isinstance(request('http://127.0.0.1:8081/subjects'), list))
        for topic in ['temp', 'processed_ksqldb_temp', 'processed_KafkaStreams_temp']:
            process(compose(state, 'exec', '-T', 'kafka', 'kafka-topics', '--bootstrap-server', 'kafka:9092', '--create', '--if-not-exists', '--topic', topic, '--partitions', '1', '--replication-factor', '1'), state)
        source = json.loads((ROOT / 'deployment/kafka/connectors/mqtt_source_temp.json').read_text())
        kafka_connector(state, source.pop('name'), source)
        processed = run['processing'] != 'raw'
        fname = f'jdbc_sink_temp_{"processed" if processed else "raw"}_{run["database"]}.json'
        sink = json.loads((ROOT / 'deployment/kafka/connectors' / fname).read_text())
        sink['connection.password'] = read_env()['DB_PASSWORD']
        sink.pop('topics.regex', None)
        sink['topics'] = 'temp' if not processed else 'processed_ksqldb_temp' if run['processing'] == 'ksqldb' else 'processed_KafkaStreams_temp'
        kafka_connector(state, sink.pop('name'), sink)
        if processed:
            service = 'ksqldb-server' if run['processing'] == 'ksqldb' else 'kafka-streams'
            process(compose(state, 'up', '-d', service), state)
            def consumer_ready():
                result = process(compose(state, 'exec', '-T', 'kafka', 'kafka-consumer-groups', '--bootstrap-server', 'kafka:9092', '--all-groups', '--describe', '--state'), state, capture=True)
                needle = 'ksql' if run['processing'] == 'ksqldb' else 'benchmark-temperature'
                return any(needle in line and 'Stable' in line for line in result.stdout.splitlines())
            wait_until('stream processor consumer assignment', consumer_ready)
    else:
        admin = lambda *args: compose(state, 'exec', '-T', 'pulsar', 'bin/pulsar-admin', *args)
        wait_until('Pulsar broker', lambda: process(admin('clusters', 'list'), state, capture=True).returncode == 0)
        # NARs are isolated classloaders: sinks use their bundled JDBC drivers.
        process(admin('functions', 'create', '--tenant', 'public', '--namespace', 'default', '--name', 'temp-schema-fn', '--jar', '/pulsar/functions/PulsarFunction-1.0-SNAPSHOT.jar', '--classname', 'org.example.TempSchemaEnforcerFunction', '--inputs', 'persistent://public/default/temp', '--custom-serde-inputs', json.dumps({'persistent://public/default/temp': 'org.example.schema.SensorReadingTempJsonSerde'}), '--output', 'persistent://public/default/temp_schema', '--parallelism', '1'), state)
        if run['processing'] == 'functions':
            process(admin('functions', 'create', '--tenant', 'public', '--namespace', 'default', '--name', 'temp-processing-fn', '--jar', '/pulsar/functions/PulsarFunction-1.0-SNAPSHOT.jar', '--classname', 'org.example.TemperatureProcessorFunction', '--inputs', 'persistent://public/default/temp_schema', '--output', 'persistent://public/default/processed_temp', '--parallelism', '1'), state)
        process(admin('sinks', 'create', '--sink-config-file', '/pulsar/conf/benchmark/sink.yaml', '--parallelism', '1'), state)
        fname = 'jdbc_sink_pulsar_' + ('functions_' if run['processing'] == 'functions' else '') + 'to_' + run['database'] + '_temp'
        for kind, name in [('functions', 'temp-schema-fn')] + ([('functions', 'temp-processing-fn')] if run['processing'] == 'functions' else []) + [('sinks', fname)]:
            def running(kind=kind, name=name):
                status = json.loads(process(admin(kind, 'status', '--tenant', 'public', '--namespace', 'default', '--name', name), state, capture=True).stdout)
                return status.get('numRunning', 0) >= 1 and all(i.get('status', {}).get('running', False) for i in status.get('instances', []))
            wait_until(name, running)
    state['configured'] = True
    state['configured_utc'] = now()
    save(state)
    print('Selected pipeline is configured and ready for the workload.')

def run_workload(state, drain_timeout):
    if not state['configured'] or state['workload_started_utc']:
        raise RuntimeError('Configure first; each trial can execute its workload only once.')
    r = state['run']
    args = ['--broker=tcp://' + ('mqtt-broker' if r['platform'] == 'kafka' else 'pulsar') + ':1883', '--topic=' + ('mqtt/temp' if r['platform'] == 'kafka' else 'persistent://public/default/temp'), f'--producer-count={r["producer_count"]}', f'--messages-per-producer={r["messages_per_producer"]}', f'--delay-between-messages={r["delay_between_messages_ms"]}', f'--payload-size={r["payload_size_argument"]}']
    state['workload_started_utc'] = now()
    state['generator_arguments'] = args
    save(state)
    result = process(compose(state, 'run', '--rm', '--no-deps', 'load-generator', *args), state, log=ROOT / state['output'] / 'generator.log', check=False)
    state['generator_exit_code'] = result.returncode
    state['workload_finished_utc'] = now()
    state['drain_timeout_seconds'] = drain_timeout
    save(state)
    deadline = time.monotonic() + drain_timeout
    observed = None
    while time.monotonic() < deadline:
        observed = count_rows(state)
        if observed >= r['attempted_total']:
            break
        time.sleep(2)
    state['observed_persisted_unique_rows'] = observed
    state['all_attempts_observed'] = result.returncode == 0 and observed == r['attempted_total']
    state['drain_finished_utc'] = now()
    save(state)
    print(f"Attempted {r['attempted_total']}; observed persisted unique rows {observed}. Capture results with collect.")
    if not state['all_attempts_observed']:
        raise RuntimeError('Incomplete trial. Preserve logs/results; QoS 0 and generator exit code do not prove delivery.')

def collect(state):
    output = ROOT / state['output']
    processed = state['run']['processing'] != 'raw'
    procedure = 'calculate_delays_processed_pipeline' if processed else 'calculate_delays_raw_pipeline'
    extension = 'csv' if state['run']['database'] == 'postgres' else 'tsv'
    errors = []
    checks = [
        ('latency.' + extension, db_command(state, f'CALL {procedure}();', csv=True)),
        ('rows.' + extension, db_command(state, f'SELECT * FROM {table(state)};', csv=True)),
        ('containers.txt', compose(state, 'ps', '--all')),
        ('services.log', compose(state, 'logs', '--no-color')),
        ('images.txt', compose(state, 'images')),
        ('docker-version.txt', ['docker', 'version'])]
    for name, cmd in checks:
        result = process(cmd, state, log=output / name, check=False)
        if result.returncode:
            errors.append(name)
    # A release source ZIP can be used without checking out its accompanying bundle.
    revision = process(['git', 'rev-parse', 'HEAD'], state, capture=True, check=False)
    (output / 'source-commit.txt').write_text(revision.stdout if revision.returncode == 0 else 'Unversioned source copy; record the exact source version used for this trial.\n')
    result = process(compose(state, 'ps', '--all', '-q'), state, capture=True, check=False)
    if result.returncode == 0 and result.stdout.strip():
        process(['docker', 'inspect', *result.stdout.split()], state, log=output / 'containers.inspect.local.json', check=False)
        (output / 'containers.inspect.local.json').chmod(0o600)
        # Safe image metadata records exact resolved image digests, without container environment.
        for cid in result.stdout.split():
            ref = process(['docker', 'inspect', '--format', '{{.Image}}', cid], state, capture=True, check=False).stdout.strip()
            if ref:
                process(['docker', 'image', 'inspect', ref], state, log=output / f'image-{ref.replace(":", "-")}.json', check=False)
    if state['run']['platform'] == 'kafka':
        try:
            names = request('http://127.0.0.1:8083/connectors')
            statuses = {name: request(f'http://127.0.0.1:8083/connectors/{name}/status') for name in names}
            (output / 'connector-status.json').write_text(json.dumps(statuses, indent=2))
        except Exception as exc:
            errors.append('connector status: ' + str(exc))
    else:
        names = [('functions', 'temp-schema-fn')]
        if processed:
            names.append(('functions', 'temp-processing-fn'))
        sink = 'jdbc_sink_pulsar_' + ('functions_' if processed else '') + 'to_' + state['run']['database'] + '_temp'
        names.append(('sinks', sink))
        for kind, name in names:
            result = process(compose(state, 'exec', '-T', 'pulsar', 'bin/pulsar-admin', kind, 'status', '--tenant', 'public', '--namespace', 'default', '--name', name), state, log=output / f'{name}-status.json', check=False)
            if result.returncode:
                errors.append(name + ': status capture failed')
    if state['monitoring'] and state['workload_started_utc']:
        selector = '{image!="",container_label_com_docker_compose_project="' + state['project'] + '",container_label_com_docker_compose_service=~"' + PIPELINE_SERVICES + '"}'
        queries = {'cpu_user_percent': f'sum(rate(container_cpu_user_seconds_total{selector}[1m])) * 100', 'cpu_total_percent': f'sum(rate(container_cpu_usage_seconds_total{selector}[1m])) * 100', 'memory_mib': f'sum(container_memory_usage_bytes{selector}) / 1024 / 1024'}
        for name, query in queries.items():
            try:
                params = urllib.parse.urlencode({'query': query, 'start': state['workload_started_utc'], 'end': now(), 'step': '15s'})
                result = request('http://127.0.0.1:9090/api/v1/query_range?' + params)
                (output / f'{name}.prometheus.json').write_text(json.dumps(result, indent=2))
                if result.get('status') != 'success' or not any(x.get('values') for x in result.get('data', {}).get('result', [])):
                    errors.append(name + ': no samples; check cAdvisor/Compose labels')
            except Exception as exc:
                errors.append(name + ': ' + str(exc))
    state['collected_utc'] = now()
    state['collection_errors'] = errors
    state['database_deduplicates_by_message_id'] = True
    save(state)
    print('Results:', output)
    if errors:
        raise RuntimeError('Some captures failed: ' + ', '.join(errors))

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', choices=RUNS)
    parser.add_argument('action', choices=['plan', 'up', 'configure', 'run', 'collect', 'down'])
    parser.add_argument('--smoke', action='store_true', help='up only: 2 producers, 10 messages each, 100 ms delay')
    parser.add_argument('--no-monitoring', action='store_true', help='up only: skip monitoring services, for a small functional smoke test')
    parser.add_argument('--drain-timeout', type=int, default=180, help='run only: seconds to await expected database row count')
    args = parser.parse_args()
    if args.drain_timeout < 1:
        parser.error('--drain-timeout must be positive')
    if (args.smoke or args.no_monitoring) and args.action not in ['up', 'plan']:
        parser.error('--smoke and --no-monitoring belong to up or plan')
    if args.action == 'plan':
        plan = dict(RUNS[args.run])
        if args.smoke:
            plan.update(producer_count=2, messages_per_producer=10, delay_between_messages_ms=100, nominal_messages_per_second=20, attempted_total=20, smoke_test=True)
        print(json.dumps(plan, indent=2))
        print('Execution actions: up -> configure -> run -> collect -> down. No Docker operation was performed.')
    elif args.action == 'up':
        up(RUNS[args.run], args.smoke, not args.no_monitoring)
    else:
        state = active(args.run)
        if args.action == 'configure':
            configure(state)
        elif args.action == 'run':
            run_workload(state, args.drain_timeout)
        elif args.action == 'collect':
            collect(state)
        elif args.action == 'down':
            process(compose(state, 'down'), state)
            state['stopped_utc'] = now()
            save(state)
            STATE.unlink()
            print('Stopped. Results and database volumes are retained. Each subsequent up uses a new project.')

if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, OSError, ValueError, urllib.error.URLError) as exc:
        print('ERROR:', exc, file=sys.stderr)
        sys.exit(1)
