import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
operator = load('operator_cli', ROOT / 'scripts/experiment.py')
assets = load('asset_importer', ROOT / 'scripts/import-runtime-assets.py')

class OperatorTests(unittest.TestCase):
    def state(self, rid):
        return {'run': dict(operator.RUNS[rid]), 'project': 'isolated-test', 'instance': 'test', 'output': 'results/test', 'monitoring': False, 'configured': False, 'workload_started_utc': None}

    def test_paper_matrix_platform_and_processing_assignments(self):
        expected = [
            ('kafka','postgres','raw',130,100), ('pulsar','postgres','raw',130,100),
            ('kafka','mysql','raw',130,100), ('pulsar','mysql','raw',130,100),
            ('kafka','postgres','raw',512,100), ('pulsar','postgres','raw',512,100),
            ('kafka','postgres','raw',1024,100), ('pulsar','postgres','raw',1024,100),
            ('kafka','postgres','raw',2048,100), ('pulsar','postgres','raw',2048,100),
            ('kafka','postgres','ksqldb',130,100), ('pulsar','postgres','functions',130,100),
            ('kafka','postgres','streams',130,100), ('pulsar','postgres','raw',130,1000),
            ('kafka','postgres','raw',130,1000), ('pulsar','postgres','raw',130,2000),
            ('kafka','postgres','raw',130,2000), ('pulsar','postgres','raw',130,4000),
            ('kafka','postgres','raw',130,4000)]
        self.assertEqual(len(operator.RUNS), 19)
        for i, want in enumerate(expected, 1):
            with self.subTest(run=i):
                r = operator.RUNS[f'R{i}']
                self.assertEqual(tuple(r[k] for k in ['platform','database','processing','payload_size_argument','nominal_messages_per_second']), want)
                self.assertEqual(r['attempted_total'], r['producer_count'] * r['messages_per_producer'])
                self.assertEqual(r['nominal_messages_per_second'], r['producer_count'] * 1000 / r['delay_between_messages_ms'])

    def test_plan_is_read_only_and_needs_no_docker(self):
        with patch.object(operator, 'up', side_effect=AssertionError('must not start')), patch('sys.argv', ['experiment.py','R18','plan']), patch('builtins.print'):
            operator.main()

    def test_kafka_processed_sink_selects_only_one_output(self):
        for rid, expected_topic in [('R11','processed_ksqldb_temp'),('R13','processed_KafkaStreams_temp')]:
            state = self.state(rid)
            statuses = subprocess.CompletedProcess([], 0, stdout='ksql Stable\nbenchmark-temperature Stable\n', stderr='')
            with patch.object(operator,'count_rows',return_value=0), patch.object(operator,'request',return_value=[]), patch.object(operator,'wait_until',side_effect=lambda desc,fn: fn()), patch.object(operator,'read_env',return_value={'DB_PASSWORD':'local-test'}), patch.object(operator,'kafka_connector') as register, patch.object(operator,'process',return_value=statuses), patch.object(operator,'save'):
                operator.configure(state)
            sink = register.call_args_list[-1].args[2]
            self.assertEqual(sink['topics'], expected_topic)
            self.assertNotIn('topics.regex', sink)
            self.assertEqual(sink['connection.password'], 'local-test')
            self.assertTrue(state['configured'])

    def test_pulsar_enrichment_and_optional_temperature_function(self):
        status = subprocess.CompletedProcess([],0,stdout=json.dumps({'numRunning':1,'instances':[{'status':{'running':True}}]}),stderr='')
        for rid, functions in [('R2',1),('R12',2)]:
            with self.subTest(run=rid):
                state = self.state(rid)
                with patch.object(operator,'count_rows',return_value=0), patch.object(operator,'wait_until',side_effect=lambda desc,fn: fn()), patch.object(operator,'process',return_value=status) as run, patch.object(operator,'save'):
                    operator.configure(state)
                calls = [c.args[0] for c in run.call_args_list]
                creates = [cmd for cmd in calls if 'functions' in cmd and 'create' in cmd]
                self.assertEqual(len(creates), functions)
                self.assertIn('--custom-serde-inputs', creates[0])
                self.assertTrue(any('sinks' in cmd and 'create' in cmd for cmd in calls))

    def test_zero_exit_does_not_mask_missing_database_messages(self):
        state = self.state('R1')
        state['configured'] = True
        ok = subprocess.CompletedProcess([],0,stdout='',stderr='')
        with patch.object(operator,'save'), patch.object(operator,'process',return_value=ok), patch.object(operator,'count_rows',return_value=7), patch.object(operator.time,'sleep'), patch.object(operator.time,'monotonic',side_effect=[0,0,2]):
            with self.assertRaisesRegex(RuntimeError,'Incomplete trial'):
                operator.run_workload(state,1)
        self.assertFalse(state['all_attempts_observed'])
        self.assertEqual(state['observed_persisted_unique_rows'],7)
        self.assertEqual(state['generator_exit_code'],0)

    def test_trial_cannot_republish_to_existing_state(self):
        state = self.state('R1')
        state['configured'] = True
        state['workload_started_utc'] = 'already-started'
        with patch.object(operator,'process') as run:
            with self.assertRaises(RuntimeError):
                operator.run_workload(state,1)
        run.assert_not_called()

class AssetTests(unittest.TestCase):
    def fixture(self, root, payload=b'expected', recorded=None):
        path = 'runtime/test/dependency.jar'
        manifest = {'files':[{'path':path,'size_bytes':len(payload),'sha256':recorded or hashlib.sha256(payload).hexdigest()}]}
        (root/'assets').mkdir()
        (root/'assets/runtime-manifest.json').write_text(json.dumps(manifest))
        archive = root/'assets.zip'
        with zipfile.ZipFile(archive,'w') as z:
            z.writestr(path,payload)
        return archive, root/path

    def test_checksum_failure_prevents_installation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive,dest = self.fixture(root,recorded='0'*64)
            with patch.object(assets,'ROOT',root), self.assertRaisesRegex(ValueError,'Checksum mismatch'):
                assets.install(archive)
            self.assertFalse(dest.exists())

    def test_matching_asset_is_installed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive,dest = self.fixture(root)
            with patch.object(assets,'ROOT',root), patch('builtins.print'):
                assets.install(archive)
            self.assertEqual(dest.read_bytes(),b'expected')

if __name__ == '__main__':
    unittest.main()
