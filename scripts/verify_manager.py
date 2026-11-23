"""Exercise the real manager API and native VM lifecycle; requires WHP/KVM."""
from pathlib import Path
import argparse
import hashlib
import http.client
import json
import os
import shutil
import socket
import subprocess
import time

TERMINAL = {'completed', 'failed', 'cancelled', 'interrupted'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--bundle', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True)
    binary = args.binary.resolve()
    root = out / 'runtime'
    shutil.copytree(args.bundle.resolve(), root)
    # The supplied bundle must be fresh: snapshots are intentionally not copied in.
    assert not (root / 'cache').exists()
    data = out / 'manager-state'
    endpoint_path = Path.home() / '.pybox/endpoint.json'
    records = []
    service = None
    endpoint = None
    output = (out / 'manager.log').open('ab')

    def check(name, passed, detail=None):
        record = {'name': name, 'passed': bool(passed), 'detail': detail}
        records.append(record)
        with (out / 'checks.jsonl').open('a', encoding='utf-8') as stream:
            stream.write(json.dumps(record, ensure_ascii=True) + '\n')
        if not passed:
            raise AssertionError(name + ': ' + str(detail))
        print(name + ': passed', flush=True)

    def start(use_default_port=False):
        nonlocal service, endpoint
        options = {'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}
        command = [str(binary), 'manage', '--root', str(root), '--data', str(data)]
        if not use_default_port:
            command += ['--port', '0']
        service = subprocess.Popen(command,
                                   stdout=output, stderr=output, **options)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if service.poll() is not None:
                raise RuntimeError('Manager startup failed; see retained manager.log')
            try:
                candidate = json.loads(endpoint_path.read_text(encoding='utf-8'))
                if candidate['pid'] == service.pid:
                    endpoint = candidate
                    return
            except (FileNotFoundError, ValueError):
                pass
            time.sleep(.03)
        raise TimeoutError('No endpoint for owned manager')

    def request(path, body=None, auth=True, headers=None, method=None):
        conn = http.client.HTTPConnection(endpoint['authority'], timeout=10)
        fields = {'Content-Type': 'application/json'}
        if auth:
            fields['Authorization'] = 'Bearer ' + endpoint['token']
        fields.update(headers or {})
        conn.request(method or ('GET' if body is None else 'POST'), path,
                     body=None if body is None else json.dumps(body), headers=fields)
        result = conn.getresponse()
        raw = result.read()
        conn.close()
        try:
            parsed = json.loads(raw)
        except ValueError:
            parsed = raw.decode('utf-8')
        return result.status, parsed

    def api(path, body=None):
        status, value = request(path, body)
        if status >= 400:
            raise RuntimeError(f'{path}: {status} {value}')
        return value

    def submit(**values):
        return api('/api/jobs', {'project': 'Manager verification', **values})

    def wait(job, predicate=lambda j: j['state'] in TERMINAL, seconds=70):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            value = api('/api/jobs/' + job['id'])
            if predicate(value):
                return value
            time.sleep(.03)
        raise TimeoutError('Job wait timed out: ' + job['id'])

    def stop_service():
        api('/api/shutdown', {})
        service.wait(timeout=20)
        if service.returncode != 0:
            raise RuntimeError('Manager shutdown failed')

    try:
        occupied = socket.socket()
        try:
            occupied.bind(('127.0.0.1', 8790))
            occupied.listen()
        except OSError:
            occupied.close()  # Another application already occupies the default.
        try:
            start(use_default_port=True)
            check('occupied_default_port_uses_free_loopback', not endpoint['authority'].endswith(':8790'))
        finally:
            occupied.close()
        check('loopback_api_auth', request('/api/overview', auth=False)[0] == 401)
        check('cross_origin_refused', request('/api/overview', headers={'Origin': 'https://example.invalid'})[0] == 403)
        check('foreign_host_refused', request('/api/overview', headers={'Host': 'example.invalid'})[0] == 403)
        check('static_console_has_no_token', endpoint['token'] not in request('/')[1])
        initial = api('/api/overview')
        check('initial_empty_inventory', initial['counts']['total'] == 0 and initial['counts']['live_vms'] == 0)
        check('unsupported_policy_refused', request('/api/settings', {'max_concurrent': 3, 'timeout_ms': 15000, 'cold': False})[0] == 400)
        check('unsupported_guest_option_refused', request('/api/jobs', {'project': 'x', 'network': True})[0] == 400)
        another = subprocess.run([str(binary), 'manage', '--root', str(root), '--data', str(out / 'other-state'), '--port', '0'], capture_output=True, timeout=10)
        check('one_manager_per_user_even_different_state', another.returncode != 0)
        standalone = subprocess.run([str(binary), '--root', str(root)], capture_output=True, timeout=10)
        check('standalone_cli_cannot_bypass_manager', standalone.returncode != 0 and b'manager is active' in standalone.stdout)
        api('/api/settings', {'max_concurrent': 1, 'timeout_ms': 15000, 'cold': False})
        first = submit()
        live = wait(first, lambda j: j['pid'] is not None)
        pending = submit()
        cancelled = api('/api/jobs/' + pending['id'] + '/stop', {})
        check('queued_cancel_never_spawns', cancelled['state'] == 'cancelled' and cancelled['last_pid'] is None and cancelled['cleanup_verified'])
        done = wait(first)
        check('first_preparation_and_workload', done['state'] == 'completed' and done['cleanup_verified'] and done['result']['snapshot_prepare_ms'] > 0)
        check('real_spawn_reap_and_output', len(done['events']) == 4 and done['last_pid'] is not None and 'langgraph' in done['stdout'] and done['pid'] is None)
        check('effective_policy_and_receipt', done['config']['guest_memory_mib'] == 512 and done['config']['network'] == 'disabled' and done['result']['native_result']['resources_reclaimed'])
        api('/api/settings', {'max_concurrent': 2, 'timeout_ms': 15000, 'cold': True})
        one, two, three = submit(), submit(), submit()
        maximum = 0
        deadline = time.monotonic() + 20
        observed_queue = False
        while time.monotonic() < deadline:
            state = api('/api/overview')
            maximum = max(maximum, state['counts']['live_vms'])
            observed_queue |= state['counts']['queued'] > 0
            if maximum == 2:
                break
            time.sleep(.02)
        check('two_real_vms_and_queued_work', maximum == 2 and observed_queue)
        api('/api/stop-all', {})
        stopped = [wait(job) for job in (one, two, three)]
        check('stop_all_reclaims_owned_work', all(j['state'] == 'cancelled' and j['cleanup_verified'] for j in stopped))
        check('cancelled_pid_count_returns_to_zero', api('/api/overview')['counts']['live_vms'] == 0)
        timed = wait(submit(timeout_ms=1))
        check('timeout_receipt', timed['state'] == 'failed' and timed['result']['native_result']['status'] == 'timeout' and timed['cleanup_verified'])
        warm = wait(submit(cold=False))
        check('healthy_after_cancellation_and_timeout', warm['state'] == 'completed' and warm['result']['snapshot_prepare_ms'] == 0)
        api('/api/settings', {'max_concurrent': 1, 'timeout_ms': 5000, 'cold': False})
        unchanged = api('/api/jobs/' + first['id'])
        check('historical_config_is_immutable', unchanged['config']['timeout_ms'] == 15000 and unchanged['config']['cold'] is False)
        cli = subprocess.run([str(binary), 'submit', '--project', 'CLI integration', '--cold'], capture_output=True, text=True, timeout=10)
        check('cli_uses_same_api', cli.returncode == 0 and json.loads(cli.stdout)['project'] == 'CLI integration')
        wait(json.loads(cli.stdout))
        count = api('/api/overview')['counts']['total']
        stop_service()
        check('shutdown_removes_own_endpoint', not endpoint_path.exists())
        start()
        restored = api('/api/overview')
        check('restart_preserves_history_and_defaults', restored['counts']['total'] == count and restored['settings']['timeout_ms'] == 5000 and restored['unhealthy'] is None)
        verify = wait(submit(action='verify'))
        check('image_snapshot_verification', verify['state'] == 'completed' and verify['last_pid'] is None)
        # A forced service crash must not claim the still-active record was reclaimed.
        crash_job = submit(cold=True, timeout_ms=15000)
        running = wait(crash_job, lambda j: j['pid'] is not None)
        service.kill()
        service.wait(timeout=10)
        time.sleep(.3)
        start()
        interrupted = api('/api/jobs/' + crash_job['id'])
        check('crash_restart_blocks_unverified_cleanup', interrupted['state'] == 'interrupted' and interrupted['cleanup_verified'] is False and request('/api/jobs', {'project': 'blocked'})[0] == 400)
        check('crash_restart_never_replays_work', api('/api/overview')['counts']['active'] == 0)
        stop_service()
        report = {'all_passed': True, 'checks': len(records), 'platform': 'windows-whp' if os.name == 'nt' else 'ubuntu-wsl2-kvm',
                  'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'results': [{k: r[k] for k in ('name', 'passed')} for r in records]}
        (out / 'finished.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        print(json.dumps({'all_passed': True, 'checks': len(records)}))
    finally:
        if service is not None and service.poll() is None:
            try:
                stop_service()
            except Exception:
                service.kill()
                service.wait(timeout=10)
        output.close()


if __name__ == '__main__':
    main()
