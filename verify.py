"""Development-only native-runtime verification; pybox never invokes Python."""
from pathlib import Path
import argparse
import copy
import ctypes
import hashlib
import json
import os
import queue
import shutil
import signal
import subprocess
import threading
import time

ROOT=Path(__file__).resolve().parent


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--payload',type=Path,required=True)
    parser.add_argument('--tools',type=Path,required=True)
    parser.add_argument('--out',type=Path,required=True)
    args=parser.parse_args()
    out=args.out.resolve();out.mkdir(parents=True)
    root=out/'샌드박스 실행'
    shutil.copytree(args.payload.resolve(),root)
    win=os.name=='nt';native='pybox.exe' if win else 'pybox'
    exe=root/native
    tools=args.tools.resolve()
    fixture=tools/('guard-fixture.exe' if win else 'guard-fixture')
    probe=tools/('guard-probe.exe' if win else 'guard-probe')
    if not win:
        for p in (exe,root/'hluk',fixture,probe):p.chmod(0o755)
    env=dict(os.environ,PATH='',PYTHONHOME='/host-python-is-not-available',PYTHONPATH='/host-python-is-not-available')
    expected=json.loads((root/'docker-baseline.json').read_text(encoding='utf-8'))['output']
    records=[]
    log=(out/'checks.jsonl').open('x',encoding='utf-8',buffering=1)

    def record(name,passed,detail):
        value={'name':name,'passed':bool(passed),'detail':detail};records.append(value)
        log.write(json.dumps(value,ensure_ascii=False)+'\n')
        if not passed:raise RuntimeError('Check failed: '+name)

    def call(argv,timeout=70,extra=None):
        start=time.perf_counter()
        p=subprocess.Popen([str(v) for v in argv],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                           **(extra or {}))
        stdout,stderr=p.communicate(timeout=timeout)
        text=stdout.decode('utf-8',errors='replace')
        try:body=json.loads(text)
        except ValueError:body=None
        return {'argv':[str(v) for v in argv],'exit_code':p.returncode,'controller_pid':p.pid,
                'body':body,'stdout':text,'stderr':stderr.decode('utf-8',errors='replace'),
                'cli_ms':(time.perf_counter()-start)*1000}

    def run(*flags):return call([exe,*flags])

    def good(value):
        body=value['body']
        return (value['exit_code']==0 and isinstance(body,dict) and body.get('passed') is True
            and body.get('host_python_required') is False and body['native_result']['resources_reclaimed']
            and body['native_result']['status']=='completed' and body['output']['versions']==expected['versions']
            and body['output']['image_contract']==expected['image_contract']
            and body['output']['langgraph']['first']==expected['langgraph']['first']
            and body['output']['langgraph']['second']==expected['langgraph']['second']
            and body['output']['langgraph']['history_count']==8
            and body['output']['deepagents']['tool_calls']==['write_file','read_file','add_numbers']
            and body['output']['deepagents']['tool_outputs']==expected['deepagents']['tool_outputs'])

    def live(pid):
        if win:
            k=ctypes.WinDLL('kernel32',use_last_error=True)
            k.OpenProcess.argtypes=[ctypes.c_ulong,ctypes.c_int,ctypes.c_ulong];k.OpenProcess.restype=ctypes.c_void_p
            k.GetExitCodeProcess.argtypes=[ctypes.c_void_p,ctypes.POINTER(ctypes.c_ulong)]
            k.CloseHandle.argtypes=[ctypes.c_void_p]
            handle=k.OpenProcess(0x1000,False,pid)
            if not handle:return False
            code=ctypes.c_ulong();ok=k.GetExitCodeProcess(handle,ctypes.byref(code));k.CloseHandle(handle)
            return bool(ok and code.value==259)
        try:
            fields=Path(f'/proc/{pid}/stat').read_text(encoding='utf-8').split(') ',1)[1].split()
            return fields[0]!='Z'
        except FileNotFoundError:return False

    def started(extra=None):
        p=subprocess.Popen([str(exe),'--cold','--events'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,**(extra or {}))
        lines=[];events=queue.Queue()
        def watch():
            for line in iter(p.stderr.readline,b''):
                lines.append(line.decode('utf-8',errors='replace'))
                try:
                    event=json.loads(line)
                    if event.get('event')=='native_spawned':events.put(event)
                except ValueError:pass
        reader=threading.Thread(target=watch,daemon=True);reader.start()
        output=[]
        drain=threading.Thread(target=lambda:output.append(p.stdout.read()),daemon=True);drain.start()
        event=events.get(timeout=15)
        return p,reader,lines,event['pid'],drain,output

    def finish(item):
        p,reader,lines,pid,drain,output=item
        p.wait(timeout=20);drain.join(timeout=2);reader.join(timeout=2)
        stdout=b''.join(output).decode('utf-8',errors='replace')
        try:body=json.loads(stdout)
        except ValueError:body=None
        p.stdout.close();p.stderr.close()
        return {'exit_code':p.returncode,'body':body,'stdout':stdout,'stderr':''.join(lines),'native_pid':pid}

    try:
        record('no_host_python_scripts',not any(root.rglob('*.py')),[str(p.name) for p in root.iterdir()])
        status=run('status');record('status_without_guest_launch',status['body']['passed'] and not status['body']['snapshot_ready'],status)
        first=run();record('first_ready_preparation_no_python_PATH',good(first) and first['body']['snapshot_prepare_ms']>0,first)
        pins=json.loads((root/'cache/snapshot-ready/controller-snapshot.json').read_text(encoding='utf-8'))
        receipt=pins['preparation_receipt']['policy_receipt']
        policy_ok=receipt['applied'] and (receipt['job_commit_limit_mib']==1536 if win else receipt['address_space_bytes']==3*1024**3 and receipt['cpu_seconds']==30)
        record('preparation_native_policy_receipt',policy_ok,receipt)
        for index in range(3):
            value=run();record('ready_clone_'+str(index),good(value) and value['body']['snapshot_prepare_ms']==0,value)
        cold=run('--cold');record('cold_same_docker_identity',good(cold),cold)
        status=run('status');record('ready_status_full_hashes',status['body']['passed'] and status['body']['snapshot_ready'],status)
        for mode,expected_status in [('stdout','output_limit'),('stderr','output_limit'),('loop','timeout'),('policy','completed')]:
            value=call([probe,fixture,mode,'150' if mode=='loop' else '1500'])
            body=value['body'];ok=body is not None and body['status']==expected_status and body['resources_reclaimed']
            if mode in ('stdout','stderr'):ok=ok and body['output_truncated'] and max(body['captured_bytes'])<=8192
            if mode=='policy':
                rec=body['policy_receipt'];ok=ok and rec['applied']
                ok=ok and (rec['job_associated_at_birth'] and rec['job_commit_limit_mib']==384 and rec['affinity_cpu_count']==1 if win
                           else rec['native_limiter'] and rec['cpu_seconds']==5 and rec['address_space_bytes']==1024**3 and rec['close_on_exec'] and rec['expected_parent']==value['controller_pid'] and rec['pid']==body['process_id'])
            record('native_guard_'+mode,ok,value)
        value=call([probe,fixture,'loop','1500','80'])
        record('native_guard_cancel',value['body']['status']=='cancelled' and value['body']['resources_reclaimed'],value)
        value=call([probe,tools/'does-not-exist','policy','1000'])
        record('missing_program_no_fallback',value['exit_code']!=0 or value['body']['status']=='runtime_error',value)
        timeout=run('--cold','--timeout-ms','200')
        record('real_VM_timeout',timeout['exit_code']!=0 and timeout['body']['native_result']['status']=='timeout' and timeout['body']['native_result']['resources_reclaimed'],timeout)
        cancel=run('--cold','--cancel-after-ms','700')
        body=cancel['body'];cancelled=body.get('status')=='cancelled' or body.get('native_result',{}).get('status')=='cancelled'
        record('real_VM_cancel',cancel['exit_code']!=0 and cancelled,cancel)
        index=root/'cache/snapshot-ready/index.json';held=index.with_name('index.json.test-held')
        assert not held.exists();index.rename(held)
        try:
            bad=run('status');record('snapshot_inventory_refused',bad['exit_code']!=0 and 'inventory' in bad['body']['error'],bad)
        finally:held.rename(index)
        marker=root/'state/unclean.json';assert not marker.exists();marker.write_text('{"owned_test_marker":true}')
        try:
            bad=run();record('uncertain_cleanup_blocks_launch',bad['exit_code']!=0 and 'unhealthy' in bad['body']['error'],bad)
        finally:
            if marker.read_text(encoding='utf-8')=='{"owned_test_marker":true}':marker.unlink()
        badroot=out/'corrupt-payload';shutil.copytree(args.payload.resolve(),badroot)
        if not win:(badroot/native).chmod(0o755)
        artifact=badroot/'rootfs.cpio'
        with artifact.open('r+b') as stream:
            stream.seek(4096);b=stream.read(1);stream.seek(4096);stream.write(bytes([b[0]^1]))
        bad=call([badroot/native,'--cold','--events'])
        record('same_size_payload_corruption_refused',bad['exit_code']!=0 and 'integrity mismatch' in bad['body']['error'] and 'native_spawned' not in bad['stderr'],bad)
        bundle_file=badroot/'BUNDLE_SHA256.json';original=bundle_file.read_text(encoding='utf-8');malformed=json.loads(original);malformed.pop('rootfs.cpio');bundle_file.write_text(json.dumps(malformed),encoding='utf-8')
        bad=call([badroot/native,'--cold'])
        record('incomplete_payload_manifest_refused',bad['exit_code']!=0 and 'incomplete bundle' in bad['body']['error'],bad)
        held_files=[]
        try:
            for i in range(2):
                f=(root/f'state/leases/slot-{i}.lock').open('a+b');held_files.append(f)
                if f.tell()==0:f.write(b'\0');f.flush()
                f.seek(0)
                if win:
                    import msvcrt
                    msvcrt.locking(f.fileno(),msvcrt.LK_NBLCK,1)
                else:
                    import fcntl
                    fcntl.flock(f.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
            busy=run('--cold');record('cross_language_file_lock_interoperability',busy['exit_code']!=0 and 'busy' in busy['body']['error'],busy)
        finally:
            for f in held_files:f.close()
        first_live=started();second_live=started()
        try:
            busy=run('--cold')
            record('two_actual_VMs_third_refused',live(first_live[3]) and live(second_live[3]) and busy['exit_code']!=0 and 'busy' in busy['body']['error'],busy)
            time.sleep(.05);first_live[0].kill();dead=finish(first_live)
            end=time.monotonic()+4
            while live(first_live[3]) and time.monotonic()<end:time.sleep(.02)
            record('controller_crash_kills_actual_VMM',not live(first_live[3]),dead)
            other=finish(second_live);record('other_concurrent_VM_healthy',good(other),other)
        finally:
            for item in (first_live,second_live):
                if item[0].poll() is None:item[0].kill();item[0].wait(timeout=5)
        healthy=run();record('crash_slot_reclaimed_and_post_failure_health',good(healthy),healthy)
        extra={}
        if win:
            si=subprocess.STARTUPINFO();si.dwFlags|=subprocess.STARTF_USESHOWWINDOW;si.wShowWindow=0
            extra={'creationflags':subprocess.CREATE_NEW_CONSOLE|subprocess.CREATE_NEW_PROCESS_GROUP,'startupinfo':si}
        signal_case=started(extra)
        try:
            if not win:os.kill(signal_case[0].pid,signal.SIGINT);sent=True
            else:
                k=ctypes.WinDLL('kernel32',use_last_error=True)
                old=signal.signal(signal.SIGBREAK,signal.SIG_IGN)
                k.FreeConsole();attached=bool(k.AttachConsole(signal_case[0].pid))
                sent=attached and bool(k.GenerateConsoleCtrlEvent(1,signal_case[0].pid))
                time.sleep(.1);k.FreeConsole();k.AttachConsole(os.getppid());signal.signal(signal.SIGBREAK,old)
            value=finish(signal_case)
            record('actual_console_cancel_signal',sent and value['body']['native_result']['status']=='cancelled' and value['body']['native_result']['resources_reclaimed'],value)
        finally:
            if signal_case[0].poll() is None:signal_case[0].kill();signal_case[0].wait(timeout=5)
        final=run();record('final_healthy_ready',good(final),final)
        metadata={'all_passed':all(r['passed'] for r in records),'checks':len(records),
            'controller_sha256':sha(exe),'cargo_lock_sha256':sha(ROOT/'Cargo.lock'),
            'runtime_root':str(root),'platform':'windows' if win else 'ubuntu-wsl2',
            'runtime_PATH':env['PATH'],'host_python_required':False,
            'source_hashes':{p.relative_to(ROOT).as_posix():sha(p) for p in (ROOT/'src').rglob('*.rs')}}
        (out/'finished.json').open('x',encoding='utf-8').write(json.dumps(metadata,indent=2))
        print(json.dumps({'all_passed':True,'checks':len(records),'out':str(out)},indent=2))
    finally:log.close()


if __name__=='__main__':main()
