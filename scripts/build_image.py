"""Actual Docker image build/run/export and restricted Python CPIO conversion."""
from pathlib import Path, PurePosixPath
import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tarfile
import time

ROOT=Path(__file__).resolve().parent.parent
import cpio as lab
DOCKER='docker'
LABEL='pybox.image-build'


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x',encoding='utf-8') as stream:json.dump(value,stream,indent=2,ensure_ascii=False)


def command(folder,name,argv,timeout=60,check=True):
    began=time.perf_counter()
    try:
        process=subprocess.run(argv,cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
            encoding='utf-8',errors='replace',timeout=timeout)
        result={'argv':argv,'exit_code':process.returncode,'stdout':process.stdout,'stderr':process.stderr,
                'elapsed_ms':(time.perf_counter()-began)*1000}
    except subprocess.TimeoutExpired as exc:
        decode=lambda value:value.decode('utf-8',errors='replace') if isinstance(value,bytes) else value or ''
        result={'argv':argv,'exit_code':None,'timeout':True,'stdout':decode(exc.stdout),'stderr':decode(exc.stderr),
                'elapsed_ms':(time.perf_counter()-began)*1000}
    write(folder/(name+'.json'),result)
    if check and result['exit_code']!=0:raise RuntimeError('Command failed; retained '+str(folder/(name+'.json')))
    return result


def oracle(body):
    try:
        graph=body['langgraph'];agent=body['deepagents'];image=body['image_contract']
        return (body['python']=='3.12.15' and body['versions']['langgraph']=='1.2.19'
            and body['versions']['deepagents']=='0.7.32' and body['uv_in_guest'] is False
            and graph['first']=={'value':6,'visits':['plan','tool']}
            and graph['second']=={'value':10,'visits':['plan','tool','plan','tool']}
            and graph['history_count']>=5 and all(graph['checks'].values())
            and agent['tool_calls']==['write_file','read_file','add_numbers']
            and agent['files']==['/notes/demo.txt'] and all(agent['checks'].values())
            and 'sandbox note' in agent['tool_outputs'][1]['content'] and agent['tool_outputs'][2]['content']=='5'
            and image['cwd']=='/app' and image['marker']=='docker-uv-agents-v6')
    except (KeyError,TypeError):return False


def cleanup(folder,name,image_id,run_id):
    # Never remove by name alone: verify exact generated name, immutable image and ownership label.
    inspected=command(folder,'cleanup-inspect-'+name,[DOCKER,'inspect',name],check=False,timeout=15)
    if inspected['exit_code']!=0:
        return {'name':name,'confirmed_absent':'no such object' in inspected['stderr'].lower() or 'no such container' in inspected['stderr'].lower()}
    info=json.loads(inspected['stdout'])[0]
    if info['Image']!=image_id or info['Config']['Labels'].get(LABEL)!=run_id:
        raise RuntimeError('Container ownership mismatch; cleanup refused')
    removed=command(folder,'cleanup-rm-'+name,[DOCKER,'rm','-f',info['Id']],timeout=30)
    after=command(folder,'cleanup-after-'+name,[DOCKER,'inspect',info['Id']],timeout=15,check=False)
    return {'name':name,'id':info['Id'],'removed':removed['exit_code']==0,
            'confirmed_absent':after['exit_code']!=0 and ('no such object' in after['stderr'].lower() or 'no such container' in after['stderr'].lower())}


def docker_build(run_id,context=None):
    context=ROOT/'guest' if context is None else context.resolve()
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,47}',run_id):
        raise ValueError('Invalid run-id')
    folder=ROOT/'runs'/run_id
    folder.mkdir(parents=True)
    write(folder/'source-manifest.json',{p.name:lab.file_sha(p) for p in context.iterdir() if p.is_file()})
    engine=command(folder,'engine-before',[DOCKER,'version','--format','{{json .Server}}'],timeout=15)
    image='pybox-agents:'+run_id
    print('Building actual Python Docker image',image,flush=True)
    command(folder,'docker-build',[DOCKER,'build','--platform','linux/amd64','--progress','plain',
        '--label',LABEL+'='+run_id,'-t',image,str(context)],timeout=600)
    inspected=command(folder,'image-inspect',[DOCKER,'image','inspect',image])
    info=json.loads(inspected['stdout'])[0]
    if info['Os']!='linux' or info['Architecture']!='amd64':raise RuntimeError('Unexpected target image')
    image_id=info['Id']
    names=[];teardown=[]
    try:
        smoke='pybox-'+run_id+'-smoke';names.append(smoke)
        created=command(folder,'docker-create-smoke',[DOCKER,'create','--name',smoke,'--label',LABEL+'='+run_id,
            '--network','none','--read-only','--memory','512m','--pids-limit','64','--tmpfs','/tmp:rw,size=64m',image_id])
        cid=created['stdout'].strip()
        if not re.fullmatch('[a-f0-9]{64}',cid):raise RuntimeError('Unexpected create reply')
        ran=command(folder,'docker-start-smoke',[DOCKER,'start','-a',cid],timeout=90)
        body=json.loads(ran['stdout'])
        state=json.loads(command(folder,'docker-smoke-inspect',[DOCKER,'inspect',cid])['stdout'])[0]
        write(folder/'docker-smoke.json',{'passed':oracle(body) and state['State']['ExitCode']==0,'output':body,
            'image_id':image_id,'container_id':cid,'state':state['State']})
        if not oracle(body) or state['State']['ExitCode']!=0:raise RuntimeError('Docker harness oracle failed')
        clean='pybox-'+run_id+'-export';names.append(clean)
        made=command(folder,'docker-create-export',[DOCKER,'create','--name',clean,'--label',LABEL+'='+run_id,
            '--network','none',image_id])
        export_cid=made['stdout'].strip()
        command(folder,'docker-export',[DOCKER,'export','--output',str(folder/'rootfs.tar'),export_cid],timeout=180)
        write(folder/'export-manifest.json',{'image_id':image_id,'image_tag':image,'container_id':export_cid,
            'exported_tar_sha256':lab.file_sha(folder/'rootfs.tar'),'exported_tar_bytes':(folder/'rootfs.tar').stat().st_size,
            'config':{k:info['Config'].get(k) for k in ('Entrypoint','Cmd','WorkingDir','Env','User','Volumes')},
            'architecture':info['Architecture'],'os':info['Os'],
            'scope':'Merged filesystem exported by real Docker from an unstarted container of the exact verified image'})
        print('Docker app passed; exact image filesystem exported',flush=True)
    finally:
        for name in names:teardown.append(cleanup(folder,name,image_id,run_id))
        write(folder/'container-cleanup.json',{'records':teardown,'all_absent':all(r['confirmed_absent'] for r in teardown)})


def convert(run_id):
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,47}',run_id):
        raise ValueError('Invalid run-id')
    folder=ROOT/'runs'/run_id
    exported=json.loads((folder/'export-manifest.json').read_text())
    if lab.file_sha(folder/'rootfs.tar')!=exported['exported_tar_sha256']:raise RuntimeError('Export changed')
    config=exported['config']
    if config['Entrypoint']!=['/usr/local/bin/python3','/app/entrypoint.py'] or config['Cmd']:
        raise ValueError('Only the declared Python script ENTRYPOINT is qualified')
    if config['User'] not in ('',None,'0','root') or config['Volumes']:
        raise ValueError('Unsupported USER/volume contract')
    if config['WorkingDir']!='/app':raise ValueError('Unsupported working directory')
    entries={};hardlinks={};normalized=[]
    with tarfile.open(folder/'rootfs.tar',mode='r:') as archive:
        for member in archive:
            name=lab.safe_name(member.name)
            if name=='.':continue
            if name in entries or name in hardlinks:raise ValueError('Duplicate exported path')
            if member.isfile():
                entries[name]=(archive.extractfile(member).read(),stat.S_IFREG|(member.mode&0o7777))
            elif member.isdir():entries[name]=(b'',stat.S_IFDIR|(member.mode&0o7777))
            elif member.issym():entries[name]=(member.linkname.encode(),stat.S_IFLNK|(member.mode&0o7777))
            elif member.islnk():hardlinks[name]=(lab.safe_name(member.linkname.lstrip('/')),member.mode)
            else:raise ValueError('Unsupported special exported file: '+name)
    def resolve(name,seen=None):
        if name in entries:return entries[name]
        seen=set() if seen is None else seen
        if name in seen or name not in hardlinks:raise ValueError('Invalid hardlink graph')
        seen.add(name)
        target,mode=hardlinks[name]
        data,target_mode=resolve(target,seen)
        if not stat.S_ISREG(target_mode):raise ValueError('Hardlink target must be regular')
        entries[name]=(data,stat.S_IFREG|(mode&0o7777))
        return entries[name]
    for name in hardlinks:resolve(name)
    # Only the runtime driver is injected. Retain the Docker image's own CPython and libpython.
    base=ROOT/'runtime/base.cpio'
    if lab.file_sha(base)!=lab.BASE_SHA:raise RuntimeError('Driver base changed')
    original,_=lab.read_cpio(base.read_bytes())
    driver='usr/local/bin/hl_pydriver'
    if driver in entries:raise ValueError('Image already contains a driver; no overwrite')
    entries[driver]=(original[driver]['data'],original[driver]['mode'])
    for name in ('usr/local/bin/python3','usr/local/lib/libpython3.12.so.1.0','app/harness.py','app/entrypoint.py'):
        if name not in entries:raise ValueError('Missing Python image component: '+name)
    if any(PurePosixPath(name).name in ('uv','uvx','uv.exe','uvx.exe') for name in entries):
        raise ValueError('Build-time uv leaked into the runtime')
    # Docker export empties its special /etc bind-mounted files; supply deterministic offline values.
    for name,data in {'etc/hosts':b'127.0.0.1 localhost\n::1 localhost\n',
        'etc/resolv.conf':b'', 'etc/nsswitch.conf':b'hosts: files dns\n'}.items():
        if name not in entries or not entries[name][0]:
            entries[name]=(data,stat.S_IFREG|0o644);normalized.append(name)
    output=folder/'converted'
    output.mkdir()
    with (output/'rootfs.cpio').open('xb') as stream:
        for inode,(name,(data,mode)) in enumerate(sorted(entries.items(),key=lambda x:(x[0].count('/'),x[0])),1):
            stream.write(lab.record(name,data,mode,inode))
        stream.write(lab.record('TRAILER!!!',b'',0,0))
        stream.write(b'\0'*(-stream.tell()%512))
    changes={driver}|set(normalized)
    preserved={name:lab.sha(data) for name,(data,mode) in entries.items() if stat.S_ISREG(mode) and name not in changes}
    write(output/'preserved-files.json',preserved)
    allowed={'PATH','LANG','PYTHON_VERSION','PYTHON_SHA256','SANDBOX_IMAGE_MARKER','LANGSMITH_TRACING','PYTHONDONTWRITEBYTECODE'}
    env=dict(s.split('=',1) for s in config['Env'] if s.split('=',1)[0] in allowed)
    write(output/'manifest.json',{'image_id':exported['image_id'],'tar_sha256':exported['exported_tar_sha256'],
        'rootfs_sha256':lab.file_sha(output/'rootfs.cpio'),'rootfs_bytes':(output/'rootfs.cpio').stat().st_size,
        'injected_driver_sha256':lab.sha(original[driver]['data']),'normalized_files':normalized,
        'preserved_regular_file_count':len(preserved),'environment':env,'working_dir':config['WorkingDir'],
        'entrypoint':config['Entrypoint'],'driver_base_sha256':lab.BASE_SHA,
        'scope':'Actual Docker export -> CPIO, injecting driver only; source interpreter and dependencies retained'})
    print('Converted actual Docker filesystem; preserved',len(preserved),'regular files',flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('action',choices=['docker','convert'])
    parser.add_argument('--run-id',default='r1')
    parser.add_argument('--context',type=Path)
    args=parser.parse_args()
    if args.action=='docker':docker_build(args.run_id,args.context)
    else:convert(args.run_id)
