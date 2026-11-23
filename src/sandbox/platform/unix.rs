use crate::guard::Profile;
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};

pub fn capability() -> Value {
    use std::fs::OpenOptions;
    match OpenOptions::new().read(true).write(true).open("/dev/kvm") {
        Ok(file) => {
            let version = unsafe { libc::ioctl(file.as_raw_fd(), 0xAE00, 0 as libc::c_ulong) };
            json!({"available":version==12,"mechanism":"KVM","api_version":version,
                "error":if version<0 {Some(std::io::Error::last_os_error().to_string())} else {None}})
        }
        Err(e) => json!({"available":false,"mechanism":"KVM","error":e.to_string()}),
    }
}

pub fn limiter(arguments: &[OsString]) -> Result<()> {
    if arguments.len() < 4 {
        bail!("invalid internal limiter invocation");
    }
    let parse = |i: usize| -> Result<u64> {
        Ok(arguments[i]
            .to_str()
            .context("non-UTF8 policy number")?
            .parse()?)
    };
    let parent = parse(0)? as libc::pid_t;
    let fd = parse(1)? as libc::c_int;
    let address = parse(2)?;
    if ![1024u64.pow(3), 3 * 1024u64.pow(3)].contains(&address) || fd < 3 {
        bail!("invalid native policy");
    }
    let cpu = if address == Profile::Preparation.linux_as_bytes() {
        Profile::Preparation.linux_cpu_seconds()
    } else {
        Profile::Execution.linux_cpu_seconds()
    };
    let mut receipt = unsafe { File::from_raw_fd(fd) };
    let applied = (|| -> Result<Value> {
        unsafe {
            if libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            if libc::getppid() != parent {
                libc::_exit(125);
            }
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            for (resource, limit) in [
                (libc::RLIMIT_CPU, cpu),
                (libc::RLIMIT_CORE, 0),
                (libc::RLIMIT_AS, address),
            ] {
                let policy = libc::rlimit {
                    rlim_cur: limit as _,
                    rlim_max: limit as _,
                };
                if libc::setrlimit(resource, &policy) < 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
            }
            let mut allowed: libc::cpu_set_t = std::mem::zeroed();
            if libc::sched_getaffinity(0, std::mem::size_of_val(&allowed), &mut allowed) < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let cpus: Vec<_> = (0..libc::CPU_SETSIZE as usize)
                .filter(|i| libc::CPU_ISSET(*i, &allowed))
                .collect();
            if cpus.is_empty() {
                bail!("no allowed CPU");
            }
            let first = cpus[std::process::id() as usize % cpus.len()];
            let mut selected: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_ZERO(&mut selected);
            libc::CPU_SET(first, &mut selected);
            if libc::sched_setaffinity(0, std::mem::size_of_val(&selected), &selected) < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(
                json!({"applied":true,"pid":std::process::id(),"expected_parent":parent,"parent_death_signal":"SIGKILL",
                "close_on_exec":true,"cpu_seconds":cpu,"address_space_bytes":address,"affinity":[first],"native_limiter":true}),
            )
        }
    })();
    match applied {
        Ok(policy) => {
            receipt.write_all(serde_json::to_string(&policy)?.as_bytes())?;
            receipt.flush()?;
        }
        Err(e) => {
            let _ = receipt.write_all(
                serde_json::to_string(&json!({"applied":false,"error":e.to_string()}))?.as_bytes(),
            );
            return Err(e);
        }
    }
    let error = Command::new(&arguments[3]).args(&arguments[4..]).exec();
    Err(error.into())
}

pub struct NativeProcess {
    pub pid: u32,
    pub stdout: Option<Box<dyn Read + Send>>,
    pub stderr: Option<Box<dyn Read + Send>>,
    pub receipt: Option<Box<dyn Read + Send>>,
    _child: Child,
    exit: Option<i64>,
    usage: Option<libc::rusage>,
}
impl NativeProcess {
    pub fn spawn(
        program: &Path,
        args: &[OsString],
        cwd: &Path,
        profile: Profile,
        env: &BTreeMap<OsString, OsString>,
    ) -> Result<Self> {
        let mut pipes = [0; 2];
        if unsafe { libc::pipe2(pipes.as_mut_ptr(), libc::O_CLOEXEC) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let read = unsafe { File::from_raw_fd(pipes[0]) };
        let write = unsafe { File::from_raw_fd(pipes[1]) };
        // Keep CLOEXEC in the multithreaded parent: clearing it here lets another
        // worker's concurrent spawn inherit this writer and hold the pipe open.
        // Only the forked child changes its own descriptor table before exec.
        let policy_fd = write.as_raw_fd();
        let mut command = Command::new(std::env::current_exe()?);
        unsafe {
            command.pre_exec(move || {
                if libc::fcntl(policy_fd, libc::F_SETFD, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command
            .arg("--__limit-exec")
            .arg(std::process::id().to_string())
            .arg(write.as_raw_fd().to_string())
            .arg(profile.linux_as_bytes().to_string())
            .arg(program)
            .args(args)
            .current_dir(cwd)
            .env_clear()
            .envs(env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();
        drop(write);
        let mut child = child?;
        Ok(Self {
            pid: child.id(),
            stdout: Some(Box::new(child.stdout.take().unwrap())),
            stderr: Some(Box::new(child.stderr.take().unwrap())),
            receipt: Some(Box::new(read)),
            _child: child,
            exit: None,
            usage: None,
        })
    }
    pub fn ready(&mut self) -> Result<()> {
        Ok(())
    }
    pub fn poll(&mut self) -> Result<Option<i64>> {
        if self.exit.is_some() {
            return Ok(self.exit);
        }
        let mut status = 0;
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        let result = unsafe { libc::wait4(self.pid as _, &mut status, libc::WNOHANG, &mut usage) };
        if result < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if result > 0 {
            self.exit = Some(if libc::WIFEXITED(status) {
                libc::WEXITSTATUS(status) as i64
            } else {
                -(libc::WTERMSIG(status) as i64)
            });
            self.usage = Some(usage);
        }
        Ok(self.exit)
    }
    pub fn kill(&mut self) -> Result<()> {
        let group = unsafe { libc::kill(-(self.pid as libc::pid_t), libc::SIGKILL) };
        let single = unsafe { libc::kill(self.pid as _, libc::SIGKILL) };
        if group < 0
            && single < 0
            && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
        {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }
    pub fn cleanup_group(&mut self) -> Result<()> {
        if unsafe { libc::kill(-(self.pid as libc::pid_t), libc::SIGKILL) } < 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.into());
            }
        }
        Ok(())
    }
    pub fn metrics(&self) -> Value {
        if let Some(usage) = self.usage {
            let seconds = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
            json!({"peak_rss_bytes":usage.ru_maxrss as u64*1024,"cpu_user_seconds":seconds(usage.ru_utime),
                "cpu_system_seconds":seconds(usage.ru_stime),"scope":"Linux wait4 native VMM process RSS/CPU; not total host VM RAM"})
        } else {
            Value::Null
        }
    }
    pub fn policy(&self) -> Option<Value> {
        None
    }
}
impl Drop for NativeProcess {
    fn drop(&mut self) {
        if self.exit.is_none() {
            let _ = self.kill();
            let _ = self.poll();
        }
    }
}
