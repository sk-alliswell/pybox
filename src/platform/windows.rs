use crate::guard::Profile;
use anyhow::{bail, Result};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::ffi::{c_void, OsStr, OsString};
use std::fs::File;
use std::io::Read;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::FromRawHandle;
use std::path::Path;
use std::ptr::{null, null_mut};
use windows_sys::Win32::Foundation::*;
use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
use windows_sys::Win32::Storage::FileSystem::*;
use windows_sys::Win32::System::JobObjects::*;
use windows_sys::Win32::System::LibraryLoader::*;
use windows_sys::Win32::System::Pipes::CreatePipe;
use windows_sys::Win32::System::ProcessStatus::*;
use windows_sys::Win32::System::Threading::*;

fn checked(value: i32) -> Result<()> {
    if value == 0 {
        Err(std::io::Error::last_os_error().into())
    } else {
        Ok(())
    }
}
fn wide(value: &OsStr) -> Result<Vec<u16>> {
    let mut result: Vec<_> = value.encode_wide().collect();
    if result.contains(&0) {
        bail!("NUL in native argument");
    }
    result.push(0);
    Ok(result)
}
pub fn quote_arg(value: &OsStr) -> Result<Vec<u16>> {
    let input: Vec<_> = value.encode_wide().collect();
    if input.contains(&0) {
        bail!("NUL in native argument");
    }
    let mut result = vec![34];
    let mut slashes = 0;
    for unit in input {
        if unit == 92 {
            slashes += 1;
            continue;
        }
        if unit == 34 {
            result.extend(std::iter::repeat_n(92, 2 * slashes + 1));
            result.push(34);
        } else {
            result.extend(std::iter::repeat_n(92, slashes));
            result.push(unit);
        }
        slashes = 0;
    }
    result.extend(std::iter::repeat_n(92, 2 * slashes));
    result.push(34);
    Ok(result)
}

struct Handle(HANDLE);
impl Handle {
    fn file(mut self) -> File {
        let raw = self.0;
        self.0 = null_mut();
        unsafe { File::from_raw_handle(raw) }
    }
}
impl Drop for Handle {
    fn drop(&mut self) {
        if !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

struct Attributes {
    ptr: LPPROC_THREAD_ATTRIBUTE_LIST,
    layout: std::alloc::Layout,
    initialized: bool,
}
impl Attributes {
    fn new() -> Result<Self> {
        let mut bytes = 0;
        unsafe {
            InitializeProcThreadAttributeList(null_mut(), 2, 0, &mut bytes);
        }
        if bytes == 0 {
            bail!("attribute list unavailable");
        }
        let layout = std::alloc::Layout::from_size_align(bytes, 16)?;
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            bail!("attribute allocation failed");
        }
        let mut result = Self {
            ptr: ptr.cast(),
            layout,
            initialized: false,
        };
        unsafe {
            checked(InitializeProcThreadAttributeList(
                result.ptr, 2, 0, &mut bytes,
            ))?;
        }
        result.initialized = true;
        Ok(result)
    }
}
impl Drop for Attributes {
    fn drop(&mut self) {
        unsafe {
            if self.initialized {
                DeleteProcThreadAttributeList(self.ptr);
            }
            std::alloc::dealloc(self.ptr.cast(), self.layout);
        }
    }
}

pub fn capability() -> Value {
    unsafe {
        let name = wide(OsStr::new("WinHvPlatform.dll")).unwrap();
        let library = LoadLibraryExW(name.as_ptr(), null_mut(), LOAD_LIBRARY_SEARCH_SYSTEM32);
        if library.is_null() {
            return json!({"available":false,"mechanism":"WHP","error":std::io::Error::last_os_error().to_string()});
        }
        let entry = GetProcAddress(library, c"WHvGetCapability".as_ptr().cast());
        let result = if let Some(entry) = entry {
            let function: unsafe extern "system" fn(u32, *mut c_void, u32, *mut u32) -> i32 =
                std::mem::transmute(entry);
            let mut value = 0i32;
            let mut written = 0u32;
            let hr = function(0, (&mut value as *mut i32).cast(), 4, &mut written);
            json!({"available":hr==0 && value!=0,"mechanism":"Windows Hypervisor Platform","hresult":hr})
        } else {
            json!({"available":false,"mechanism":"WHP","error":"capability API unavailable"})
        };
        FreeLibrary(library);
        result
    }
}

pub struct NativeProcess {
    pub pid: u32,
    pub stdout: Option<Box<dyn Read + Send>>,
    pub stderr: Option<Box<dyn Read + Send>>,
    pub receipt: Option<Box<dyn Read + Send>>,
    process: Handle,
    thread: Handle,
    job: Handle,
    exit: Option<i64>,
    policy: Option<Value>,
    profile: Profile,
}
impl NativeProcess {
    pub fn spawn(
        program: &Path,
        args: &[OsString],
        cwd: &Path,
        profile: Profile,
        env: &BTreeMap<OsString, OsString>,
    ) -> Result<Self> {
        unsafe {
            let job = Handle(CreateJobObjectW(null(), null()));
            if job.0.is_null() {
                return Err(std::io::Error::last_os_error().into());
            }
            let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
                | JOB_OBJECT_LIMIT_JOB_MEMORY
                | JOB_OBJECT_LIMIT_ACTIVE_PROCESS;
            limits.BasicLimitInformation.ActiveProcessLimit = 1;
            limits.JobMemoryLimit = profile.windows_commit_mib() * 1024 * 1024;
            checked(SetInformationJobObject(
                job.0,
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of_val(&limits) as u32,
            ))?;
            let security = SECURITY_ATTRIBUTES {
                nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
                lpSecurityDescriptor: null_mut(),
                bInheritHandle: 1,
            };
            fn pipe(security: &SECURITY_ATTRIBUTES) -> Result<(Handle, Handle)> {
                unsafe {
                    let mut read = null_mut();
                    let mut write = null_mut();
                    checked(CreatePipe(&mut read, &mut write, security, 0))?;
                    let pair = (Handle(read), Handle(write));
                    checked(SetHandleInformation(pair.0 .0, HANDLE_FLAG_INHERIT, 0))?;
                    Ok(pair)
                }
            }
            let (out_read, out_write) = pipe(&security)?;
            let (err_read, err_write) = pipe(&security)?;
            let nul_name = wide(OsStr::new("NUL"))?;
            let input = Handle(CreateFileW(
                nul_name.as_ptr(),
                0x80000000,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                &security,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL,
                null_mut(),
            ));
            if input.0 == INVALID_HANDLE_VALUE {
                return Err(std::io::Error::last_os_error().into());
            }
            let attributes = Attributes::new()?;
            let handles = [input.0, out_write.0, err_write.0];
            let jobs = [job.0];
            checked(UpdateProcThreadAttribute(
                attributes.ptr,
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                handles.as_ptr().cast(),
                std::mem::size_of_val(&handles),
                null_mut(),
                null(),
            ))?;
            checked(UpdateProcThreadAttribute(
                attributes.ptr,
                0,
                PROC_THREAD_ATTRIBUTE_JOB_LIST as usize,
                jobs.as_ptr().cast(),
                std::mem::size_of_val(&jobs),
                null_mut(),
                null(),
            ))?;
            let mut startup: STARTUPINFOEXW = std::mem::zeroed();
            startup.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
            startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES | STARTF_USESHOWWINDOW;
            startup.StartupInfo.wShowWindow = 0;
            startup.StartupInfo.hStdInput = input.0;
            startup.StartupInfo.hStdOutput = out_write.0;
            startup.StartupInfo.hStdError = err_write.0;
            startup.lpAttributeList = attributes.ptr;
            let application = wide(program.as_os_str())?;
            let directory = wide(cwd.as_os_str())?;
            let mut command = quote_arg(program.as_os_str())?;
            for arg in args {
                command.push(32);
                command.extend(quote_arg(arg)?);
            }
            command.push(0);
            let mut env: Vec<_> = env.iter().collect();
            env.sort_by_key(|(k, _)| k.to_string_lossy().to_uppercase());
            let mut block = Vec::<u16>::new();
            for (key, value) in env {
                let mut entry = OsString::from(key);
                entry.push("=");
                entry.push(value);
                block.extend(wide(&entry)?);
            }
            block.push(0);
            if block.len() == 1 {
                block.push(0);
            }
            let mut info: PROCESS_INFORMATION = std::mem::zeroed();
            checked(CreateProcessW(
                application.as_ptr(),
                command.as_mut_ptr(),
                null(),
                null(),
                1,
                EXTENDED_STARTUPINFO_PRESENT
                    | CREATE_SUSPENDED
                    | CREATE_NO_WINDOW
                    | CREATE_UNICODE_ENVIRONMENT,
                block.as_ptr().cast(),
                directory.as_ptr(),
                &startup.StartupInfo,
                &mut info,
            ))?;
            Ok(Self {
                pid: info.dwProcessId,
                stdout: Some(Box::new(out_read.file())),
                stderr: Some(Box::new(err_read.file())),
                receipt: None,
                process: Handle(info.hProcess),
                thread: Handle(info.hThread),
                job,
                exit: None,
                policy: None,
                profile,
            })
        }
    }
    pub fn ready(&mut self) -> Result<()> {
        unsafe {
            let mut own = 0usize;
            let mut system = 0usize;
            checked(GetProcessAffinityMask(
                GetCurrentProcess(),
                &mut own,
                &mut system,
            ))?;
            // Spread VM processes across the allowed set while retaining one CPU per VM.
            static NEXT_CPU: std::sync::atomic::AtomicUsize =
                std::sync::atomic::AtomicUsize::new(0);
            let cpus: Vec<_> = (0..usize::BITS)
                .filter(|i| own & (1usize << i) != 0)
                .collect();
            if cpus.is_empty() {
                bail!("no allowed CPU");
            }
            let offset = NEXT_CPU.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let selected = 1usize << cpus[offset % cpus.len()];
            if selected == 0 {
                bail!("no allowed CPU");
            }
            checked(SetProcessAffinityMask(self.process.0, selected))?;
            let mut associated = 0;
            checked(IsProcessInJob(self.process.0, self.job.0, &mut associated))?;
            let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            checked(QueryInformationJobObject(
                self.job.0,
                JobObjectExtendedLimitInformation,
                (&mut limits as *mut JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of_val(&limits) as u32,
                null_mut(),
            ))?;
            if associated == 0
                || limits.JobMemoryLimit != self.profile.windows_commit_mib() * 1024 * 1024
                || limits.BasicLimitInformation.ActiveProcessLimit != 1
            {
                bail!("native Job policy differs");
            }
            if ResumeThread(self.thread.0) == u32::MAX {
                return Err(std::io::Error::last_os_error().into());
            }
            self.policy = Some(
                json!({"applied":true,"job_associated_at_birth":true,"job_commit_limit_mib":self.profile.windows_commit_mib(),
                "active_process_limit":1,"affinity_cpu_count":selected.count_ones(),"kill_on_close":true}),
            );
            Ok(())
        }
    }
    pub fn poll(&mut self) -> Result<Option<i64>> {
        if self.exit.is_none() {
            unsafe {
                match WaitForSingleObject(self.process.0, 0) {
                    WAIT_OBJECT_0 => {
                        let mut value = 0;
                        checked(GetExitCodeProcess(self.process.0, &mut value))?;
                        self.exit = Some(value as i64);
                    }
                    WAIT_FAILED => return Err(std::io::Error::last_os_error().into()),
                    _ => {}
                }
            }
        }
        Ok(self.exit)
    }
    pub fn kill(&mut self) -> Result<()> {
        unsafe { checked(TerminateJobObject(self.job.0, 124)) }
    }
    pub fn cleanup_group(&mut self) -> Result<()> {
        Ok(())
    }
    pub fn policy(&self) -> Option<Value> {
        self.policy.clone()
    }
    pub fn metrics(&self) -> Value {
        unsafe {
            let mut memory: PROCESS_MEMORY_COUNTERS_EX = std::mem::zeroed();
            memory.cb = std::mem::size_of_val(&memory) as u32;
            let mut created: FILETIME = std::mem::zeroed();
            let mut exited: FILETIME = std::mem::zeroed();
            let mut kernel: FILETIME = std::mem::zeroed();
            let mut user: FILETIME = std::mem::zeroed();
            if GetProcessMemoryInfo(
                self.process.0,
                (&mut memory as *mut PROCESS_MEMORY_COUNTERS_EX).cast(),
                memory.cb,
            ) == 0
                || GetProcessTimes(
                    self.process.0,
                    &mut created,
                    &mut exited,
                    &mut kernel,
                    &mut user,
                ) == 0
            {
                return Value::Null;
            }
            let seconds = |t: FILETIME| {
                (((t.dwHighDateTime as u64) << 32) | t.dwLowDateTime as u64) as f64 / 1e7
            };
            json!({"peak_working_set_bytes":memory.PeakWorkingSetSize,"peak_private_commit_bytes":memory.PeakPagefileUsage,
                "cpu_user_seconds":seconds(user),"cpu_system_seconds":seconds(kernel),
                "scope":"Windows native VMM process counters; WHP guest RAM may be charged elsewhere, not total VM RAM"})
        }
    }
}
impl Drop for NativeProcess {
    fn drop(&mut self) {
        if self.exit.is_none() {
            let _ = self.kill();
            unsafe {
                WaitForSingleObject(self.process.0, 2000);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quote_spaces_quotes_and_trailing_slash() {
        let render = |s: &str| String::from_utf16(&quote_arg(OsStr::new(s)).unwrap()).unwrap();
        assert_eq!(render(""), "\"\"");
        assert_eq!(render("a b"), "\"a b\"");
        assert_eq!(render("a\"b"), "\"a\\\"b\"");
        assert_eq!(render("a\\"), "\"a\\\\\"");
        assert!(quote_arg(OsStr::new("bad\0arg")).is_err());
    }
}
