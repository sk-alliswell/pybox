use std::io::{stderr, stdout, Write};
fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("stdout") => {
            for _ in 0..1024 {
                let _ = stdout().write_all(&[b'x'; 4096]);
            }
        }
        Some("stderr") => {
            for _ in 0..1024 {
                let _ = stderr().write_all(&[b'e'; 4096]);
            }
        }
        Some("loop") => loop {
            std::thread::sleep(std::time::Duration::from_millis(20));
        },
        Some("policy") => {
            #[cfg(unix)]
            unsafe {
                let mut cpu: libc::rlimit = std::mem::zeroed();
                libc::getrlimit(libc::RLIMIT_CPU, &mut cpu);
                println!("{{\"cpu_seconds\":{}}}", cpu.rlim_cur);
            }
            #[cfg(windows)]
            println!("policy fixture");
        }
        _ => println!("fixture-ok"),
    }
}
