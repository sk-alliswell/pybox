param([switch]$WithProbes, [switch]$Linux)
$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    $buildArgs = @('--bin', 'pybox')
    if ($WithProbes) { $buildArgs = @('--bins', '--features', 'dev-tools') }
    & cargo build --release --locked @buildArgs
    if ($LASTEXITCODE) { throw 'Windows build failed' }
    if ($Linux) {
        & rustup target add x86_64-unknown-linux-musl
        if ($LASTEXITCODE) { throw 'musl target setup failed' }
        $toolchain = (& rustc --print sysroot).Trim()
        $oldLinker = $env:CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER
        $oldFlags = $env:CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS
        try {
            $env:CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER = Join-Path $toolchain 'lib/rustlib/x86_64-pc-windows-msvc/bin/rust-lld.exe'
            $env:CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS = '-C linker-flavor=ld.lld -C link-self-contained=yes'
            & cargo build --release --locked --target x86_64-unknown-linux-musl @buildArgs
            if ($LASTEXITCODE) { throw 'Linux build failed' }
        } finally {
            $env:CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER = $oldLinker
            $env:CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS = $oldFlags
        }
    }
} finally { Pop-Location }
