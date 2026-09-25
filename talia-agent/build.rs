//! Builds the Talia agent BPF object or a local-development placeholder.

#![deny(warnings)]
#![allow(
    clippy::disallowed_methods,
    reason = "the build script reads Cargo and TALIA_AGENT_REQUIRE_BPF variables directly"
)]

use std::env;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=src/modules/cpu/cpu.bpf.c");
    println!("cargo:rerun-if-changed=src/modules/disk_io/disk_io.bpf.c");
    println!("cargo:rerun-if-changed=src/modules/network/network.bpf.c");
    println!("cargo:rerun-if-changed=src/bpf/vmlinux.h");
    println!("cargo:rerun-if-changed=src/bpf/bpf/bpf_helpers.h");
    println!("cargo:rerun-if-changed=src/bpf/bpf/bpf_core_read.h");
    println!("cargo:rerun-if-changed=src/bpf/bpf/bpf_tracing.h");
    println!("cargo:rerun-if-env-changed=CLANG");
    println!("cargo:rerun-if-env-changed=TALIA_AGENT_REQUIRE_BPF");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR should be set"));
    let objects = [
        BpfObject {
            name: "CPU",
            source_path: Path::new("src/modules/cpu/cpu.bpf.c"),
            object_path: out_dir.join("talia_cpu.bpf.o"),
        },
        BpfObject {
            name: "network",
            source_path: Path::new("src/modules/network/network.bpf.c"),
            object_path: out_dir.join("talia_network.bpf.o"),
        },
        BpfObject {
            name: "disk I/O",
            source_path: Path::new("src/modules/disk_io/disk_io.bpf.c"),
            object_path: out_dir.join("talia_disk_io.bpf.o"),
        },
    ];
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux") {
        for object in &objects {
            write_placeholder_bpf_object(&object.object_path);
        }
        return;
    }

    for object in &objects {
        match compile_bpf_object(object) {
            Ok(()) => {},
            Err(error) if bpf_build_required() => panic!("{error}"),
            Err(error) => {
                println!(
                    "cargo:warning=Talia {} BPF object was not built: {error}; \
                     set TALIA_AGENT_REQUIRE_BPF=1 to make this a build error",
                    object.name
                );
                write_placeholder_bpf_object(&object.object_path);
            },
        }
    }
}

struct BpfObject<'a> {
    name: &'static str,
    source_path: &'a Path,
    object_path: PathBuf,
}

fn compile_bpf_object(object: &BpfObject<'_>) -> Result<(), String> {
    let clang = env::var("CLANG").unwrap_or_else(|_| "clang".to_string());
    let target_arch = target_arch_define()?;
    let status = Command::new(&clang)
        .args([
            "-O2",
            "-g",
            "-target",
            "bpf",
            "-Wall",
            "-Werror",
            "-Wno-missing-declarations",
            "-Isrc/bpf",
            "-c",
        ])
        .arg(format!("-D{target_arch}"))
        .arg(object.source_path)
        .arg("-o")
        .arg(&object.object_path)
        .status()
        .map_err(|error| {
            format!(
                "failed to run {clang} for Talia {} BPF object: {error}",
                object.name
            )
        })?;
    if !status.success() {
        return Err(format!(
            "{clang} failed to compile {} to {}",
            object.source_path.display(),
            object.object_path.display()
        ));
    }
    Ok(())
}

fn write_placeholder_bpf_object(object_path: &Path) {
    fs::write(object_path, []).expect("placeholder BPF object should be writable");
}

fn bpf_build_required() -> bool {
    env::var("TALIA_AGENT_REQUIRE_BPF")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
        .unwrap_or(false)
}

fn target_arch_define() -> Result<&'static str, String> {
    match env::var("CARGO_CFG_TARGET_ARCH").as_deref() {
        Ok("x86") | Ok("x86_64") => Ok("__TARGET_ARCH_x86"),
        Ok("aarch64") => Ok("__TARGET_ARCH_arm64"),
        Ok("arm") => Ok("__TARGET_ARCH_arm"),
        Ok("riscv64") => Ok("__TARGET_ARCH_riscv"),
        Ok("powerpc64") => Ok("__TARGET_ARCH_powerpc"),
        Ok("s390x") => Ok("__TARGET_ARCH_s390"),
        Ok("loongarch64") => Ok("__TARGET_ARCH_loongarch"),
        Ok(arch) => Err(format!(
            "unsupported Talia CPU BPF target architecture: {arch}"
        )),
        Err(error) => Err(format!("failed to read CARGO_CFG_TARGET_ARCH: {error}")),
    }
}
