#ifndef __VMLINUX_H__
#define __VMLINUX_H__

/*
 * Minimal CO-RE type surface for the Talia CPU collector.
 *
 * Full bpftool-generated vmlinux.h files are kernel-wide type dumps and are
 * several megabytes. This program only needs scalar BPF helper types, map enum
 * constants, and the task_struct fields read with BPF_CORE_READ().
 */

typedef signed char __s8;
typedef unsigned char __u8;
typedef signed short __s16;
typedef unsigned short __u16;
typedef signed int __s32;
typedef unsigned int __u32;
typedef signed long long __s64;
typedef unsigned long long __u64;

typedef __u16 __be16;
typedef __u32 __be32;
typedef __u32 __wsum;

typedef __u32 u32;
typedef __u64 u64;
typedef _Bool bool;

enum {
    BPF_ANY = 0,
    BPF_NOEXIST = 1,
    BPF_EXIST = 2,
};

enum bpf_map_type {
    BPF_MAP_TYPE_UNSPEC = 0,
    BPF_MAP_TYPE_HASH = 1,
    BPF_MAP_TYPE_ARRAY = 2,
    BPF_MAP_TYPE_PROG_ARRAY = 3,
    BPF_MAP_TYPE_PERF_EVENT_ARRAY = 4,
    BPF_MAP_TYPE_PERCPU_HASH = 5,
    BPF_MAP_TYPE_PERCPU_ARRAY = 6,
    BPF_MAP_TYPE_STACK_TRACE = 7,
    BPF_MAP_TYPE_CGROUP_ARRAY = 8,
    BPF_MAP_TYPE_LRU_HASH = 9,
    BPF_MAP_TYPE_LRU_PERCPU_HASH = 10,
};

struct task_struct {
    unsigned int flags;
    u64 utime;
    u64 stime;
} __attribute__((preserve_access_index));

#endif
