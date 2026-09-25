#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_tracing.h>

#define PF_IDLE 0x00000002

struct cpu_counters {
    /* One CPU's scheduler ledger. User space reads monotonic counters and computes deltas. */
    __u64 last_switch_ns;
    __u64 active_task_user_ns;
    __u64 active_task_system_ns;
    __u64 idle_ns;
    __u64 user_ns;
    __u64 system_ns;
};

struct {
    /* Per-CPU counters live in slot 0 of each CPU's private array copy. */
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct cpu_counters);
} cpu_counters_per_cpu_map SEC(".maps");

static __always_inline void remember_active_task(
    struct cpu_counters *counters,
    struct task_struct *task
)
{
    counters->active_task_user_ns = BPF_CORE_READ(task, utime);
    counters->active_task_system_ns = BPF_CORE_READ(task, stime);
}

SEC("tp_btf/sched_switch")
int BPF_PROG(talia_sched_switch, bool preempt, struct task_struct *prev,
             struct task_struct *next, unsigned int prev_state)
{
    (void)preempt;
    (void)prev_state;

    __u64 now = bpf_ktime_get_ns();
    __u32 key = 0;
    struct cpu_counters *counters = bpf_map_lookup_elem(&cpu_counters_per_cpu_map, &key);
    if (!counters) {
        return 0;
    }

    if (counters->last_switch_ns == 0) {
        counters->last_switch_ns = now;
        remember_active_task(counters, next);
        return 0;
    }

    __u64 delta_ns = now - counters->last_switch_ns;
    counters->last_switch_ns = now;

    __u32 flags = BPF_CORE_READ(prev, flags);
    if ((flags & PF_IDLE) != 0) {
        counters->idle_ns += delta_ns;
        remember_active_task(counters, next);
        return 0;
    }

    __u64 current_user_ns = BPF_CORE_READ(prev, utime);
    if (current_user_ns >= counters->active_task_user_ns) {
        counters->user_ns += current_user_ns - counters->active_task_user_ns;
    }

    __u64 current_system_ns = BPF_CORE_READ(prev, stime);
    if (current_system_ns >= counters->active_task_system_ns) {
        counters->system_ns += current_system_ns - counters->active_task_system_ns;
    }

    remember_active_task(counters, next);
    return 0;
}

char LICENSE[] SEC("license") = "Dual BSD/GPL";
