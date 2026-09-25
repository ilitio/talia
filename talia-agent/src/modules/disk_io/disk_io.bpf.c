#include "vmlinux.h"
#include <bpf/bpf_helpers.h>

#define DISK_IO_DIRECTION_READ 1
#define DISK_IO_DIRECTION_WRITE 2

#define MAX_DISK_IO_COUNTERS 2
#define MAX_IN_FLIGHT_REQUESTS 32768

struct disk_io_key {
    /* One host-wide ledger per I/O direction. */
    __u8 direction;
    __u8 _pad[7];
};

struct disk_io_counters {
    /* Request lifecycle counters read by user space once per collection interval. */
    __u64 bytes;
    __u64 operations;
    __u64 latency_ns;
    __u64 errors;
    __s64 in_flight_bytes;
    __s64 in_flight_operations;
};

struct disk_io_request_key {
    /* Stable enough identity to match issue and complete tracepoint records. */
    __u64 sector;
    __u32 dev;
    __u32 bytes;
    __u8 direction;
    __u8 _pad[7];
};

struct disk_io_request_start {
    /* Timestamp captured at issue so completion can account device latency. */
    __u64 issue_ns;
};

struct {
    /* Per-CPU disk buckets avoid cross-CPU writes on block I/O hot paths. */
    __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
    __uint(max_entries, MAX_DISK_IO_COUNTERS);
    __type(key, struct disk_io_key);
    __type(value, struct disk_io_counters);
} disk_io_counters_per_cpu_map SEC(".maps");

struct {
    /* Short-lived request starts; LRU bounds memory if completions are missed. */
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, MAX_IN_FLIGHT_REQUESTS);
    __type(key, struct disk_io_request_key);
    __type(value, struct disk_io_request_start);
} disk_io_request_start_map SEC(".maps");

struct block_rq_issue_ctx {
    __u64 _common;
    __u32 dev;
    __u32 _pad0;
    __u64 sector;
    __u32 nr_sector;
    __u32 bytes;
    __u16 ioprio;
    char rwbs[10];
};

struct block_rq_complete_ctx {
    __u64 _common;
    __u32 dev;
    __u32 _pad0;
    __u64 sector;
    __u32 nr_sector;
    __s32 error;
    __u16 ioprio;
    char rwbs[10];
};

static __always_inline __u8 disk_io_direction_from_rwbs(const char rwbs[10])
{
    if (rwbs[0] == 'R') {
        return DISK_IO_DIRECTION_READ;
    }
    if (rwbs[0] == 'W') {
        return DISK_IO_DIRECTION_WRITE;
    }
    return 0;
}

static __always_inline struct disk_io_counters *disk_io_counters_for(__u8 direction)
{
    struct disk_io_key key = {};
    key.direction = direction;

    struct disk_io_counters *counters =
        bpf_map_lookup_elem(&disk_io_counters_per_cpu_map, &key);
    if (!counters) {
        struct disk_io_counters initial = {};
        bpf_map_update_elem(&disk_io_counters_per_cpu_map, &key, &initial, BPF_NOEXIST);
        counters = bpf_map_lookup_elem(&disk_io_counters_per_cpu_map, &key);
        if (!counters) {
            return 0;
        }
    }

    return counters;
}

static __always_inline void disk_io_request_key_from_ctx(
    struct disk_io_request_key *key,
    __u32 dev,
    __u64 sector,
    __u64 bytes,
    __u8 direction
)
{
    key->sector = sector;
    key->dev = dev;
    key->bytes = (__u32)bytes;
    key->direction = direction;
}

static __always_inline void account_disk_io_issue(
    __u32 dev,
    __u64 sector,
    __u32 bytes,
    __u8 direction
)
{
    if (bytes == 0 || direction == 0) {
        return;
    }

    struct disk_io_request_key request_key = {};
    disk_io_request_key_from_ctx(&request_key, dev, sector, bytes, direction);

    struct disk_io_request_start start = {};
    start.issue_ns = bpf_ktime_get_ns();
    if (bpf_map_update_elem(&disk_io_request_start_map, &request_key, &start, BPF_ANY) != 0) {
        return;
    }

    struct disk_io_counters *counters = disk_io_counters_for(direction);
    if (!counters) {
        bpf_map_delete_elem(&disk_io_request_start_map, &request_key);
        return;
    }

    counters->in_flight_bytes += bytes;
    counters->in_flight_operations += 1;
}

static __always_inline void account_disk_io_complete(
    __u32 dev,
    __u64 sector,
    __u32 nr_sector,
    __s32 error,
    __u8 direction
)
{
    __u64 bytes = ((__u64)nr_sector) << 9;
    if (bytes == 0 || direction == 0 || bytes > 0xffffffffULL) {
        return;
    }

    struct disk_io_counters *counters = disk_io_counters_for(direction);
    if (!counters) {
        return;
    }

    struct disk_io_request_key request_key = {};
    disk_io_request_key_from_ctx(&request_key, dev, sector, bytes, direction);
    struct disk_io_request_start *start =
        bpf_map_lookup_elem(&disk_io_request_start_map, &request_key);
    if (start) {
        counters->latency_ns += bpf_ktime_get_ns() - start->issue_ns;
        counters->in_flight_bytes -= bytes;
        counters->in_flight_operations -= 1;
        bpf_map_delete_elem(&disk_io_request_start_map, &request_key);
    }

    if (error != 0) {
        counters->errors += 1;
        return;
    }

    counters->bytes += bytes;
    counters->operations += 1;
}

SEC("tracepoint/block/block_rq_issue")
int talia_block_rq_issue(struct block_rq_issue_ctx *ctx)
{
    account_disk_io_issue(
        ctx->dev,
        ctx->sector,
        ctx->bytes,
        disk_io_direction_from_rwbs(ctx->rwbs)
    );
    return 0;
}

SEC("tracepoint/block/block_rq_complete")
int talia_block_rq_complete(struct block_rq_complete_ctx *ctx)
{
    account_disk_io_complete(
        ctx->dev,
        ctx->sector,
        ctx->nr_sector,
        ctx->error,
        disk_io_direction_from_rwbs(ctx->rwbs)
    );
    return 0;
}

char LICENSE[] SEC("license") = "Dual BSD/GPL";
