#include "vmlinux.h"
#include <bpf/bpf_helpers.h>

#define NETWORK_DIRECTION_INGRESS 1
#define NETWORK_DIRECTION_EGRESS 2

#define MAX_NETWORK_COUNTERS 2

struct network_key {
    /* One host-wide byte ledger per direction. */
    __u8 direction;
    __u8 _pad[7];
};

struct network_counters {
    /* Monotonic counters read by user space once per collection interval. */
    __u64 bytes;
    __u64 packets;
};

struct {
    /* Per-CPU network buckets avoid cross-CPU writes on packet hot paths. */
    __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
    __uint(max_entries, MAX_NETWORK_COUNTERS);
    __type(key, struct network_key);
    __type(value, struct network_counters);
} network_counters_per_cpu_map SEC(".maps");

struct netif_receive_skb_ctx {
    __u64 _common;
    void *skbaddr;
    __u32 len;
    __u32 _name_loc;
    __u64 net_cookie;
};

struct net_dev_xmit_ctx {
    __u64 _common;
    void *skbaddr;
    __u32 len;
    __s32 rc;
    __u32 _name_loc;
    __u64 net_cookie;
};

static __always_inline void account_network_bytes(__u32 len, __u8 direction)
{
    if (len == 0) {
        return;
    }

    struct network_key key = {};
    key.direction = direction;

    struct network_counters *counters =
        bpf_map_lookup_elem(&network_counters_per_cpu_map, &key);
    if (!counters) {
        struct network_counters initial = {};
        bpf_map_update_elem(&network_counters_per_cpu_map, &key, &initial, BPF_NOEXIST);
        counters = bpf_map_lookup_elem(&network_counters_per_cpu_map, &key);
        if (!counters) {
            return;
        }
    }

    counters->bytes += len;
    counters->packets += 1;
}

SEC("tracepoint/net/netif_receive_skb")
int talia_netif_receive_skb(struct netif_receive_skb_ctx *ctx)
{
    account_network_bytes(ctx->len, NETWORK_DIRECTION_INGRESS);
    return 0;
}

SEC("tracepoint/net/net_dev_xmit")
int talia_net_dev_xmit(struct net_dev_xmit_ctx *ctx)
{
    if (ctx->rc != 0) {
        return 0;
    }
    account_network_bytes(ctx->len, NETWORK_DIRECTION_EGRESS);
    return 0;
}

char LICENSE[] SEC("license") = "Dual BSD/GPL";
