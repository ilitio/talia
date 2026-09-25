#ifndef __BPF_HELPERS__
#define __BPF_HELPERS__

static void *(*const bpf_map_lookup_elem)(void *map, const void *key) = (void *)1;
static long (*const bpf_map_update_elem)(
    void *map,
    const void *key,
    const void *value,
    __u64 flags
) = (void *)2;
static long (*const bpf_map_delete_elem)(void *map, const void *key) = (void *)3;
static __u64 (*const bpf_ktime_get_ns)(void) = (void *)5;

#define __uint(name, val) int (*name)[val]
#define __type(name, val) typeof(val) *name

#define SEC(name) __attribute__((section(name), used))

#undef __always_inline
#define __always_inline inline __attribute__((always_inline))

#ifndef NULL
#define NULL ((void *)0)
#endif

#endif
