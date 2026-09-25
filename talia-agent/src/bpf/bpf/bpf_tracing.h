#ifndef __BPF_TRACING_H__
#define __BPF_TRACING_H__

#define ___bpf_concat(left, right) left##right
#define ___bpf_apply(fn, n) ___bpf_concat(fn, n)
#define ___bpf_nth(_0, _1, _2, _3, _4, _5, N, ...) N
#define ___bpf_narg(...) ___bpf_nth(_, ##__VA_ARGS__, 5, 4, 3, 2, 1, 0)

#define ___bpf_ctx_cast0() ctx
#define ___bpf_ctx_cast1(arg) ___bpf_ctx_cast0(), ctx[0]
#define ___bpf_ctx_cast2(arg, args...) ___bpf_ctx_cast1(args), ctx[1]
#define ___bpf_ctx_cast3(arg, args...) ___bpf_ctx_cast2(args), ctx[2]
#define ___bpf_ctx_cast4(arg, args...) ___bpf_ctx_cast3(args), ctx[3]
#define ___bpf_ctx_cast5(arg, args...) ___bpf_ctx_cast4(args), ctx[4]
#define ___bpf_ctx_cast(args...) ___bpf_apply(___bpf_ctx_cast, ___bpf_narg(args))(args)

#define BPF_PROG(name, args...)                           \
    name(unsigned long long *ctx);                        \
    static __always_inline int ____##name(                \
        unsigned long long *ctx,                          \
        args                                              \
    );                                                    \
    int name(unsigned long long *ctx)                     \
    {                                                     \
        _Pragma("GCC diagnostic push")                    \
        _Pragma("GCC diagnostic ignored \"-Wint-conversion\"") \
        return ____##name(___bpf_ctx_cast(args));         \
        _Pragma("GCC diagnostic pop")                     \
    }                                                     \
    static __always_inline int ____##name(                \
        unsigned long long *ctx,                          \
        args                                              \
    )

#endif
