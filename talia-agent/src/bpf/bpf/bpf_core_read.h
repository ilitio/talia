#ifndef __BPF_CORE_READ_H__
#define __BPF_CORE_READ_H__

#define BPF_CORE_READ(src, field) __builtin_preserve_access_index((src)->field)

#endif
