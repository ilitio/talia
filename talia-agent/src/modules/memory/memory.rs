//! Lightweight Linux memory pressure collection.

use std::collections::BTreeMap;
use std::fs;

use rustix::param::page_size;
use thiserror::Error;

const MEMINFO_PATH: &str = "/proc/meminfo";
const VMSTAT_PATH: &str = "/proc/vmstat";

/// Host memory and swap pressure for one collection interval.
#[derive(Clone, Debug, PartialEq)]
pub struct MemorySample {
    /// Total physical memory in bytes.
    pub total_bytes: u64,
    /// Kernel-estimated bytes available without swapping.
    pub available_bytes: u64,
    /// Bytes currently used, computed as total minus available.
    pub used_bytes: u64,
    /// Completely free memory bytes.
    pub free_bytes: u64,
    /// Cache bytes that can usually be reclaimed.
    pub cached_bytes: u64,
    /// Total configured swap bytes.
    pub swap_total_bytes: u64,
    /// Free swap bytes.
    pub swap_free_bytes: u64,
    /// Used swap bytes.
    pub swap_used_bytes: u64,
    /// Fraction of total memory that is not available.
    pub used_ratio: f64,
    /// Fraction of total swap currently used.
    pub swap_used_ratio: f64,
    /// Bytes swapped in during this interval.
    pub swap_in_bytes: u64,
    /// Bytes swapped out during this interval.
    pub swap_out_bytes: u64,
}

/// Errors returned while collecting memory pressure samples.
#[derive(Debug, Error)]
pub enum MemoryError {
    /// A proc file could not be read.
    #[error("failed to read {path}: {source}")]
    ReadProc {
        /// Proc path that failed.
        path: &'static str,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// A required proc field was missing.
    #[error("missing required field {field} in {path}")]
    MissingField {
        /// Proc file path.
        path: &'static str,
        /// Missing field name.
        field: &'static str,
    },
    /// A proc field could not be parsed.
    #[error("invalid value for field {field} in {path}: {value}")]
    InvalidField {
        /// Proc file path.
        path: &'static str,
        /// Field name.
        field: String,
        /// Raw value.
        value: String,
    },
}

/// Stateful memory collector that turns vmstat counters into interval deltas.
pub struct MemoryCollector {
    page_size_bytes: u64,
    previous_swap: Option<SwapCounters>,
}

impl MemoryCollector {
    /// Creates a memory collector using the host page size.
    pub fn new() -> Self {
        Self {
            page_size_bytes: page_size() as u64,
            previous_swap: None,
        }
    }

    /// Collects one memory pressure sample.
    pub fn collect(&mut self) -> Result<MemorySample, MemoryError> {
        let meminfo = read_meminfo()?;
        let swap = read_vmstat_swap()?;
        let previous = self.previous_swap.replace(swap).unwrap_or(swap);
        Ok(sample_from_proc(
            meminfo,
            swap,
            previous,
            self.page_size_bytes,
        ))
    }
}

impl Default for MemoryCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MemInfo {
    total_bytes: u64,
    available_bytes: u64,
    free_bytes: u64,
    cached_bytes: u64,
    swap_total_bytes: u64,
    swap_free_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SwapCounters {
    in_pages: u64,
    out_pages: u64,
}

fn read_meminfo() -> Result<MemInfo, MemoryError> {
    let contents = fs::read_to_string(MEMINFO_PATH).map_err(|source| MemoryError::ReadProc {
        path: MEMINFO_PATH,
        source,
    })?;
    parse_meminfo(&contents)
}

fn read_vmstat_swap() -> Result<SwapCounters, MemoryError> {
    let contents = fs::read_to_string(VMSTAT_PATH).map_err(|source| MemoryError::ReadProc {
        path: VMSTAT_PATH,
        source,
    })?;
    parse_vmstat_swap(&contents)
}

fn parse_meminfo(contents: &str) -> Result<MemInfo, MemoryError> {
    let fields = parse_kib_fields(MEMINFO_PATH, contents)?;
    let total_bytes = required_field(&fields, MEMINFO_PATH, "MemTotal")?;
    let available_bytes = required_field(&fields, MEMINFO_PATH, "MemAvailable")?;
    let free_bytes = required_field(&fields, MEMINFO_PATH, "MemFree")?;
    let cached_bytes = required_field(&fields, MEMINFO_PATH, "Cached")?
        .saturating_add(fields.get("SReclaimable").copied().unwrap_or(0));
    let swap_total_bytes = required_field(&fields, MEMINFO_PATH, "SwapTotal")?;
    let swap_free_bytes = required_field(&fields, MEMINFO_PATH, "SwapFree")?;
    Ok(MemInfo {
        total_bytes,
        available_bytes,
        free_bytes,
        cached_bytes,
        swap_total_bytes,
        swap_free_bytes,
    })
}

fn parse_kib_fields(
    path: &'static str,
    contents: &str,
) -> Result<BTreeMap<String, u64>, MemoryError> {
    let mut fields = BTreeMap::new();
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let Some((name, rest)) = line.split_once(':') else {
            continue;
        };
        let Some(raw_value) = rest.split_whitespace().next() else {
            continue;
        };
        let value = raw_value
            .parse::<u64>()
            .map_err(|_| MemoryError::InvalidField {
                path,
                field: name.to_string(),
                value: raw_value.to_string(),
            })?
            .saturating_mul(1024);
        fields.insert(name.to_string(), value);
    }
    Ok(fields)
}

fn parse_vmstat_swap(contents: &str) -> Result<SwapCounters, MemoryError> {
    let mut in_pages = None;
    let mut out_pages = None;
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let mut fields = line.split_whitespace();
        let Some(name) = fields.next() else {
            continue;
        };
        let Some(raw_value) = fields.next() else {
            continue;
        };
        if name != "pswpin" && name != "pswpout" {
            continue;
        }
        let value = raw_value
            .parse::<u64>()
            .map_err(|_| MemoryError::InvalidField {
                path: VMSTAT_PATH,
                field: name.to_string(),
                value: raw_value.to_string(),
            })?;
        match name {
            "pswpin" => in_pages = Some(value),
            "pswpout" => out_pages = Some(value),
            _ => {},
        }
    }
    Ok(SwapCounters {
        in_pages: in_pages.ok_or(MemoryError::MissingField {
            path: VMSTAT_PATH,
            field: "pswpin",
        })?,
        out_pages: out_pages.ok_or(MemoryError::MissingField {
            path: VMSTAT_PATH,
            field: "pswpout",
        })?,
    })
}

fn required_field(
    fields: &BTreeMap<String, u64>,
    path: &'static str,
    field: &'static str,
) -> Result<u64, MemoryError> {
    fields
        .get(field)
        .copied()
        .ok_or(MemoryError::MissingField { path, field })
}

fn sample_from_proc(
    meminfo: MemInfo,
    swap: SwapCounters,
    previous_swap: SwapCounters,
    page_size_bytes: u64,
) -> MemorySample {
    let used_bytes = meminfo.total_bytes.saturating_sub(meminfo.available_bytes);
    let swap_used_bytes = meminfo
        .swap_total_bytes
        .saturating_sub(meminfo.swap_free_bytes);
    MemorySample {
        total_bytes: meminfo.total_bytes,
        available_bytes: meminfo.available_bytes,
        used_bytes,
        free_bytes: meminfo.free_bytes,
        cached_bytes: meminfo.cached_bytes,
        swap_total_bytes: meminfo.swap_total_bytes,
        swap_free_bytes: meminfo.swap_free_bytes,
        swap_used_bytes,
        used_ratio: ratio(used_bytes, meminfo.total_bytes),
        swap_used_ratio: ratio(swap_used_bytes, meminfo.swap_total_bytes),
        swap_in_bytes: swap
            .in_pages
            .saturating_sub(previous_swap.in_pages)
            .saturating_mul(page_size_bytes),
        swap_out_bytes: swap
            .out_pages
            .saturating_sub(previous_swap.out_pages)
            .saturating_mul(page_size_bytes),
    }
}

fn ratio(value: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        value as f64 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_meminfo_and_vmstat_into_memory_sample() {
        // Given: the scenario for parses meminfo and vmstat into memory sample is prepared.
        let meminfo = parse_meminfo(
            "\
MemTotal:       1000 kB
MemFree:         100 kB
MemAvailable:    400 kB
Cached:          200 kB
SReclaimable:     50 kB
SwapTotal:       500 kB
SwapFree:        300 kB
",
        )
        .expect("meminfo should parse");
        let previous = SwapCounters {
            in_pages: 10,
            out_pages: 20,
        };
        let current = parse_vmstat_swap("pswpin 13\npswpout 25\n").expect("vmstat should parse");

        // When: the behavior under test runs.
        let sample = sample_from_proc(meminfo, current, previous, 4096);

        // Then: the assertions confirm that parses meminfo and vmstat into memory sample.
        assert_eq!(sample.used_bytes, 600 * 1024);
        assert_eq!(sample.cached_bytes, 250 * 1024);
        assert_eq!(sample.swap_used_bytes, 200 * 1024);
        assert_eq!(sample.swap_in_bytes, 3 * 4096);
        assert_eq!(sample.swap_out_bytes, 5 * 4096);
        assert_eq!(sample.used_ratio, 0.6);
        assert_eq!(sample.swap_used_ratio, 0.4);
    }

    #[test]
    fn first_collect_reports_zero_swap_delta() {
        // Given: the scenario for first collect reports zero swap delta is prepared.
        let meminfo = MemInfo {
            total_bytes: 100,
            available_bytes: 80,
            free_bytes: 10,
            cached_bytes: 20,
            swap_total_bytes: 0,
            swap_free_bytes: 0,
        };
        let swap = SwapCounters {
            in_pages: 7,
            out_pages: 9,
        };

        // When: the behavior under test runs.
        let sample = sample_from_proc(meminfo, swap, swap, 4096);

        // Then: the assertions confirm that first collect reports zero swap delta.
        assert_eq!(sample.swap_in_bytes, 0);
        assert_eq!(sample.swap_out_bytes, 0);
    }
}
