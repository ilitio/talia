//! Parser for the basic TCP counters in `/proc/net/snmp`.
//!
//! This module performs no I/O: [`parse_snmp`] operates on `&str`, so unit
//! tests stay hermetic and the future provider only has to read the file
//! and hand over its contents.

use super::proc_generic_parser::ParseError;
use super::proc_generic_parser::required_u64;
use super::proc_generic_parser::section_values;

/// Basic TCP counters from the `Tcp:` section of `/proc/net/snmp`.
///
/// All values are cumulative since boot (except [`Self::curr_estab`], which
/// is a gauge), in the unit the MIB defines for each counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpSnmpCounters {
    /// Segments retransmitted because their ACK never arrived (`RetransSegs`).
    pub retrans_segs: u64,
    /// Segments sent, including retransmissions (`OutSegs`).
    pub out_segs: u64,
    /// Segments received (`InSegs`).
    pub in_segs: u64,
    /// Connections currently in ESTABLISHED or CLOSE-WAIT state
    /// (`CurrEstab`). A gauge, not a cumulative counter.
    pub curr_estab: u64,
}

/// Required `Tcp:` fields from `/proc/net/snmp`, in MIB order.
const SNMP_FIELDS: [&str; 4] = ["RetransSegs", "OutSegs", "InSegs", "CurrEstab"];

/// Parses the `Tcp:` section of `/proc/net/snmp` text.
///
/// Finds the first line starting with `Tcp:` (the field names) and the next
/// line starting with `Tcp:` (the values), pairs them by name, and extracts
/// the four counters. Other sections (`Ip:`, `Udp:`, …) are ignored.
///
/// # Errors
///
/// Returns [`ParseError`] when the header or values line is missing, when
/// their field counts differ, when a required field name is absent, or when
/// a value is not a valid unsigned integer.
pub fn parse_snmp(content: &str) -> Result<TcpSnmpCounters, ParseError> {
    let values = section_values(content, "Tcp:")?;
    Ok(TcpSnmpCounters {
        retrans_segs: required_u64(&values, "Tcp:", SNMP_FIELDS[0])?,
        out_segs: required_u64(&values, "Tcp:", SNMP_FIELDS[1])?,
        in_segs: required_u64(&values, "Tcp:", SNMP_FIELDS[2])?,
        curr_estab: required_u64(&values, "Tcp:", SNMP_FIELDS[3])?,
    })
}

#[cfg(test)]
mod tests {
    use super::TcpSnmpCounters;
    use super::parse_snmp;
    use crate::modules::tcp::proc_generic_parser::ParseError;

    /// Realistic `/proc/net/snmp` excerpt (other sections included to prove
    /// they are ignored).
    const SNMP_FIXTURE: &str = "\
Ip: Forwarding DefaultTTL InReceives InHdrErrors InAddrErrors ForwDatagrams InUnknownProtos InDiscards InDelivers OutRequests OutDiscards OutNoRoutes\n\
Ip: 2 64 100 0 0 0 0 0 90 0 0\n\
Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs InErrs OutRsts InCsumErrors\n\
Tcp: 1 200 120000 -1 95 12 3 14 7 7170 5542 42 0 16 0\n\
Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors\n\
Udp: 30 0 0 28 0 0\n";

    #[test]
    fn parses_snmp_happy_path() {
        let parsed = parse_snmp(SNMP_FIXTURE).expect("fixture must parse");

        assert_eq!(
            parsed,
            TcpSnmpCounters {
                retrans_segs: 42,
                out_segs: 5542,
                in_segs: 7170,
                curr_estab: 7,
            }
        );
    }

    #[test]
    fn scrambled_field_order_still_parses() {
        // Same counters, columns in a different order: name-based pairing
        // must not care.
        let scrambled = "\
Tcp: RetransSegs CurrEstab OutSegs InSegs\n\
Tcp: 42 7 5542 7170\n";

        let parsed = parse_snmp(scrambled).expect("scrambled order must parse");
        assert_eq!(parsed.retrans_segs, 42);
        assert_eq!(parsed.curr_estab, 7);
        assert_eq!(parsed.out_segs, 5542);
        assert_eq!(parsed.in_segs, 7170);
    }

    #[test]
    fn extra_whitespace_is_tolerated() {
        let spaced = "Tcp:   RetransSegs\tOutSegs  InSegs   CurrEstab\n\
                      Tcp:   42\t5542  7170   7\n";

        let parsed = parse_snmp(spaced).expect("whitespace must be tolerated");
        assert_eq!(parsed.retrans_segs, 42);
        assert_eq!(parsed.out_segs, 5542);
        assert_eq!(parsed.in_segs, 7170);
        assert_eq!(parsed.curr_estab, 7);
    }

    #[test]
    fn missing_required_field_is_an_error() {
        // No RetransSegs column at all.
        let missing = "\
Tcp: OutSegs InSegs CurrEstab\n\
Tcp: 5542 7170 7\n";

        let err = parse_snmp(missing).expect_err("must fail without RetransSegs");
        assert_eq!(
            err,
            ParseError::MissingField {
                prefix: "Tcp:",
                field: "RetransSegs",
            }
        );
    }

    #[test]
    fn non_numeric_value_is_an_error() {
        let bad = "\
Tcp: RtoAlgorithm RtoMin RetransSegs OutSegs InSegs CurrEstab\n\
Tcp: 1 200 oops 5542 7170 7\n";

        let err = parse_snmp(bad).expect_err("must fail on non-numeric value");
        assert_eq!(
            err,
            ParseError::InvalidInteger {
                prefix: "Tcp:",
                field: "RetransSegs",
                value: "oops".to_string(),
            }
        );
    }

    #[test]
    fn negative_value_is_an_error() {
        // Kernel uses -1 for MaxConn ("no limit"); our counters are u64, so
        // a negative value must fail loudly rather than wrap.
        let negative = "\
Tcp: RetransSegs OutSegs InSegs CurrEstab\n\
Tcp: -1 5542 7170 7\n";

        let err = parse_snmp(negative).expect_err("must fail on negative value");
        assert!(matches!(err, ParseError::InvalidInteger { .. }));
    }

    #[test]
    fn header_value_count_mismatch_is_an_error() {
        let mismatched = "\
Tcp: RetransSegs OutSegs InSegs CurrEstab\n\
Tcp: 42 5542 7170\n";

        let err = parse_snmp(mismatched).expect_err("must fail on count mismatch");
        assert_eq!(
            err,
            ParseError::FieldCountMismatch {
                prefix: "Tcp:",
                headers: 4,
                values: 3,
            }
        );
    }

    #[test]
    fn missing_header_line_is_an_error() {
        let err = parse_snmp("Ip: 1 2 3\n").expect_err("must fail without Tcp: line");
        assert_eq!(err, ParseError::MissingHeader { prefix: "Tcp:" });
    }

    #[test]
    fn missing_values_line_is_an_error() {
        let header_only = "Tcp: RetransSegs OutSegs InSegs CurrEstab\n";
        let err = parse_snmp(header_only).expect_err("must fail without values line");
        assert_eq!(err, ParseError::MissingValues { prefix: "Tcp:" });
    }

    #[test]
    fn snmp_prefix_does_not_match_tcpext_lines() {
        // "TcpExt:" must not be mistaken for a "Tcp:" section.
        let only_tcpext = "\
TcpExt: TCPTimeouts TCPFastRetrans\n\
TcpExt: 9 15\n";

        let err = parse_snmp(only_tcpext).expect_err("Tcp: must not match TcpExt:");
        assert_eq!(err, ParseError::MissingHeader { prefix: "Tcp:" });
    }
}
