//! Parser for the extended TCP counters in `/proc/net/netstat`.
//!
//! This module performs no I/O: [`parse_netstat`] operates on `&str`, so
//! unit tests stay hermetic and the future provider only has to read the
//! file and hand over its contents.

use super::proc_generic_parser::ParseError;
use super::proc_generic_parser::required_u64;
use super::proc_generic_parser::section_values;

/// Extended TCP counters from the `TcpExt:` section of `/proc/net/netstat`.
///
/// These split retransmission activity by recovery trigger. All values are
/// cumulative since boot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpNetstatCounters {
    /// Segments retransmitted after the retransmission timer expired
    /// (`TCPTimeouts`): the "total silence" recovery path.
    pub tcp_timeouts: u64,
    /// Segments retransmitted via fast retransmit on duplicate ACKs
    /// (`TCPFastRetrans`): the "gap detected while traffic flows" path.
    pub tcp_fast_retrans: u64,
}

/// Required `TcpExt:` fields from `/proc/net/netstat`.
const NETSTAT_FIELDS: [&str; 2] = ["TCPTimeouts", "TCPFastRetrans"];

/// Parses the `TcpExt:` section of `/proc/net/netstat` text.
///
/// Same layout contract as `parse_snmp`: header line first, values line
/// second, paired by name.
///
/// # Errors
///
/// Returns [`ParseError`] under the same conditions as `parse_snmp`.
pub fn parse_netstat(content: &str) -> Result<TcpNetstatCounters, ParseError> {
    let values = section_values(content, "TcpExt:")?;
    Ok(TcpNetstatCounters {
        tcp_timeouts: required_u64(&values, "TcpExt:", NETSTAT_FIELDS[0])?,
        tcp_fast_retrans: required_u64(&values, "TcpExt:", NETSTAT_FIELDS[1])?,
    })
}

#[cfg(test)]
mod tests {
    use super::TcpNetstatCounters;
    use super::parse_netstat;
    use crate::modules::tcp::proc_generic_parser::ParseError;

    /// Realistic `/proc/net/netstat` excerpt (trimmed TcpExt columns).
    const NETSTAT_FIXTURE: &str = "\
TcpExt: SyncookiesSent SyncookiesRecv TCPFastRetrans TCPSlowStartRetrans TCPTimeouts TCPLossProbes ListenOverflows ListenDrops\n\
TcpExt: 0 0 15 2 9 1 0 3\n\
IpExt: InNoRoutes InTruncatedPkts InMcastPkts\n\
IpExt: 0 0 5\n";

    #[test]
    fn parses_netstat_happy_path() {
        let parsed = parse_netstat(NETSTAT_FIXTURE).expect("fixture must parse");

        assert_eq!(
            parsed,
            TcpNetstatCounters {
                tcp_timeouts: 9,
                tcp_fast_retrans: 15,
            }
        );
    }

    #[test]
    fn netstat_missing_field_is_an_error() {
        let missing = "\
TcpExt: TCPTimeouts\n\
TcpExt: 9\n";

        let err = parse_netstat(missing).expect_err("must fail without TCPFastRetrans");
        assert_eq!(
            err,
            ParseError::MissingField {
                prefix: "TcpExt:",
                field: "TCPFastRetrans",
            }
        );
    }
}
