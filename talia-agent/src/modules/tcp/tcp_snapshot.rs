//! Snapshot of TCP counters from a Linux network namespace.

use super::proc_net_netstat::TcpNetstatCounters;
use super::proc_net_snmp::TcpSnmpCounters;

/// One host-wide (or network-namespace-wide) TCP snapshot.
///
/// Combines the basic counters from `/proc/net/snmp` with the extended
/// counters from `/proc/net/netstat`. Build it with [`Self::combine`] once
/// both files have been parsed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpSnapshot {
    /// Segments retransmitted because their ACK never arrived.
    pub retrans_segs: u64,
    /// Segments sent, including retransmissions.
    pub out_segs: u64,
    /// Segments received.
    pub in_segs: u64,
    /// Connections currently in ESTABLISHED or CLOSE-WAIT state (gauge).
    pub curr_estab: u64,
    /// Segments retransmitted after the retransmission timer expired.
    pub tcp_timeouts: u64,
    /// Segments retransmitted via fast retransmit on duplicate ACKs.
    pub tcp_fast_retrans: u64,
}

impl TcpSnapshot {
    /// Combines the two parsed counter sets into one snapshot.
    pub fn combine(snmp: TcpSnmpCounters, netstat: TcpNetstatCounters) -> Self {
        Self {
            retrans_segs: snmp.retrans_segs,
            out_segs: snmp.out_segs,
            in_segs: snmp.in_segs,
            curr_estab: snmp.curr_estab,
            tcp_timeouts: netstat.tcp_timeouts,
            tcp_fast_retrans: netstat.tcp_fast_retrans,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TcpSnapshot;
    use crate::modules::tcp::proc_net_netstat::parse_netstat;
    use crate::modules::tcp::proc_net_snmp::parse_snmp;

    /// Realistic `/proc/net/snmp` excerpt (other sections included to prove
    /// they are ignored).
    const SNMP_FIXTURE: &str = "\
Ip: Forwarding DefaultTTL InReceives InHdrErrors InAddrErrors ForwDatagrams InUnknownProtos InDiscards InDelivers OutRequests OutDiscards OutNoRoutes\n\
Ip: 2 64 100 0 0 0 0 0 90 0 0\n\
Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs InErrs OutRsts InCsumErrors\n\
Tcp: 1 200 120000 -1 95 12 3 14 7 7170 5542 42 0 16 0\n\
Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors\n\
Udp: 30 0 0 28 0 0\n";

    /// Realistic `/proc/net/netstat` excerpt (trimmed TcpExt columns).
    const NETSTAT_FIXTURE: &str = "\
TcpExt: SyncookiesSent SyncookiesRecv TCPFastRetrans TCPSlowStartRetrans TCPTimeouts TCPLossProbes ListenOverflows ListenDrops\n\
TcpExt: 0 0 15 2 9 1 0 3\n\
IpExt: InNoRoutes InTruncatedPkts InMcastPkts\n\
IpExt: 0 0 5\n";

    #[test]
    fn combines_into_snapshot() {
        let snapshot = TcpSnapshot::combine(
            parse_snmp(SNMP_FIXTURE).unwrap(),
            parse_netstat(NETSTAT_FIXTURE).unwrap(),
        );

        assert_eq!(snapshot.retrans_segs, 42);
        assert_eq!(snapshot.out_segs, 5542);
        assert_eq!(snapshot.in_segs, 7170);
        assert_eq!(snapshot.curr_estab, 7);
        assert_eq!(snapshot.tcp_timeouts, 9);
        assert_eq!(snapshot.tcp_fast_retrans, 15);
    }
}
