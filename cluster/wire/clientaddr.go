package wire

import (
	"net"
	"net/netip"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// ClientAddrFor returns the client address a node commits for itself (the tcp.advertise and
// tcp.listen keys). An advertise address is the operator's word and is taken as written; the
// cluster still checks it when it commits (cluster.ValidateClientAddr). Without one, the listen
// address is committed only when another machine could dial it and the cluster would accept
// it: a wildcard, loopback, hostless, zoned or malformed listen address ("0.0.0.0:7434",
// "127.0.0.1:7434", ":7434", "localhost.:7434", "[fe80::1%eth0]:7434") would send every driver
// to itself or nowhere, so it commits nothing and drivers fall back to the data address.
func ClientAddrFor(advertise, listen string) string {
	if advertise != "" {
		return advertise
	}
	host, _, err := net.SplitHostPort(listen)
	if err != nil || cluster.ValidateClientAddr(listen, listen) != nil {
		return ""
	}
	if strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") {
		return ""
	}
	if ip, err := netip.ParseAddr(host); err == nil && ip.Unmap().IsLoopback() {
		return ""
	}
	return listen
}
