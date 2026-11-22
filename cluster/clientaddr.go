package cluster

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"unicode"
)

// ValidateClientAddr checks the client address a member row publishes. Every driver that routes by
// TOPOLOGY sends its user name and password to that address after the TLS handshake, so a row that
// can name any host is a credential-redirect primitive. It must be host:port with a numeric port
// from 1 to 65535, hold no scheme, user info, space or control character, name no wildcard or
// zoned address, and sit on the same host as the member's data address (a different port is fine;
// the data address is the one the member's own pinned peer key vouches for). The command applier
// runs it, so every replica and every replay gives the same answer.
func ValidateClientAddr(clientAddr, dataAddr string) error {
	if len(clientAddr) > MaxClientAddrBytes {
		return fmt.Errorf("the client address is %d bytes and at most %d are kept (a host name of at most %d characters, a colon and the port)", len(clientAddr), MaxClientAddrBytes, MaxClientAddrBytes-6)
	}
	for _, r := range clientAddr {
		if unicode.IsSpace(r) || unicode.IsControl(r) || r == '@' || r == '/' || r == '%' {
			return fmt.Errorf("the client address %q holds %q: write plain host:port, with no scheme, user info, space or zone", clientAddr, r)
		}
	}
	host, port, err := net.SplitHostPort(clientAddr)
	if err != nil || host == "" {
		return fmt.Errorf("the client address %q is not host:port: write it as host:port, for example db1.example:7434", clientAddr)
	}
	if number, err := strconv.ParseUint(port, 10, 16); err != nil || number == 0 {
		return fmt.Errorf("the client address %q has port %q: use a number from 1 to 65535", clientAddr, port)
	}
	if ip, err := netip.ParseAddr(host); err == nil && ip.Unmap().IsUnspecified() {
		return fmt.Errorf("the client address %q is a wildcard no driver can dial: name the address drivers use", clientAddr)
	}
	dataHost, _, err := net.SplitHostPort(dataAddr)
	if err != nil || !sameHost(host, dataHost) {
		return fmt.Errorf("the client address %q is not on the host of the data address %q: a node's client address must be on its own host (a different port is fine), or drivers would send their password elsewhere", clientAddr, dataAddr)
	}
	return nil
}

// sameHost compares two hosts as addresses when both parse (a 4-in-6 form equals its IPv4), and as
// names otherwise, ignoring case and one trailing dot.
func sameHost(a, b string) bool {
	left, leftErr := netip.ParseAddr(a)
	right, rightErr := netip.ParseAddr(b)
	if leftErr == nil && rightErr == nil {
		return left.Unmap() == right.Unmap()
	}
	if leftErr == nil || rightErr == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSuffix(a, "."), strings.TrimSuffix(b, "."))
}
