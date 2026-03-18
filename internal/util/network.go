package util

import "net"

// get outbound address without opening a connection
// useful for cases when you need to determine which IP to announce to cluster members
func GetOutboundIP(target string) net.IP {
	conn, err := net.Dial("udp", net.JoinHostPort(target, "80"))
	if err != nil {
		return nil
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP
}
