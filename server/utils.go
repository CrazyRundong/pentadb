package server

import (
	"net"
)

// get host IP
// https://gist.github.com/jniltinho/9787946
func GetMyIP() ([]net.IP, error) {
	retIPs := make([]net.IP, 0, 5)
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				retIPs = append(retIPs, ipnet.IP)
			}
		}
	}

	return retIPs, nil
}
