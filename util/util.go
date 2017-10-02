package util

import (
	"fmt"
	"strconv"
	"strings"
)

func ParseHostPort(hp string) (host string, port int, err error) {
	if pos := strings.LastIndexByte(hp, ':'); pos == -1 {
		err = fmt.Errorf("%q: bad syntax for host:port", hp)
	} else {
		// host
		host = hp[0:pos]
		// if len(host) > 0 && net.ParseIP(host) == nil {
		// 	err = errors.New("bad IP address")
		// 	return
		// }
		// port
		if port, err = strconv.Atoi(hp[pos+1:]); err != nil {
			return
		}
		if port < 1 || port > 65535 {
			err = fmt.Errorf("bad value %d for port, must be within 1-65535", port)
			return
		}
	}
	return
}
