// Copyright 2017 RapidLoop, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
