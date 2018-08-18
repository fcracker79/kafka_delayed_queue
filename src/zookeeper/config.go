package zookeeper

import "time"

type Zookeeper struct {
	Servers        []string
	SessionTimeout time.Duration
}
