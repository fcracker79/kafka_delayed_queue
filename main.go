package main

import (
	"fmt"
	"time"
	"zookeeper"
)

func main() {
	zookeeperConf := zookeeper.Zookeeper{
		Servers:        []string{"localhost:32181"},
		SessionTimeout: time.Minute,
	}
	fmt.Println(zookeeper.GetBootstrapServers(zookeeperConf))
}
