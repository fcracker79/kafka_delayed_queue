package main

import (
	"fmt"
	"os"
	"time"
	"zookeeper"
)

func main() {
	zookeeperConf := zookeeper.Zookeeper{
		Servers:        []string{"localhost:32181"},
		SessionTimeout: time.Minute,
	}
	bootstrapServers, err := zookeeper.GetBootstrapServers(zookeeperConf)
	if err != nil {
		os.Exit(1)
	}
	fmt.Println(bootstrapServers)
}
