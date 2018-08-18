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
	zookeeperServersChannel := zookeeper.CreateBootstrapServersChannel(zookeeperConf)
	for {
		select {
		case zookeeperServers := <-zookeeperServersChannel:
			fmt.Println("Kafka servers:", zookeeperServers)
		case t := <-time.After(time.Second * 10):
			fmt.Println("Timeout at", t)
		}
	}
	// bootstrapServers, err := zookeeper.GetBootstrapServers(zookeeperConf)
	// if err != nil {
	// 	os.Exit(1)
	// }
}
