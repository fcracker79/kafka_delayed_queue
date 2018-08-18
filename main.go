package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
	"zookeeper"
)

func main() {
	zookeeperConf := zookeeper.Zookeeper{
		Servers:        []string{"localhost:32181"},
		SessionTimeout: time.Minute,
	}
	zookeeperServersChannel := zookeeper.CreateBootstrapServersChannel(zookeeperConf)
	topicsChannel := zookeeper.CreateChannelListTopics(
		zookeeperConf,
		func(topic string) bool {
			return topic[0] != '_'
		})

	for {
		select {
		case kafkaServers := <-zookeeperServersChannel:
			config := sarama.NewConfig()
			config.ClientID = "AClientID"
			config.Admin.Timeout = time.Second * 30
			config.Version = sarama.V1_0_0_0
			fmt.Println("Kafka servers:", kafkaServers)
		case topics := <-topicsChannel:
			fmt.Println("Topics result:", topics)
		case t := <-time.After(time.Second * 10):
			fmt.Println("Timeout at", t)
		}
	}
	// bootstrapServers, err := zookeeper.GetBootstrapServers(zookeeperConf)
	// if err != nil {
	// 	os.Exit(1)
	// }
}
