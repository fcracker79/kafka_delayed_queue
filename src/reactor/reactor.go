package reactor

import (
	"fmt"
	"github.com/Shopify/sarama"
	"kafka"
	"time"
	"zookeeper"
)

func StartReactor(zookeeperServers []string, clientID string) {
	zookeeperConf := zookeeper.Zookeeper{
		Servers:        zookeeperServers,
		SessionTimeout: time.Minute,
	}
	zookeeperServersChannel := zookeeper.CreateBootstrapServersChannel(zookeeperConf)
	topicsChannel := zookeeper.CreateChannelListTopics(
		zookeeperConf,
		func(topic string) bool {
			return topic[0] != '_'
		})

	var kafkaServers []string
	var config *sarama.Config
	var topics []string

	for {
		select {
		case kafkaServers = <-zookeeperServersChannel:
			config = sarama.NewConfig()
			config.ClientID = clientID
			config.Admin.Timeout = time.Second * 30
			config.Version = sarama.V1_0_0_0
			fmt.Println("Kafka servers:", kafkaServers)
			if len(kafkaServers) > 0 && len(topics) > 0 && config != nil {
				break
			}
		case topics = <-topicsChannel:
			fmt.Println("Topics result:", topics)
			if len(kafkaServers) > 0 && len(topics) > 0 && config != nil {
				break
			}
		}
	}
	for {
		commandsChannel, err := kafka.GetMessagesChannel(kafkaServers, topics, config)
		if err != nil {
			panic(err)
		}
		select {
		case kafkaServers = <-zookeeperServersChannel:
			config = sarama.NewConfig()
			config.ClientID = clientID
			config.Admin.Timeout = time.Second * 30
			config.Version = sarama.V1_0_0_0
			fmt.Println("Kafka servers:", kafkaServers)
			commandsChannel <- kafka.Command{CommandType: kafka.STOP, Data: nil}
		case topics = <-topicsChannel:
			fmt.Println("Topics result:", topics)
			commandsChannel <- kafka.Command{CommandType: kafka.STOP, Data: nil}
		}
	}
}
