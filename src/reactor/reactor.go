package reactor

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"kafka"
	"time"
	"zookeeper"
)

func StartReactor(zookeeperServers []string, clientID string, consumerGroup string) {
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
	var kafkaPublisherConfig *sarama.Config
	var kafkaConsumerConfig *cluster.Config
	var topics []string

	foundKafkaServers, foundTopics := false, false
	for !foundKafkaServers || !foundTopics {
		kafkaServers = <-zookeeperServersChannel
		kafkaConsumerConfig = cluster.NewConfig()
		kafkaPublisherConfig = sarama.NewConfig()

		kafkaPublisherConfig.Producer.Return.Successes = true
		kafkaPublisherConfig.ClientID = clientID
		kafkaPublisherConfig.Admin.Timeout = time.Second * 30
		kafkaPublisherConfig.Version = sarama.V1_0_0_0

		kafkaConsumerConfig.Producer.Return.Successes = true
		kafkaConsumerConfig.ClientID = clientID
		kafkaConsumerConfig.Admin.Timeout = time.Second * 30
		kafkaConsumerConfig.Version = sarama.V1_0_0_0

		fmt.Println("Kafka servers:", kafkaServers)
		foundKafkaServers = len(kafkaServers) > 0
		topics = <-topicsChannel
		fmt.Println("TODO 2 got", topics)
		fmt.Println("Topics result:", topics)
		foundTopics = len(topics) > 0
		fmt.Println("condition %v (%v %v)", !foundKafkaServers || !foundTopics, foundKafkaServers, foundTopics)
	}

	fmt.Println("Starting listening to messages on hosts %v, topics %v", kafkaServers, topics)
	for {
		commandsChannel, err := kafka.GetMessagesChannel(
			kafkaServers, consumerGroup, topics, kafkaConsumerConfig, kafkaPublisherConfig)
		if err != nil {
			panic(err)
		}
		select {
		case kafkaServers = <-zookeeperServersChannel:
			kafkaPublisherConfig = sarama.NewConfig()
			kafkaPublisherConfig.ClientID = clientID
			kafkaPublisherConfig.Admin.Timeout = time.Second * 30
			kafkaPublisherConfig.Version = sarama.V1_0_0_0
			fmt.Println("Kafka servers:", kafkaServers)
			commandsChannel <- kafka.Command{CommandType: kafka.STOP, Data: nil}
		case topics = <-topicsChannel:
			fmt.Println("Topics result:", topics)
			fmt.Println("TODO 3 got", topics)
			commandsChannel <- kafka.Command{CommandType: kafka.STOP, Data: nil}
		}
	}
}
