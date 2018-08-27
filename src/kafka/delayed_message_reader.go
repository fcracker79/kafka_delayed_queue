package kafka

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"strings"
	"time"
)

func consume(topics []string, master sarama.Consumer, commandsChannel chan int) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
	consumers := make(chan *sarama.ConsumerMessage)
	errors := make(chan *sarama.ConsumerError)

	go func() {
		for {
			for _, topic := range topics {
				if strings.Contains(topic, "__consumer_offsets") {
					continue
				}
				partitions, _ := master.Partitions(topic)
				for partition := range partitions {
					consumer, err := master.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
					if nil != err {
						fmt.Printf("Topic %v Partitions: %v", topic, partitions)
						panic(err)
					}
					fmt.Println(" Start consuming topic ", topic)
					go func(topic string, consumer sarama.PartitionConsumer) {
						for {
							select {
							case consumerError := <-consumer.Errors():
								errors <- consumerError
								fmt.Println("consumerError: ", consumerError.Err)
							case msg := <-consumer.Messages():
								consumers <- msg
								fmt.Println("Got message on topic ", topic, msg.Value)
							case <-commandsChannel:
								return
							}
						}
					}(topic, consumer)
				}
			}
		}
	}()

	return consumers, errors
}

func GetMessagesChannel(kafkaServers []string, topics []string, kafkaConfig *sarama.Config) (chan int, error) {
	commandsChannel := make(chan int)
	subCommandsChannel := make(chan int)
	consumer, err := sarama.NewConsumer(kafkaServers, kafkaConfig)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducer(kafkaServers, kafkaConfig)
	if err != nil {
		return nil, err
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumedMessages, errors := consume(topics, consumer, subCommandsChannel)

	go func() {
		for {
			select {
			case msg := <-consumedMessages:
				messageJsonData := make(map[string]interface{})
				if err := json.Unmarshal(msg.Value, &messageJsonData); err != nil {
					continue
				}
				if messageJsonData["expiration"].(int64) > time.Now().Unix() {
					producer.SendMessage(&sarama.ProducerMessage{
						Key:   sarama.ByteEncoder(msg.Key),
						Value: sarama.ByteEncoder(msg.Value), Topic: msg.Topic})
				} else {
					origKey, _ := base64.StdEncoding.DecodeString(messageJsonData["origKey"].(string))
					origValue, _ := base64.StdEncoding.DecodeString(messageJsonData["origValue"].(string))
					origTopic, _ := messageJsonData["origTopic"].(string)
					producer.SendMessage(&sarama.ProducerMessage{
						Key:   sarama.ByteEncoder(origKey),
						Value: sarama.ByteEncoder(origValue), Topic: origTopic})
				}
			case errorMsg := <-errors:
				fmt.Println("Received error", errorMsg)
			case <-commandsChannel:
				consumer.Close()
				subCommandsChannel <- 0
			}
		}
	}()

	return commandsChannel, nil
	// defer func() {
	// 	if err := consumer.Close(); err != nil {
	// 		panic(err)
	// 	}
	// }()
}
