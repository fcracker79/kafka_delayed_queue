package kafka

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"os"
	"os/signal"
	"strings"
	"time"
)

type CommandType int

const (
	STOP CommandType = iota
	RELOAD_TOPICS
	RELOAD_KAFKA_CONFIG
)

type Command struct {
	CommandType CommandType
	Data        interface{}
}

type ReloadTopicsData struct {
	topics   []string
	consumer *cluster.Consumer
}

type ReloadKafkaConfigData struct {
	kafkaServers []string
	topics       []string
	kafkaConfig  *sarama.Config
}

func consume(topics []string, master *cluster.Consumer) (chan *sarama.ConsumerMessage, chan Command, chan error) {
	consumers := make(chan *sarama.ConsumerMessage)
	errors := make(chan error)
	subCommandsChannels := make([]chan Command, 1)
	commandChannel := make(chan Command)

	go func() {
		for {
			for _, topic := range topics {
				if strings.Contains(topic, "__consumer_offsets") {
					continue
				}
				fmt.Println(" Start consuming topic ", topic)
				curCommandChannel := make(chan Command)
				subCommandsChannels = append(subCommandsChannels, curCommandChannel)
				fmt.Println("Consuming from topic %s", topic)
				go func(topic string, consumer *cluster.Consumer, commandsChannel chan Command) {
					errorsChannel := consumer.Errors()
					messagesChannel := consumer.Messages()
					for {
						fmt.Println("Dino!!!")
						select {
						case consumerError := <-errorsChannel:
							fmt.Println("ERROR", consumerError)
							errors <- consumerError
							fmt.Println("consumerError: ", consumerError)
						case msg := <-messagesChannel:
							fmt.Println("MESSAGE", msg)
							consumers <- msg
							fmt.Println("Got message on topic ", topic, msg.Value)
						case cmd := <-commandsChannel:
							if cmd.CommandType == STOP {
								fmt.Println("STOP")
								return
							}
						}
					}
				}(topic, master, curCommandChannel)
			}
			cmd := <-commandChannel
			if cmd.CommandType == RELOAD_TOPICS {
				reloadData := cmd.Data.(ReloadTopicsData)
				topics, master = reloadData.topics, reloadData.consumer
				for _, subCommandChannel := range subCommandsChannels {
					subCommandChannel <- Command{CommandType: STOP}
				}
				continue
			} else if cmd.CommandType == STOP {
				for _, subCommandChannel := range subCommandsChannels {
					subCommandChannel <- Command{CommandType: STOP}
				}
			}
		}
	}()

	return consumers, commandChannel, errors
}

func GetMessagesChannel(
	kafkaServers []string, groupID string, topics []string,
	kafkaConsumerConfig *cluster.Config,
	kafkaProducerConfig *sarama.Config) (chan Command, error) {
	commandsChannel := make(chan Command)
	consumer, err := cluster.NewConsumer(kafkaServers, groupID, topics, kafkaConsumerConfig)
	if err != nil {
		return nil, err
	}
	producer, err := sarama.NewSyncProducer(kafkaServers, kafkaProducerConfig)
	if err != nil {
		fmt.Printf("NewSyncProducerError %s", err)
		return nil, err
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumedMessages, subCommandChannel, errors := consume(topics, consumer)

	go func() {
		for {
			select {
			case msg := <-consumedMessages:
				fmt.Println("Got one message")
				messageJsonData := make(map[string]interface{})
				if err := json.Unmarshal(msg.Value, &messageJsonData); err != nil {
					fmt.Printf("Could not deserialize message %s", err)
					continue
				}
				if messageJsonData["expiration"] == nil {
					consumer.CommitOffsets()
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
				consumer.CommitOffsets()
			case errorMsg := <-errors:
				fmt.Println("Received error", errorMsg)
			case cmd := <-commandsChannel:
				if cmd.CommandType == RELOAD_KAFKA_CONFIG {
					data := cmd.Data.(ReloadKafkaConfigData)
					producer.Close()
					producer, err = sarama.NewSyncProducer(data.kafkaServers, data.kafkaConfig)
					if err != nil {
						panic(err)
					}
					consumer.Close()
					consumer, err = cluster.NewConsumer(kafkaServers, groupID, topics, kafkaConsumerConfig)
					if err != nil {
						panic(err)
					}
					topics = data.topics
					subCommandChannel <- Command{CommandType: RELOAD_TOPICS, Data: topics}
				} else if cmd.CommandType == STOP {
					subCommandChannel <- Command{CommandType: STOP}
					producer.Close()
					consumer.Close()
				} else {
					panic(cmd)
				}
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
