package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"
	"time"
	"utils"
)

type TopicFilter func(topic string) bool

func ListTopics(zookeeper Zookeeper, filter TopicFilter) ([]string, error) {
	c, _, err := zk.Connect(zookeeper.Servers, zookeeper.SessionTimeout)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	topics, _, err := c.Children("/brokers/topics")
	if err != nil {
		return nil, err
	}

	filteredTopics := make([]string, 0, len(topics))
	for _, topic := range topics {
		if filter(topic) {
			filteredTopics = append(filteredTopics, topic)
		}
	}

	return filteredTopics, nil
}

func CreateChannelListTopics(zookeeper Zookeeper, filter TopicFilter) chan []string {
	channel := make(chan []string)
	go func() {
		var topics []string
		for {
			currentTopics, err := ListTopics(zookeeper, filter)
			if err == nil {
				if !util.StringArrayEqual(topics, currentTopics) {
					topics = currentTopics
					channel <- currentTopics
				}
			}
			time.Sleep(time.Second * 30)
		}
	}()
	return channel
}
