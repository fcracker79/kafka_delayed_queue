package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"
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
	return topics, nil
}
