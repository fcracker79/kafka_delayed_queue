package zookeeper

import (
	"encoding/json"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type Zookeeper struct {
	Servers        []string
	SessionTimeout time.Duration
}

func GetBootstrapServers(zookeeper Zookeeper) ([]string, error) {
	c, _, err := zk.Connect(zookeeper.Servers, zookeeper.SessionTimeout)
	if err != nil {
		panic(err)
	}

	bootstrapServers, _, err := c.Children("/brokers/ids")
	if err != nil {
		return nil, err
	}

	result := make([]string, 1)

	for _, broker := range bootstrapServers {
		brokerNodePath := fmt.Sprintf("/brokers/ids/%s", broker)
		brokerData, _, err := c.Get(brokerNodePath)
		if err != nil {
			return nil, err
		}
		brokerJsonData := make(map[string]interface{})
		if err := json.Unmarshal(brokerData, &brokerJsonData); err != nil {
			return nil, err
		}
		result = append(result, fmt.Sprintf("%s:%v", brokerJsonData["host"], brokerJsonData["port"]))
	}
	return result, nil
}
