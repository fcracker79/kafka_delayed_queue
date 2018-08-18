package zookeeper

import (
	"encoding/json"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"time"
	"utils"
)

func GetBootstrapServers(zookeeper Zookeeper) ([]string, error) {
	c, _, err := zk.Connect(zookeeper.Servers, zookeeper.SessionTimeout)
	if err != nil {
		return nil, err
	}
	defer c.Close()

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

func CreateBootstrapServersChannel(zookeeper Zookeeper) chan []string {
	channel := make(chan []string)
	go func() {
		var bootstrapServers []string
		for {
			currentBootstrapServers, err := GetBootstrapServers(zookeeper)
			if err == nil {
				if !util.StringArrayEqual(bootstrapServers, currentBootstrapServers) {
					bootstrapServers = currentBootstrapServers
					channel <- currentBootstrapServers
				}
			}
			time.Sleep(time.Second * 30)
		}
	}()
	return channel
}
