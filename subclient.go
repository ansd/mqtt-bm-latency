package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/GaryBoone/GoStats/stats"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type SubClient struct {
	ID         int
	BrokerURL  string
	BrokerUser string
	BrokerPass string
	SubTopic   string
	SubQoS     byte
	KeepAlive  int
	Quiet      bool
	MsgCount   int
}

func (c *SubClient) run(res chan *SubResults, subDone chan bool) {
	runResults := new(SubResults)
	runResults.ID = c.ID

	forwardLatency := []float64{}
	receivedAllMsgs := make(chan bool, 1)

	ka, err := time.ParseDuration(strconv.Itoa(c.KeepAlive) + "s")
	if err != nil {
		log.Fatal(err)
	}

	opts := mqtt.NewClientOptions().
		AddBroker(c.BrokerURL).
		SetClientID(fmt.Sprintf("mqtt-benchmark-%v-%v", time.Now(), c.ID)).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetKeepAlive(ka).
		SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
			recvTime := time.Now().UnixNano()
			payload := msg.Payload()
			i := 0
			for ; i < len(payload)-3; i++ {
				if payload[i] == '#' && payload[i+1] == '@' && payload[i+2] == '#' {
					sendTime, err := strconv.ParseInt(string(payload[:i]), 10, 64)
					if err != nil {
						log.Fatal(err)
					}
					forwardLatency = append(forwardLatency, float64(recvTime-sendTime)/1_000_000) // in milliseconds
					break
				}
			}
			runResults.Received++
			if runResults.Received == int64(c.MsgCount) {
				receivedAllMsgs <- true
			}
		}).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Printf("SUBSCRIBER %v lost connection to the broker: %v. Will reconnect...\n", c.ID, reason.Error())
		})
	if c.BrokerUser != "" && c.BrokerPass != "" {
		opts.SetUsername(c.BrokerUser)
		opts.SetPassword(c.BrokerPass)
	}
	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("SUBSCRIBER %v had error connecting to the broker: %v\n", c.ID, token.Error())
		return
	}

	if token := client.Subscribe(c.SubTopic, c.SubQoS, nil); token.Wait() && token.Error() != nil {
		log.Printf("SUBSCRIBER %v had error subscribe with topic: %v\n", c.ID, token.Error())
		return
	}

	if !c.Quiet {
		log.Printf("SUBSCRIBER %v had connected to the broker: %v and subscribed with topic: %v\n", c.ID, c.BrokerURL, c.SubTopic)
	}

	subDone <- true
	//加各项统计
	for {
		select {
		case <-receivedAllMsgs:
			client.Disconnect(250)
			runResults.FwdLatencyMin = stats.StatsMin(forwardLatency)
			runResults.FwdLatencyMax = stats.StatsMax(forwardLatency)
			runResults.FwdLatencyMean = stats.StatsMean(forwardLatency)
			runResults.FwdLatencyStd = stats.StatsSampleStandardDeviation(forwardLatency)
			res <- runResults
			if !c.Quiet {
				log.Printf("SUBSCRIBER %v is done subscribe\n", c.ID)
			}
			return
		}
	}
}
