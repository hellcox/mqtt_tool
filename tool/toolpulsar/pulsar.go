package toolpulsar

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-restruct/restruct"
	jsoniter "github.com/json-iterator/go"
	"log"
	"math"
	"pulsar-demo/model"
	"strings"
	"time"
)

var client pulsar.Client

var IsStop bool

func init() {
	IsStop = false
}

func Consume(req model.Request) {
	if req.Host == "" {
		panic("参数错误：host")
	}
	if req.Topic == "" {
		panic("参数错误：topic")
	}
	Init(req.Host)

	//使用client对象实例化consumer
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            req.Topic, // "persistent://meross/iot_raw/q_emqx_stat_msg",
		SubscriptionName: "tool-sub",
		Type:             pulsar.Shared,
	})
	if err != nil {
		panic(err)
	}
	//if req.LogCount == 1 {
	//	LogCount(1000, true, req.Ext)
	//}

	topicArr := strings.Split(req.Topic, "/")
	last := topicArr[len(topicArr)-1]
	fmt.Println("\n\n\n=====> READ QUEUE <=====", last, time.Now().Format("2006-01-02 15:04:05"), "\n\n\n")

	ctx := context.Background()
	defer consumer.Close()
	//无限循环监听topic
	for {
		if IsStop {
			break
		}
		msg, err := consumer.Receive(ctx)
		if err != nil {
			log.Fatal(err)
		} else {
			// fmt.Printf("Received message :   %s \n", time.Now().Format("2006-01-02 15:04:05"))
		}

		Count++
		now := time.Now()
		LastTime = now.Unix()
		if Count == 1 {
			StartTime = LastTime
			fmt.Printf("%-15s\t%v\t%d\t\n", "[开始消费]", LastTime, Count)
		}

		ms := msg.Payload()
		for i := 0; i < 1; i++ {
			if last == "q_emqx_online" { // 上下线消息
				resStruct := frestructOnline(ms)
				_ = resStruct
				//if req.Log == 1 {
				//	fmt.Println(jsoniter.MarshalToString(resStruct))
				//}
			} else { // 常规消息
				resStruct := frestruct(ms)
				//if req.Log == 1 {
				//	fmt.Println(jsoniter.MarshalToString(resStruct))
				//}
				testStr := jsoniter.Get([]byte(resStruct.Payload), "hellcox-test").ToString()
				// 不是测试工具的消息
				if testStr == "" {
					continue
				}
				TestCount++
				TestLastTime = now.Unix()
				if TestCount == 1 {
					TestStartTime = TestLastTime
					fmt.Printf("%-15s\t%v\t%d\t\n", "[开始消费特殊消息]", TestStartTime, TestCount)
				}
				testArr := strings.Split(testStr, "-")
				_ = testArr
				//if req.Ext == "special" {
				//	sec, _ := strconv.ParseInt(testArr[1][0:10], 10, 64)
				//	nsec, _ := strconv.ParseInt(testArr[1][10:], 10, 64)
				//	sendTime := time.Unix(sec, nsec)
				//	timeSub := time.Now().Sub(sendTime)
				//	specialTime = specialTime + timeSub.Milliseconds()
				//	fmt.Printf("%-15s\tID:%v\t生成时间:%v\t当前时间:%v\t时延:%v\t时延:%vms\n",
				//		"[特殊消息]",
				//		testArr[0],
				//		sendTime.UnixNano(),
				//		now.UnixNano(),
				//		timeSub,
				//		timeSub.Milliseconds())
				//}
			}
		}
		_ = consumer.Ack(msg)
	}
}

func Init(host string) {
	c, err := pulsar.NewClient(pulsar.ClientOptions{
		MaxConnectionsPerBroker: 5,
		URL:                     host,
	})
	if err != nil {
		panic(err)
	}
	client = c
}

func Producer(req model.Request) {
	Init(req.Host)
	ctx := context.Background()
	if req.Topic == "" {
		req.Topic = "test"
	}
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: req.Topic,
	})

	if err != nil {
		log.Fatalf(" producer:%v", err)
	}

	defer producer.Close()

	msg := `{"header":{"messageId":"f612fb49845bee6191ea05e1548aa7a2","namespace":"Appliance.Control.ToggleX","triggerSrc":"CloudAlexa","method":"PUSH","payloadVersion":1,"from":"/appliance/2201208098807451860148e1e986b2fb/publish","uuid":"2201208098807451860148e1e986b2fb","timestamp":1673925167,"timestampMs":749,"sign":"2e4375b4631d573499dd0b0585cee295"},"payload":{"channel":0,"togglex":{"channel":0,"onoff":1,"lmTime":1673911325}}}`
	clientId := "2201208098807451860148e1e986b2fb"
	MsgStruct := model.NormalMsg{
		Flags:        1,
		Version:      1,
		Cluster:      1,
		QOS:          1,
		IPV4:         2130706433,
		RevTime:      uint64(time.Now().UnixMilli()),
		ClientIdSize: uint32(len(clientId)),
		ClientId:     clientId,
		TopicSize:    uint32(len(clientId)),
		Topic:        clientId,
		PayloadSize:  uint32(len(msg)),
		Payload:      msg,
	}

	Data, _ := restruct.Pack(binary.BigEndian, &MsgStruct)
	sendMsg := &pulsar.ProducerMessage{
		Payload: Data,
	}

	sd, err := producer.Send(ctx, sendMsg)
	if err != nil {
		log.Fatalf("Producer could not send message:%v", err)
	}
	fmt.Println(sd)

}

var Count int64     // 所有消息总数
var StartTime int64 //所有消息开始时间
var LastTime int64  //所有消息结束时间
var LastCount int64
var TestCount int64     //测试消息总数
var TestStartTime int64 //测试消息开始时间
var TestLastTime int64  //测试消息结束时间
var TestLastCount int64
var specialCount int64 //处理特殊消息条数
var specialTime int64  //特殊消息总耗时

func LogCount(millsec int64, isForeach bool, isSpecial string) {
	if !isForeach {
		limit := 0
		if specialCount > 0 && specialTime > 0 {
			limit = int(math.Ceil(float64(specialTime / specialCount)))
		}
		if isSpecial == "special" {
			fmt.Println(fmt.Sprintf("[%d]特殊消息:%d, 平均耗时 %v 毫秒/条", time.Now().Unix(), specialCount, limit))
		}
		fmt.Println(fmt.Sprintf("[%d]总:%d, 耗时%v", time.Now().Unix(), Count, LastTime-StartTime))
		return
	}

	go func() {
		for {
			time.Sleep(time.Duration(millsec) * time.Millisecond)
			Now := Count
			if Now == LastCount {
				continue
			}
			if Now > 0 {
				fmt.Printf("%-15s\t开始:%v\t结束:%d\t时间差:%d\t消费总数:%d\t本次消费:%v\n", "[统计所有消息]",
					StartTime,
					LastTime,
					LastTime-StartTime,
					Now,
					Now-LastCount)
				LastCount = Now
			}
			if isSpecial == "special" {
				NowSpecial := TestCount
				if NowSpecial > 0 {
					fmt.Printf("%-15s\t开始:%v\t结束:%d\t时间差:%d\t消费总数:%d\t本次消费:%v\n", "[统计工具消息]",
						TestStartTime,
						TestLastTime,
						TestLastTime-TestStartTime,
						NowSpecial,
						NowSpecial-TestLastCount)
					TestLastCount = NowSpecial
				}
			}
			if IsStop {
				break
			}
		}
	}()
}

func frestruct(bts []byte) *model.NormalMsg {
	c := model.NormalMsg{}
	_ = restruct.Unpack(bts, binary.BigEndian, &c)
	return &c
}

func frestructOnline(bts []byte) *model.OnlineMsg {
	c := model.OnlineMsg{}
	_ = restruct.Unpack(bts, binary.BigEndian, &c)
	return &c
}
