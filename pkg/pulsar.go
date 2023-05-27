package pkg

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-restruct/restruct"
	jsoniter "github.com/json-iterator/go"
	"log"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

var MainTopic = "online2"

var client pulsar.Client

func Init(host string) {
	c, err := pulsar.NewClient(pulsar.ClientOptions{
		MaxConnectionsPerBroker: 10,
		URL:                     host,
	})
	if err != nil {
		panic(err)
	}
	client = c
}

var sizeOfMyStruct = int(unsafe.Sizeof(Container{}))

func MyStructToBytes(s *Container) []byte {
	var x reflect.SliceHeader
	x.Len = sizeOfMyStruct
	x.Cap = sizeOfMyStruct
	x.Data = uintptr(unsafe.Pointer(s))
	return *(*[]byte)(unsafe.Pointer(&x))
}
func BytesToMyStruct(b []byte) *Container {
	return (*Container)(unsafe.Pointer(
		(*reflect.SliceHeader)(unsafe.Pointer(&b)).Data,
	))
}

func Producer() {
	ctx := context.Background()
	// 创建producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: MainTopic,
	})

	if err != nil {
		log.Fatalf(" producer:%v", err)
	}

	defer producer.Close()
	i := 1
	for {
		if i >= 100 {
			break
		}
		// 随机睡眠
		time.Sleep(time.Second * time.Duration(rand.Intn(10)))
		pre := fmt.Sprintf("msspulsar%05d", i)
		// 生产消息
		msg := pre + `{"header":{"messageId":"f612fb49845bee6191ea05e1548aa7a2","namespace":"Appliance.Control.ToggleX","triggerSrc":"CloudAlexa","method":"GETACK","payloadVersion":1,"from":"/appliance/2201208098807451860148e1e986b2fb/publish","uuid":"2201208098807451860148e1e986b2fb","timestamp":1673925167,"timestampMs":749,"sign":"2e4375b4631d573499dd0b0585cee295"},"payload":{"channel":0,"togglex":{"channel":0,"onoff":1,"lmTime":1673911325}}}`
		clientId := "2201208098807451860148e1e986b2fb"
		MsgStruct := Container{
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

		if _, err := producer.Send(ctx, sendMsg); err != nil {
			log.Fatalf("Producer could not send message:%v", err)
		}
		//fmt.Println(fmt.Sprintf("[Send]\t%v\t%d\t", pre, time.Now().UnixNano()))
		fmt.Printf("%-15s\t%v\t%d\t\n", "[Send]", pre, time.Now().UnixNano())
		//fmt.Println("[Send]\t", pre, time.Now().UnixNano())
		i++
	}

}

type Container struct {
	Flags        uint8  `struct:"uint8"`
	Version      uint8  `struct:"uint8"`
	Cluster      uint8  `struct:"uint8"`
	QOS          uint8  `struct:"uint8"`
	IPV4         uint32 `struct:"uint32"`
	RevTime      uint64 `struct:"uint64"`
	ClientIdSize uint32 `struct:"uint32,sizeof=ClientId"`
	ClientId     string
	TopicSize    uint32 `struct:"uint32,sizeof=Topic"`
	Topic        string
	PayloadSize  uint32 `struct:"uint32,sizeof=Payload"`
	Payload      string
}

type Online struct {
	Flags        uint8  `struct:"uint8"`
	Version      uint8  `struct:"uint8"`
	Cluster      uint8  `struct:"uint8"`
	ClientIPV4   uint32 `struct:"uint32"`
	ClientPort   uint16 `struct:"uint16"`
	OfflineTime  uint64 `struct:"uint64"`
	OnlineTime   uint64 `struct:"uint64"`
	Status       uint8  `struct:"uint8"`
	ClientIdSize uint32 `struct:"uint32,sizeof=ClientId"`
	ClientId     string
	ReasonSize   uint32 `struct:"uint32,sizeof=Reason"`
	Reason       string
}

var Count int64
var StartTime int64
var LastTime int64
var LastCount int64

func LogCount(millsec int64) {
	for {
		time.Sleep(time.Duration(millsec) * time.Millisecond)
		if Count > 0 {
			fmt.Printf("%-15s\tstart:%v\tlast:%d\tlast-start:%d\tconsumeCount:%d\tconsume:%v\n", "[ReceivedCount]",
				StartTime,
				LastTime,
				LastTime-StartTime,
				Count,
				Count-LastCount)
			LastCount = Count
		}
	}
}

func Consumer(topic, sub string) {
	//使用client对象实例化consumer
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sub,
		Type:             pulsar.Shared,
	})

	if err != nil {
		//log.Fatal(err)
	}

	ctx := context.Background()
	defer consumer.Close()
	//无限循环监听topic
	fmt.Println("\n\n\ntime:", time.Now().Format("2006-01-02 15:04:05"))
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			log.Fatal(err)
		} else {
			//fmt.Printf("Received message :   %s \n", time.Now().Format("2006-01-02 15:04:05"))
		}

		Count++
		now := time.Now()
		LastTime = now.Unix()
		if Count == 1 {
			StartTime = LastTime
			fmt.Printf("%-15s\t%v\t%d\t\n", "[ReceivedCount]", LastTime, Count)
		}

		ms := msg.Payload()
		for i := 0; i < 1; i++ {
			resStruct := frestruct(ms)
			if len(resStruct.Payload) >= 34 && resStruct.Payload[0:9] == "msspulsar" {
				str := resStruct.Payload[15:34]
				sec, _ := strconv.ParseInt(str[0:10], 10, 64)
				nsec, _ := strconv.ParseInt(str[10:], 10, 64)
				sendTime := time.Unix(sec, nsec)
				fmt.Printf("%-15s\t%v\t%d\t%v\t%v\n",
					"[Received]",
					resStruct.Payload[0:14],
					now.UnixNano(),
					sendTime.UnixNano(),
					time.Now().Sub(sendTime))
			}
		}
		_ = consumer.Ack(msg)
	}
}

func ConsumerMine(topic string) {
	Init("pulsar://pulsar-cluster-dev.meross.dev.vpc:6650")
	//使用client对象实例化consumer
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		//Topic:            "persistent://meross/iot_raw/q_emqx_stat_msg",
		Topic:            "persistent://meross/iot_raw/" + topic,
		SubscriptionName: "sub",
		Type:             pulsar.Shared,
	})
	topicArr := strings.Split(topic, "/")
	last := topicArr[len(topicArr)-1]
	fmt.Println("[[[[[[[[[[[READ QUEUE]]]]]]]]]]]", last)

	if err != nil {
		//log.Fatal(err)
	}

	ctx := context.Background()
	defer consumer.Close()
	//无限循环监听topic
	fmt.Println("\n\n\ntime:", time.Now().Format("2006-01-02 15:04:05"))
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Received message :   %s \n", time.Now().Format("2006-01-02 15:04:05"))
		}

		Count++
		now := time.Now()
		LastTime = now.Unix()
		if Count == 1 {
			StartTime = LastTime
			fmt.Printf("%-15s\t%v\t%d\t\n", "[ReceivedCount]", LastTime, Count)
		}

		ms := msg.Payload()
		for i := 0; i < 1; i++ {
			if last == "q_emqx_online" {
				resStruct := frestructOnline(ms)
				fmt.Println(jsoniter.MarshalToString(resStruct))
			} else {
				resStruct := frestruct(ms)
				fmt.Println(jsoniter.MarshalToString(resStruct))
			}
		}
		_ = consumer.Ack(msg)
	}
}

func frestruct(bts []byte) *Container {
	c := Container{}
	restruct.Unpack(bts, binary.BigEndian, &c)
	return &c
}

func frestructOnline(bts []byte) *Online {
	c := Online{}
	restruct.Unpack(bts, binary.BigEndian, &c)
	return &c
}

func readBit(bts []byte) {
	buf := bytes.NewBuffer(bts)
	var Flags, Version, Cluster, QOS int8
	var IPV4, ClientIdSize, TopicSize, PayloadSize int32
	if err := binary.Read(buf, binary.BigEndian, &Flags); err != nil {
		panic(err)
	}
	if err := binary.Read(buf, binary.BigEndian, &Version); err != nil {
		panic(err)
	}
	if err := binary.Read(buf, binary.BigEndian, &Cluster); err != nil {
		panic(err)
	}
	if err := binary.Read(buf, binary.BigEndian, &QOS); err != nil {
		panic(err)
	}
	if err := binary.Read(buf, binary.BigEndian, &IPV4); err != nil {
		panic(err)
	}

	if err := binary.Read(buf, binary.BigEndian, &ClientIdSize); err != nil {
		panic(err)
	}
	ClientIdbuf := make([]byte, ClientIdSize/8)
	if err := binary.Read(buf, binary.BigEndian, &ClientIdbuf); err != nil {
		panic(err)
	}
	//fmt.Println(string(ClientIdbuf))

	if err := binary.Read(buf, binary.BigEndian, &TopicSize); err != nil {
		panic(err)
	}
	Topicbuf := make([]byte, TopicSize/8)
	if err := binary.Read(buf, binary.BigEndian, &Topicbuf); err != nil {
		panic(err)
	}
	//fmt.Println(string(Topicbuf))

	if err := binary.Read(buf, binary.BigEndian, &PayloadSize); err != nil {
		panic(err)
	}
	Payloadbuf := make([]byte, PayloadSize/8)
	if err := binary.Read(buf, binary.BigEndian, &Payloadbuf); err != nil {
		panic(err)
	}
	//fmt.Println(string(Payloadbuf))
}

func BytesToBinaryString(bs []byte) string {
	buf := bytes.NewBuffer([]byte{})
	for _, v := range bs {
		buf.WriteString(fmt.Sprintf("%08b", v))
	}
	//fmt.Println(buf.String())
	return buf.String()
}
