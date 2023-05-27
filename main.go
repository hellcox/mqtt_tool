package main

import (
	"flag"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"math/rand"
	"os"
	"os/signal"
	"pulsar-demo/model"
	"pulsar-demo/tool"
	"pulsar-demo/tool/toolpulsar"
	"syscall"
	"time"
)

// produce：生成EMQX消息，go run .\main.go -a produce -h 127.0.0.1:1883 -nc 20 -n 0 -r 100 -b 10000 -e special -l 1 -t fw/pub/cloud/nack
// consume：消费Pulsal消息, go run main.go -a consume -h pulsar://pulsar-cluster-dev.meross.dev.vpc:6650 -e special -t persistent://meross/iot_raw/q_emqx_online

func main() {
	action := flag.String("a", "", "Action:\t执行动作：produce/consume")
	host := flag.String("h", "", "Host:\t\t主机地址")
	topic := flag.String("t", "", "SubTopic:\t\t主题")
	pubRate := flag.Int("pr", 1, "PubRate:\t\t发布消息速率/秒，默认1")
	pubMsgCount := flag.Int("pmc", 0, "PubMsgNum:\t\t发布消息总数量，默认0，表示一直发布")
	// /appliance/<32-i>/publish  /app/10500882-appid/subscribe  /appliance/<32-i>/subscribe
	pubTopic := flag.String("pt", "", "PubTopic:\t\t发布的主题，空则不发布，%i")
	// /appliance/<32-i>/subscribe /app/<8-i>-appid/subscribe
	subTopic := flag.String("st", "", "SubTopic:\t\t订阅的主题，空则不订阅，%i")
	clientCount := flag.Int("cc", 1, "ClientNum:\t\t客户端数量，默认1")
	clientRate := flag.Int("cr", 1, "ClientRate:\t\t客户端创建速度/秒，默认1")
	addr := flag.String("addr", "", "LocalAddress:\t\t指定源IP")
	qos := flag.Int("qos", 1, "qos:\t\tQos等级，默认1")
	version := flag.Int("v", 3, "MqttVersion:\t\tMQTT版本，默认3")
	useSsl := flag.Bool("ssl", false, "OpenSsl:\t\t是否启用ssl，默认false")
	port := flag.Int("p", 1883, "Port:\t\t端口号：默认1883")
	node := flag.Int("n", 0, "node:\t\t节点编号，不指定则随机生成，可能出现碰撞")
	//解析命令行参数写入注册的flag里
	flag.Parse()
	request := &model.Request{
		Action:       *action,
		Host:         *host,
		Topic:        *topic,
		LocalAddress: *addr,
		ClientCount:  *clientCount,
		ClientRate:   *clientRate,
		PubRate:      *pubRate,
		PubMsgCount:  *pubMsgCount,
		UseSsl:       *useSsl,
		Qos:          *qos,
		Version:      *version,
		Port:         *port,
		PubTopic:     *pubTopic,
		SubTopic:     *subTopic,
	}
	if *node == 0 {
		rand.Seed(time.Now().UnixNano())
		request.Node = fmt.Sprintf("%03d", rand.Intn(1000))
	} else {
		request.Node = fmt.Sprintf("%03d", *node)
	}
	if len(request.Node) > 3 {
		panic("node length need <= 3")
	}

	fmt.Println(jsoniter.MarshalToString(request))

	switch *action {
	case "produce":
		//go toolemqx.Producer(*request)
	case "consume":
		go toolpulsar.Consume(*request)
	default:
		tool.Init(*request)
	}

	// 优雅关闭，等待关闭信号
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	select {
	case sig := <-c:
		{
			switch *action {
			case "produce":
				//toolemqx.IsStop = true
				time.Sleep(1 * time.Second)
				//toolemqx.LogCount(false)
			case "consume":
				toolpulsar.IsStop = true
				time.Sleep(1 * time.Second)
				//toolpulsar.LogCount(1000, false, request.Ext)
			}
			_ = sig
			os.Exit(1)
		}
	}
}
