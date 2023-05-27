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

func main() {
	//action := flag.String("a", "", "Action:\t\t执行动作e")
	host := flag.String("h", "", fmt.Sprintf("%-15s%s", "host", "主机地址"))
	topic := flag.String("t", "", fmt.Sprintf("%-15s%s", "subTopic", "主题"))
	addr := flag.String("addr", "", fmt.Sprintf("%-15s%s", "localAddress", "指定源IP"))
	qos := flag.Int("qos", 1, fmt.Sprintf("%-15s%s", "qos", "Qos等级"))
	version := flag.Int("v", 3, fmt.Sprintf("%-15s%s", "mqttVersion", "MQTT版本"))
	useSsl := flag.Bool("ssl", false, fmt.Sprintf("%-15s%s", "openSsl", "是否启用ssl"))
	port := flag.Int("p", 1883, fmt.Sprintf("%-15s%s", "port", "端口号"))
	node := flag.Int("n", 0, fmt.Sprintf("%-15s%s", "node", "节点编号，不指定则随机生成，可能出现碰撞"))

	pubRate := flag.Int("pr", 1, fmt.Sprintf("%-15s%s", "pubRate", "发布消息速率/秒"))
	pubMsgCount := flag.Int("pmc", 0, fmt.Sprintf("%-15s%s", "pubMsgNum", "发布消息总数量"))
	pubTopic := flag.String("pt", "", fmt.Sprintf("%-15s%s\n%-15s%s", "pubTopic", "发布的主题，空则不发布，支持变量，如：/app/<node-len-i-num>/pub", "", "其中i表示index递增，node为节点id，num表示向几个topic发消息"))

	subTopic := flag.String("st", "", fmt.Sprintf("%-15s%s", "subTopic", "订阅的主题，空则不订阅，支持变量，如：/app/<len-i>/sub"))

	clientCount := flag.Int("cc", 1, fmt.Sprintf("%-15s%s", "clientNum", "客户端数量"))
	clientRate := flag.Int("cr", 1, fmt.Sprintf("%-15s%s", "clientRate", "客户端创建速度/秒"))

	//解析命令行参数写入注册的flag里
	flag.Parse()
	request := &model.Request{
		//Action:       *action,
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

	switch request.Action {
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
			switch request.Action {
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
