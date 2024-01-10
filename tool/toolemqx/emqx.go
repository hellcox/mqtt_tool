package toolemqx

import (
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/pkg/errors"
	"math"
	"math/rand"
	"net"
	"pulsar-demo/model"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var ConnSize int64
var ConnSizeOri int64
var ConnFailSize int64
var PubSize int64
var PubSizeOri int64
var PubFailSize int64
var SubMsgSize int64
var Sub int64
var SubFail int64
var PubIndex int64
var LastErr error
var ClientRateList chan int

func init() {
}

func Init(req model.Request) {
	// 初始化chanList
	ClientRateList = make(chan int, req.ClientRate)
	for i := 0; i < req.ClientRate; i++ {
		ClientRateList <- i
	}
}

func Conn(ctx context.Context, req model.Request, index int64) {
	idx := fmt.Sprintf("%s-%010d", req.Node, index)
	host := fmt.Sprintf("%s:%d", req.Host, req.Port)
	if req.UseSsl {
		host = "ssl://" + host
	}
	opts := mqtt.NewClientOptions().AddBroker(host)
	opts.SetClientID("fmware:2012316814712100031634298f1f2ade_ATyf7vIBygFZzfKI")
	opts.SetUsername("34:29:8f:1f:2a:de")
	opts.SetPassword("10500882_cb9b34f155590616a367e993feeae615")
	opts.SetDefaultPublishHandler(func(c mqtt.Client, message mqtt.Message) {
		fmt.Println("PublishHandler", message)
	})
	opts.SetDialer(&net.Dialer{
		LocalAddr: &net.TCPAddr{
			IP: net.ParseIP(req.LocalAddress),
		},
	})
	opts.SetAutoReconnect(true)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetConnectTimeout(5 * time.Second)
	opts.SetMaxReconnectInterval(time.Duration(10) * time.Millisecond)
	opts.SetReconnectingHandler(func(client mqtt.Client, options *mqtt.ClientOptions) {
		LastErr = errors.New("try reconnect")
		// 重连设置在2~16秒之间随机
		rand.Seed(time.Now().Unix())
		randNum2 := rand.Intn(14) + 2
		time.Sleep(time.Duration(randNum2) * time.Second)
	})
	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		LastErr = errors.New("lost conn")
		atomic.AddInt64(&ConnSize, -1)
		if Sub > 0 {
			atomic.AddInt64(&Sub, -1)
		}
	})
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		// 连接成功将数据放回chan,便于下一个连接使用
		ClientRateList <- 1
		atomic.AddInt64(&ConnSize, 1)
		go Subscribe(client, req, int(index))
		go Publish(client, &req)
	})
	c := mqtt.NewClient(opts)
	token := c.Connect()
	token.Wait()
	if token.Error() != nil {
		fmt.Println(token.Error())
		LastErr = token.Error()
		// 继续调用尝试连接
		time.Sleep(1 * time.Second)
		Conn(context.Background(), req, index)
		return
	}
}

func Start(ctx context.Context, req model.Request, index int64) {
	if req.Host == "" {
		panic("参数错误：host")
	}
	Conn(ctx, req, index)
}

func Publish(client mqtt.Client, req *model.Request) {
	time.Sleep(time.Second)
	if req.IsStartPub {
		return
	}
	req.IsStartPub = true
	if req.PubTopic == "" || req.PubRate == 0 {
		return
	}
	// 按变量规则生成topic，只支持一个变量
	oriTopic := strings.ReplaceAll(req.PubTopic, "}", "{")
	topicArr := strings.Split(oriTopic, "{")
	pubNum := 1 // PUB数量
	var arr []string
	isVar := false
	if len(topicArr) == 3 {
		// {node-lenAll-rule-num}={node-32-i-4}  num 为映射的数量
		arr = strings.Split(topicArr[1], "-")
		if len(arr) == 4 {
			num, err := strconv.Atoi(arr[3])
			if err != nil {
				panic(err)
			}
			if num > 1 {
				pubNum = num
			}
			isVar = true
		}
	}

	pubFunc := func(topic string, idx int64, sleepTime int, createMsg string) {
		msg := createMsg
		for true {
			if sleepTime == 0 {
				sleepTime = 1000
			}
			if msg == "" {
				msgNow := time.Now()
				msg = fmt.Sprintf(`{"header":{"messageId":"f612fb49845bee6191ea05e1548aa7a2","namespace":"Appliance.Control.ToggleX","triggerSrc":"CloudAlexa","method":"PUSH","payloadVersion":1,"from":"/appliance/2201208098807451860148e1e986b2fb/publish","uuid":"2201208098807451860148e1e986b2fb","timestamp":1673925167,"timestampMs":749,"sign":"2e4375b4631d573499dd0b0585cee295"},"payload":{"channel":0,"togglex":{"channel":0,"onoff":1,"lmTime":%d}},"mss-test":"%v-%d"}`, msgNow.Unix(), idx, msgNow.UnixNano())
			}
			pubToken := client.Publish(topic, byte(req.Qos), false, msg)
			pubToken.Wait()
			if pubToken.Error() != nil {
				atomic.AddInt64(&PubFailSize, 1)
				return
			}
			atomic.AddInt64(&PubSize, 1)
			start := time.Now()
			_ = start
			time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		}
	}
	createMsg := ""
	if req.MsgSize > 0 {
		createMsg = strings.Repeat("a", req.MsgSize)
	}
	// 向N个topic发布消息
	for i := 0; i < pubNum; i++ {
		pubIndex := atomic.AddInt64(&PubIndex, 1)
		sleepTime := req.PubRate * pubNum
		realTime := sleepTime
		if isVar {
			lenAll, _ := strconv.Atoi(arr[1])
			length := strconv.Itoa(lenAll - len(req.Node))
			nodeInt, _ := strconv.Atoi(arr[0])
			node := fmt.Sprintf("%03d", nodeInt)
			req.PubTopic = fmt.Sprintf(topicArr[0]+node+"%0"+length+"d"+topicArr[2], pubIndex)
		}
		time.Sleep(time.Duration(math.Round(float64(req.PubRate))) * time.Millisecond)
		go pubFunc(req.PubTopic, PubIndex, realTime, createMsg)
	}
}

func Subscribe(client mqtt.Client, req model.Request, index int) {
	time.Sleep(time.Second)
	if req.SubTopic != "" {
		// 按变量规则生成topic，只支持一个变量
		oriTopic := strings.ReplaceAll(req.SubTopic, "}", "{")
		topicArr := strings.Split(oriTopic, "{")
		if len(topicArr) == 3 {
			// {lenAll-rule}={32-i}
			arr := strings.Split(topicArr[1], "-")
			if len(arr) == 2 {
				lenAll, err := strconv.Atoi(arr[0])
				if err != nil {
					panic(err)
				}
				length := strconv.Itoa(lenAll - len(req.Node))
				req.SubTopic = fmt.Sprintf(topicArr[0]+req.Node+"%0"+length+"d"+topicArr[2], index)
			}
		}
		subToken := client.Subscribe(req.SubTopic, byte(req.Qos), func(client mqtt.Client, message mqtt.Message) {
			//fmt.Println("message", message.MessageID())
			atomic.AddInt64(&SubMsgSize, 1)
		})
		subToken.Wait()
		if subToken.Error() != nil {
			fmt.Println(subToken.Error())
			LastErr = subToken.Error()
			atomic.AddInt64(&SubFail, 1)
			return
		}
		atomic.AddInt64(&Sub, 1)
	}
}

func LogConnCount() {
	go func() {
		for true {
			time.Sleep(1 * time.Second)
			ConnCount, _ := ConnSize, ConnFailSize
			PubCount, PubFail := PubSize, PubFailSize
			fmt.Println(fmt.Sprintf("[%s] conn=%d,%d/s; sub=%d,fail=%d; pubMsg=%d,%d/s,fail=%d; subMsg=%d, lastErr=%v",
				time.Now().Format("2006-01-02 15:04:05"),
				ConnCount, ConnCount-ConnSizeOri,
				Sub, SubFail,
				PubCount, PubCount-PubSizeOri, PubFail,
				SubMsgSize,
				LastErr,
			))
			ConnSizeOri = ConnCount
			PubSizeOri = PubCount
			LastErr = nil
		}
	}()
}
