package toolemqx

import (
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/time/rate"
	"net"
	"pulsar-demo/model"
	"pulsar-demo/tool/queue"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ConnSize int64
var ConnSizeOri int64
var ConnFailSize int64
var ConnLimiter *rate.Limiter
var PubLimiter *rate.Limiter
var PubSize int64
var PubSizeOri int64
var PubFailSize int64
var Lock sync.Mutex
var SubMsgSize int64
var Sub int64
var SubFail int64
var Pub int64
var PubIndex int64
var LastErr error
var SubPoolNum *queue.StackSlice

func init() {
}

// 初始化订阅池
func InitSubPoolNum(size int) {
	SubPoolNum = queue.NewStackSlice(size)
	for i := 1; i <= size; i++ {
		SubPoolNum.Push(i)
	}
}

func Start(ctx context.Context, req model.Request, index int64) {
	if req.Host == "" {
		panic("参数错误：host")
	}

	idx := fmt.Sprintf("node-%010d", index)
	host := fmt.Sprintf("%s:%d", req.Host, req.Port)
	if req.UseSsl {
		host = "ssl://" + host
	}
	opts := mqtt.NewClientOptions().AddBroker(host)
	opts.SetClientID("tool-" + idx)
	opts.SetUsername("emqx" + idx)
	opts.SetPassword("public" + idx)
	opts.SetDefaultPublishHandler(func(c mqtt.Client, message mqtt.Message) {
		fmt.Println("PublishHandler", message)
	})
	opts.SetDialer(&net.Dialer{
		LocalAddr: &net.TCPAddr{
			IP: net.ParseIP(req.LocalAddress),
		},
	})
	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()
	if token.Error() != nil {
		atomic.AddInt64(&ConnFailSize, 1)
		LastErr = token.Error()
		return
	}

	// 通过链接数确定PUB速率
	nowConnSize := atomic.AddInt64(&ConnSize, 1)
	_ = nowConnSize
	if nowConnSize == int64(req.ClientCount) {
		InitPubLimiter(req.PubRate)
	} else if nowConnSize%50 == 0 {
		prePubRate := float64(req.PubRate) / float64(req.ClientCount)
		PubRate := float64(nowConnSize) * prePubRate
		InitPubLimiter(int(PubRate))
	}

	// 订阅
	go func() {
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
			if index == 1 {
				fmt.Println("OneOfSubTopic=" + req.SubTopic)
			}
			subToken := client.Subscribe(req.SubTopic, byte(req.Qos), func(client mqtt.Client, message mqtt.Message) {
				//fmt.Println("message", message)
				atomic.AddInt64(&SubMsgSize, 1)
			})
			subToken.Wait()
			if subToken.Error() != nil {
				LastErr = subToken.Error()
				atomic.AddInt64(&SubFail, 1)
				return
			}
			atomic.AddInt64(&Sub, 1)
		}
	}()

	// 发布
	go func() {
		if req.PubTopic == "" || req.PubRate == 0 {
			return
		}
		// 按变量规则生成topic，只支持一个变量
		oriTopic := strings.ReplaceAll(req.PubTopic, "}", "{")
		topicArr := strings.Split(oriTopic, "{")
		pubNum := 1 // PUB数量
		var arr []string
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
			}
		}

		pubFunc := func(topic string, idx int64) {
			if idx == 1 {
				fmt.Println("OneOfPubTopic=" + topic)
			}
			for true {
				//ctx2, _ := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(math.Round(200)+100))
				//err := PubLimiter.Wait(ctx2)
				//if err != nil {
				//	return
				//}
				_ = PubLimiter.Wait(context.Background())
				msgNow := time.Now()
				msg := fmt.Sprintf(`{"header":{"messageId":"f612fb49845bee6191ea05e1548aa7a2","namespace":"Appliance.Control.ToggleX","triggerSrc":"CloudAlexa","method":"PUSH","payloadVersion":1,"from":"/appliance/2201208098807451860148e1e986b2fb/publish","uuid":"2201208098807451860148e1e986b2fb","timestamp":1673925167,"timestampMs":749,"sign":"2e4375b4631d573499dd0b0585cee295"},"payload":{"channel":0,"togglex":{"channel":0,"onoff":1,"lmTime":%d}},"mss-test":"%s-%d"}`, msgNow.Unix(), idx, msgNow.UnixNano())
				pubToken := client.Publish(topic, byte(req.Qos), false, msg)
				pubToken.Wait()
				if pubToken.Error() != nil {
					atomic.AddInt64(&PubFailSize, 1)
					return
				}
				atomic.AddInt64(&PubSize, 1)
			}
		}
		// 向N个topic发布消息
		for i := 0; i < pubNum; i++ {
			pubIndex := atomic.AddInt64(&PubIndex, 1)
			lenAll, _ := strconv.Atoi(arr[1])
			length := strconv.Itoa(lenAll - len(req.Node))
			req.PubTopic = fmt.Sprintf(topicArr[0]+arr[0]+"%0"+length+"d"+topicArr[2], pubIndex)
			go pubFunc(req.PubTopic, PubIndex)
		}
		//log.Warn("1111111-", PubLimiter.Tokens())
	}()
}

func InitConnLimiter(size int) {
	if size == 0 {
		size = 1
	}
	limiter := rate.NewLimiter(rate.Limit(size), size)
	clearRate(limiter, size)
	ConnLimiter = limiter
}

func InitPubLimiter(size int) {
	Lock.Lock()
	defer Lock.Unlock()
	if size == 0 {
		size = 1
	}
	limiter := rate.NewLimiter(rate.Limit(size), size)
	clearRate(limiter, size)
	PubLimiter = limiter
}

func clearRate(limiter *rate.Limiter, num int) {
	//先消耗掉桶内令牌
	ctx := context.Background()
	i := 0
	for true {
		if i > num {
			break
		}
		i++
		_ = limiter.Wait(ctx)
	}
}

func LogConnCount() {
	go func() {
		for true {
			time.Sleep(1 * time.Second)
			Conn, ConnFail := ConnSize, ConnFailSize
			Pub, PubFail := PubSize, PubFailSize
			fmt.Println(fmt.Sprintf("[%s] conn=%d,%d/s,fail=%d; sub=%d,fail=%d; pubMsg=%d,%d/s,fail=%d; subMsg=%d, lastErr=%v",
				time.Now().Format("2006-01-02 15:04:05"),
				Conn, Conn-ConnSizeOri, ConnFail,
				Sub, SubFail,
				Pub, Pub-PubSizeOri, PubFail,
				SubMsgSize,
				LastErr,
			))
			ConnSizeOri = Conn
			PubSizeOri = Pub
			LastErr = nil
		}
	}()
}
