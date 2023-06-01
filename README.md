# 说明

用于 mqtt 的压力测试工具

时间：2023-05-27

版本：0.0.1

# 使用说明

./tool_xxx -[key] [val]  或  go run main.go -[key] [val]

可直接源码执行或者编译成可执行文件后执行压测

# 更新日志

- [20230531] 支持重连



# 参数说明

```
  -addr string                                      
        localAddress   指定源IP                     
  -cc int                                           
        clientNum      客户端数量 (default 1)       
  -cr int                                           
        clientRate     客户端创建速度/秒 (default 1)
  -h string                                         
        host           主机地址                     
  -n int                                            
        node           节点编号，不指定则随机生成，可能出现碰撞
  -p int
        port           端口号 (default 1883)
  -pms int
        pubMsgSize     发布消息的字符数，不填则使用默认消息，如填2则字符串为aa
  -pr int
        pubRate        发布消息速率，间隔n毫秒 (default 1000)
  -pt string
        pubTopic       发布的主题，空则不发布，支持变量，如：/app/{node-len-i-num}/pub
                       其中i表示index递增，node为节点id，num表示向几个topic发消息
  -qos int
        qos            Qos等级 (default 1)
  -ssl
        openSsl        是否启用ssl
  -st string
        subTopic       订阅的主题，空则不订阅，支持变量，如：/app/{len-i}/sub
  -v int
        mqttVersion    MQTT版本 (default 3)
```

# 使用DEMO

模拟设备，每个链接订阅一个topic

go run main.go -cr 20 -cc 500 -h host.docker.internal -p 1883 -addr 172.18.0.3 -st /appliance/{32-i}/subscribe -n 123

模拟APP，每个链接向5个设备发布消息

go run main.go -cr 10 -cc 100 -h host.docker.internal -p 1883 -addr 172.18.0.3 -pt /appliance/{123-32-i-5}/subscribe -pr 500

