# 说明

用于 mqtt 与 pulsar 的压力测试工具，发布消息到 emqx，消费 pulsar 中的消息

时间：2023-05-22

作者：hellcox

版本：0.0.1

# 使用示例

mss_emqx_tool -[key] [val]

# 参数说明

```
-a string
      action:         执行动作：produce/consume
-b int
      burst:          令牌桶容量 (default 10000)
-e string
      ext:            额外参数
                      action=produce,ext=special
-h string
      host:           主机地址
-l int
      burst:          输出日志
-lc int
      burst:          输出统计日志
-n int
      num:            数量
-nc int
      num client:     客户端数量 (default 1)
-ng int
      num go:         协程数量 (default 100)
-r int
      rate:           每秒生成消息量 (default 1)
-t string
      topic:          主题
```

# 使用DEMO

生产
go run main.go -a produce -h 127.0.0.1:1883 -nc 20 -n 0 -r 100 -b 10000 -e special -l 1 -t fw/pub/cloud/nack

消费
go run main.go -a consume -h pulsar://pulsar-cluster-dev.meross.dev.vpc:6650 -e special -t persistent:
//meross/iot_raw/q_emqx_online

## 常用CLI

```
一直生产消息，直到接收到退出信号
go run main.go -a produce -h 127.0.0.1:1883 -nc 20 -n 0 -r 100 -b 10000 -e special -l 1 -t fw/pub/cloud/nack

生产固定条数消息
go run main.go -a produce -h 127.0.0.1:1883 -nc 20 -n 9999 -r 100 -b 10000 -e special -l 1 -t fw/pub/cloud/nack

生产特殊消息，主要用于测试消息时延
go run .\main.go -a produce -h 127.0.0.1:1883 -nc 1 -n 0 -r 1 -b 2 -e special -l 1 -t fw/pub/cloud/nack

消费并输出该消息
go run main.go -a consume -h pulsar://pulsar-cluster-dev.meross.dev.vpc:6650 -t persistent://meross/iot_raw/q_emqx_stat_msg -l 1

消费并输出消费数量
go run main.go -a consume -h pulsar://pulsar-cluster-dev.meross.dev.vpc:6650 -t persistent://meross/iot_raw/q_emqx_stat_msg -lc 1

消费特殊消息并统计平均时延（接受到退出信号输出统计信息）
go run main.go -a consume -h pulsar://pulsar-cluster-dev.meross.dev.vpc:6650 -e special -t persistent://meross/iot_raw/q_emqx_stat_msg -lc 1

```