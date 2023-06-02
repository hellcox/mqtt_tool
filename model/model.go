package model

type Request struct {
	Action           string // 操作
	Host             string // HOST
	Topic            string
	LocalAddress     string // 指定网卡IP
	ClientCount      int    // 客户端总数量
	ClientRate       int    // 创建客户端速率
	PubRate          int    // 发布消息速率
	PubMsgCount      int    // 发布消息总数量，达到后停止发布
	UseSsl           bool   // 是否开启SSL
	Qos              int    // Qos 等级
	Version          int    // mqtt 版本，默认3
	Port             int    // 端口号
	PubTopic         string
	SubTopic         string
	Node             string
	IsStartPub       bool // 是否已经开始PUB
	IsStartConnCheck bool // 是否开启连接监测
	MsgSize          int  // 自动生成消息的长度
}

type NormalMsg struct {
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

type OnlineMsg struct {
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
