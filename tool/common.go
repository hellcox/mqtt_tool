package tool

import (
	"context"
	"pulsar-demo/model"
	"pulsar-demo/tool/toolemqx"
	"time"
)

func Init(req model.Request) {
	ctx := context.Background()
	toolemqx.Init(req)
	go toolemqx.LogConnCount()
	idx := 0
	for true {
		for i := 0; i < req.ClientRate; i++ {
			// chan消耗完毕则阻塞
			<-toolemqx.ClientRateList
			idx++
			if idx > req.ClientCount {
				// 调用消耗chan逻辑，避免重连成功后写入chan时导致阻塞
				go outChan()
				return
			}
			go toolemqx.Start(ctx, req, int64(idx))
		}
		time.Sleep(time.Second)
	}
}

func outChan() {
	for true {
		<-toolemqx.ClientRateList
	}
}
