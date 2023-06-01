package tool

import (
	"context"
	"pulsar-demo/model"
	"pulsar-demo/tool/toolemqx"
	"time"
)

func Init(req model.Request) {
	ctx := context.Background()
	go toolemqx.LogConnCount()
	idx := 0
	for true {
		for i := 0; i < req.ClientRate; i++ {
			idx++
			if idx > req.ClientCount {
				return
			}
			go toolemqx.Start(ctx, req, int64(idx))
		}
		time.Sleep(time.Second)
	}
}
