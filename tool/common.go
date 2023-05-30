package tool

import (
	"context"
	"pulsar-demo/model"
	"pulsar-demo/tool/toolemqx"
	"time"
)

func Init(req model.Request) {
	ctx := context.Background()
	//toolemqx.InitSubPoolNum(req.ClientCount)
	//toolemqx.InitConnLimiter(req.ClientRate)
	toolemqx.InitPubLimiter(3)
	go toolemqx.LogConnCount()
	//for i := 0; i < req.ClientCount; i++ {
	//	_ = toolemqx.ConnLimiter.Wait(ctx)
	//	if toolemqx.ConnSize >= int64(req.ClientCount) {
	//		toolemqx.ConnLimiter.SetBurst(0)
	//		toolemqx.ConnLimiter.SetLimit(0)
	//		return
	//	}
	//	go toolemqx.Start(ctx, req, int64(i+1))
	//}
	idx := 0
	for true {
		for i := 0; i < req.ClientRate; i++ {
			idx++
			if idx > req.ClientCount {
				return
			}
			if toolemqx.ConnSize >= int64(req.ClientCount) {
				return
			}
			go toolemqx.Start(ctx, req, int64(idx))
		}
		time.Sleep(time.Second)
	}
}
