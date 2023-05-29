package tool

import (
	"context"
	"pulsar-demo/model"
	"pulsar-demo/tool/toolemqx"
)

func Init(req model.Request) {
	ctx := context.Background()
	//toolemqx.InitSubPoolNum(req.ClientCount)
	toolemqx.InitConnLimiter(req.ClientRate)
	toolemqx.InitPubLimiter(3)
	go toolemqx.LogConnCount()
	for i := 0; i < req.ClientCount*2; i++ {
		_ = toolemqx.ConnLimiter.Wait(ctx)
		if toolemqx.ConnSize >= int64(req.ClientCount) {
			toolemqx.ConnLimiter.SetBurst(0)
			toolemqx.ConnLimiter.SetLimit(0)
			return
		}
		go toolemqx.Start(ctx, req, int64(i+1))
	}
}
