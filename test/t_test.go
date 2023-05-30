package test

import (
	"fmt"
	"math/big"
	"testing"
	"time"
)

func Test_a(t *testing.T) {
	val := big.NewInt(0xfffffff)

	fmt.Printf("%s的十进制为%s \n", val.String(), val.String())
}

var Count int
var CountOri int
var sTime time.Duration

func Test_b(t *testing.T) {
	go func() {
		for true {
			Now := Count
			fmt.Printf("Count=%d,%d, sTime=%v\n", Now, Now-CountOri, sTime)
			CountOri = Now
			time.Sleep(time.Second)
		}
	}()
	for true {
		Count++
		start := time.Now()
		time.Sleep(1 * time.Microsecond)
		sTime = time.Now().Sub(start)
	}
}
