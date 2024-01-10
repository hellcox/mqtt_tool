package test

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"net"
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

func Test_c(t *testing.T) {
	//ipv6 := net.ParseIP("::ffff:7f00:1")
	ipv6 := net.ParseIP("2001:4860:4860::8888")
	ipv4 := ipv6.To4()
	fmt.Printf("IPv6: %v\nIPv4: %v\n", ipv6, ipv4)
}

func Test_d(t *testing.T) {
	big.NewInt(42540766411282592856903984951653826562)
	num := uint64(42540766411282592856903984951653826561)
	ipv6 := make(net.IP, 16)

	// 将长整型数转换为IPv6地址
	binary.BigEndian.PutUint64(ipv6[8:], num)

	// 将IPv6地址转换为字符串
	str := ipv6.String()

	fmt.Printf("Long Int: %d\nIPv6: %s\n", num, str)
}
