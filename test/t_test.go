package test

import (
	"fmt"
	"math/big"
	"testing"
)

func Test_a(t *testing.T) {
	val := big.NewInt(0xffffffffffffffffffffffffffffffff)

	fmt.Printf("%s的十进制为%s \n", val.String(), val.String())
}
