package main

import (
	"fmt"
	"math/big"

	"github.com/statechannels/go-nitro/rpc"
	"github.com/statechannels/go-nitro/types"
)

func addCommas(count uint64) string {
	str := fmt.Sprintf("%d", count)
	for i := len(str) - 3; i > 0; i -= 3 {
		str = str[:i] + "," + str[i:]
	}
	return str
}

// checkPaymentChannelBalance checks a payment channel balance and returns true if the AmountPaid is greater than the expected amount
func checkPaymentChannelBalance(rpcClient *rpc.RpcClient, paymentChannelId types.Destination, expectedAmount *big.Int) bool {
	if rpcClient == nil {
		panic("the rpcClient is nil")
	}
	payCh := rpcClient.GetVirtualChannel(paymentChannelId)
	return payCh.Balance.PaidSoFar.ToInt().Cmp(expectedAmount) > 0

}
