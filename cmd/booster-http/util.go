package main

import (
	"fmt"
	"math/big"
	"os"

	"github.com/rs/zerolog"
	"github.com/statechannels/go-nitro/rpc"
	"github.com/statechannels/go-nitro/rpc/transport/ws"
	"github.com/statechannels/go-nitro/types"
)

func addCommas(count uint64) string {
	str := fmt.Sprintf("%d", count)
	for i := len(str) - 3; i > 0; i -= 3 {
		str = str[:i] + "," + str[i:]
	}
	return str
}

// CreateNitroClient creates a Nitro RPC client for the given RPC server URL and Nitro address
// It uses the WS/HTTP transport
func CreateNitroClient(
	rpcServerUrl string, myNitroAddress types.Address) (*rpc.RpcClient, error) {

	conn, err := ws.NewWebSocketTransportAsClient(rpcServerUrl)
	if err != nil {
		return nil, err
	}

	// TODO: Log this somewhere
	logger := zerolog.New(os.Stdout).
		Level(zerolog.TraceLevel).
		With().
		Timestamp().
		Str("client", myNitroAddress.String()).
		Str("scope", "").
		Logger()

	rpcClient, err := rpc.NewRpcClient(rpcServerUrl, myNitroAddress, logger, conn)
	if err != nil {
		return nil, err
	}

	return rpcClient, nil

}

// checkPaymentChannelBalance checks a payment channel balance and returns true if the AmountPaid is greater than the expected amount
func checkPaymentChannelBalance(rpcClient *rpc.RpcClient, paymentChannelId types.Destination, expectedAmount *big.Int) bool {
	if rpcClient == nil {
		panic("the rpcClient is nil")
	}
	payCh := rpcClient.GetVirtualChannel(paymentChannelId)
	return payCh.Balance.PaidSoFar.ToInt().Cmp(expectedAmount) > 0

}
