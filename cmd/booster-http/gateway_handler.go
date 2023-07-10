package main

import (
	"fmt"
	"math/big"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ipfs/boxo/gateway"
	"github.com/statechannels/go-nitro/rpc"
	"github.com/statechannels/go-nitro/types"

	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/statechannels/go-nitro/crypto"
	"github.com/statechannels/go-nitro/payments"
)

type gatewayHandler struct {
	gwh              http.Handler
	supportedFormats map[string]struct{}
	nitroRpcClient   *rpc.RpcClient
}

func newGatewayHandler(gw *gateway.BlocksBackend, supportedFormats []string, nitroRpcClient *rpc.RpcClient) http.Handler {
	headers := map[string][]string{}
	gateway.AddAccessControlHeaders(headers)

	fmtsMap := make(map[string]struct{}, len(supportedFormats))
	for _, f := range supportedFormats {
		fmtsMap[f] = struct{}{}
	}

	// TODO: For the integration demo, we need to allow CORS requests to the gateway.
	return &gatewayHandler{
		gwh:              &corsHandler{gateway.NewHandler(gateway.Config{Headers: headers, DeserializedResponses: true}, gw)},
		supportedFormats: fmtsMap,
		nitroRpcClient:   nitroRpcClient,
	}
}

func (h *gatewayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	responseFormat, _, err := customResponseFormat(r)
	if err != nil {
		webError(w, fmt.Errorf("error while processing the Accept header: %w", err), http.StatusBadRequest)
		return
	}

	if _, ok := h.supportedFormats[responseFormat]; !ok {
		if responseFormat == "" {
			responseFormat = "unixfs"
		}
		webError(w, fmt.Errorf("unsupported response format: %s", responseFormat), http.StatusBadRequest)
		return
	}

	if h.nitroRpcClient != nil {
		// This the payment we expect to receive for the file.
		const expectedPayment = int64(5)

		params, _ := url.ParseQuery(r.URL.RawQuery)

		v, err := parseVoucher(params)
		if err != nil {
			webError(w, fmt.Errorf("could not parse voucher: %w", err), http.StatusBadRequest)
			return
		}

		// delta is the change in the channel balance caused by adding this voucher.
		_, delta := h.nitroRpcClient.ReceiveVoucher(v)

		// TODO: A nil value indicates an error with the voucher. We should update to the latest go-nitro which properly returns the error.
		if delta == nil {
			webError(w, fmt.Errorf("invalid voucher received %+v", v), http.StatusBadRequest)
			return
		}

		// If the voucher resulted in a payment less than the expected payment, return an error.
		if delta.Cmp(big.NewInt(expectedPayment)) < 0 {
			webError(w, fmt.Errorf("payment of %d required, the voucher only resulted in a payment of %d", expectedPayment, delta.Uint64()), http.StatusPaymentRequired)
			return
		}
	}

	h.gwh.ServeHTTP(w, r)
}

// parseVoucher takes in an a collection of query params and parses out a voucher.
func parseVoucher(params url.Values) (payments.Voucher, error) {
	if !params.Has("channelId") {
		return payments.Voucher{}, fmt.Errorf("a valid channel id must be provided")
	}
	if !params.Has("amount") {
		return payments.Voucher{}, fmt.Errorf("a valid amount must be provided")
	}
	if !params.Has("signature") {
		return payments.Voucher{}, fmt.Errorf("a valid signature must be provided")
	}
	rawChId := params.Get("channelId")
	rawAmt := params.Get("amount")
	amount := big.NewInt(0)
	amount.SetString(rawAmt, 10)
	rawSignature := params.Get("signature")

	v := payments.Voucher{
		ChannelId: types.Destination(common.HexToHash(rawChId)),
		Amount:    amount,
		Signature: crypto.SplitSignature(hexutil.MustDecode(rawSignature)),
	}
	return v, nil
}

func webError(w http.ResponseWriter, err error, code int) {
	// TODO: This is a hack to allow CORS requests to the gateway for the boost integration demo.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	fmt.Printf("ERROR CODE %d\n", code)
	http.Error(w, err.Error(), code)
}

// Unfortunately this function is not exported from go-libipfs so we need to copy it here.
// return explicit response format if specified in request as query parameter or via Accept HTTP header
func customResponseFormat(r *http.Request) (mediaType string, params map[string]string, err error) {
	if formatParam := r.URL.Query().Get("format"); formatParam != "" {
		// translate query param to a content type
		switch formatParam {
		case "raw":
			return "application/vnd.ipld.raw", nil, nil
		case "car":
			return "application/vnd.ipld.car", nil, nil
		case "tar":
			return "application/x-tar", nil, nil
		case "json":
			return "application/json", nil, nil
		case "cbor":
			return "application/cbor", nil, nil
		case "dag-json":
			return "application/vnd.ipld.dag-json", nil, nil
		case "dag-cbor":
			return "application/vnd.ipld.dag-cbor", nil, nil
		case "ipns-record":
			return "application/vnd.ipfs.ipns-record", nil, nil
		}
	}
	// Browsers and other user agents will send Accept header with generic types like:
	// Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8
	// We only care about explicit, vendor-specific content-types and respond to the first match (in order).
	// TODO: make this RFC compliant and respect weights (eg. return CAR for Accept:application/vnd.ipld.dag-json;q=0.1,application/vnd.ipld.car;q=0.2)
	for _, header := range r.Header.Values("Accept") {
		for _, value := range strings.Split(header, ",") {
			accept := strings.TrimSpace(value)
			// respond to the very first matching content type
			if strings.HasPrefix(accept, "application/vnd.ipld") ||
				strings.HasPrefix(accept, "application/x-tar") ||
				strings.HasPrefix(accept, "application/json") ||
				strings.HasPrefix(accept, "application/cbor") ||
				strings.HasPrefix(accept, "application/vnd.ipfs") {
				mediatype, params, err := mime.ParseMediaType(accept)
				if err != nil {
					return "", nil, err
				}
				return mediatype, params, nil
			}
		}
	}
	// If none of special-cased content types is found, return empty string
	// to indicate default, implicit UnixFS response should be prepared
	return "", nil, nil
}
