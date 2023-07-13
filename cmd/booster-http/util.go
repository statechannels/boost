package main

import (
	"fmt"
	"net/http"
)

func addCommas(count uint64) string {
	str := fmt.Sprintf("%d", count)
	for i := len(str) - 3; i > 0; i -= 3 {
		str = str[:i] + "," + str[i:]
	}
	return str
}

type corsHandler struct {
	sub http.Handler
}

func (h *corsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, DELETE, PUT")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	if r.Method == "OPTIONS" {
		_, _ = w.Write([]byte("OK"))
		return
	}

	h.sub.ServeHTTP(w, r)
}
