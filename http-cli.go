package opentsdb

import (
	"crypto/tls"
	"net/http"
	"time"
)

// DefaultClient is the default http client for requests.
var DefaultClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	},
	Timeout: time.Minute,
}

var userAgent = ""

func UserAgentSet(ua string) {
	userAgent = ua
}
