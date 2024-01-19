package opentsdb

import (
	"crypto/tls"
	"net/http"
	"time"
)

// DefaultClient is the default http client for requests.
var DefaultClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
	Timeout: 30 * time.Second,
}

var userAgent = ""

func UserAgentSet(ua string) { userAgent = ua }
func SetUserAgent(ua string) { userAgent = ua }
func GetUserAgent() string   { return ua }
