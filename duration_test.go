package opentsdb

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestDuration(t *testing.T) {

	var q = []byte(
		`{"start":1633860709920,
		  "end":  1660000000000,
			"queries":[{"metric":"system.cpu.percent","aggregator":"sum","downsample":"15s-avg","tags":{"host":"*"}}],"msResolution":false,"globalAnnotations":true,"showQuery":true}`)

	r := &Request{}
	dec := json.NewDecoder(bytes.NewReader(q))
	err := dec.Decode(&r)
	if err != nil {
		t.Errorf("decode failed - %v", err)
	}

	reqSpan, err := r.GetDuration()
	if reqSpan.SpanString() != "7260h54m51s" || err != nil {
		t.Errorf("span failed - %v", err)
		t.Errorf("want %s have %s", "7260h54m51s", reqSpan.SpanString())
	}
}
