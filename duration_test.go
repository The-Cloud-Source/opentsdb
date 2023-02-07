package opentsdb

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestDownsample(t *testing.T) {

	ds, err := ParseDownsample("500ms-avg")
	if err != nil {
		t.Errorf("parse failed - %v", err)
	}
	t.Logf("%f %d", ds.Seconds(), ds.SecondsInt64())
}

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
