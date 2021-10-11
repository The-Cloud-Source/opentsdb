package opentsdb

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestJSON(t *testing.T) {

	var q = []byte(
		`{"start":1633860709920,"queries":[{"metric":"system.cpu.percent","aggregator":"sum","downsample":"15s-avg","tags":{"host":"*"}}],"msResolution":false,"globalAnnotations":true,"showQuery":true}`)

	r := &Request{}
	dec := json.NewDecoder(bytes.NewReader(q))
	err := dec.Decode(&r)

	t.Errorf("||| %s", r.String())
	t.Errorf("||| %s", r.Encode())
	t.Errorf("||| %v", err)

}

var in1 = []byte(`
{
  "start": 1629380400000,
  "end":   1629385200000
}
`)

func TestM(t *testing.T) {

	r := &Request{}
	dec := json.NewDecoder(bytes.NewReader(in1))
	err := dec.Decode(&r)

	t.Errorf("%s %s", r.Start, r.End)
	t.Errorf("%v", err)

	r2 := &Request{}
	r2.Start = "1629380400000"
	r2.End = "1629385200000"
	t.Errorf("%v %v", r2.Start, r2.End)

	b, err := json.Marshal(r2)
	t.Errorf("%v %v", string(b), err)

	resp := &Response{
		Metric: "metric.metric",
		Tags:   TagSet{},
		DPS: map[Epoch]Point{
			123: 1.1,
		},
	}
	b2, err := json.Marshal(resp)
	t.Errorf("%v %v", string(b2), err)

	//in := []byte(`{"metric":"","tags":null,"aggregateTags":null,"query":{"metric":"","aggregator":"","rateOptions":{}},"dps":{"123":1.1}}`)
	res := &Request{}
	dec1 := json.NewDecoder(bytes.NewReader(b2))
	err1 := dec1.Decode(&res)

	t.Errorf("%v", *res)
	t.Errorf("%v", err1)

}
