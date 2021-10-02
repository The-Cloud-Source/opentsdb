package opentsdb

import (
	"bytes"
	"encoding/json"
	"testing"
)

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

	t.Errorf("%v %v", r.Start, r.End)
	t.Errorf("%v", err)

	r2 := &Request{}
	r2.Start = "1629380400000"
	r2.End = "1629385200000"
	t.Errorf("%v %v", r2.Start, r2.End)

	b, err := json.Marshal(r2)
	t.Errorf("%v %v", string(b), err)
}
