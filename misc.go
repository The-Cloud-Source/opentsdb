package opentsdb

import (
	"encoding/json"
	"os"
	"sort"
)

func stableKey(r *Response) string {
	key := r.Metric
	tags := []string{}
	for _, k := range r.AggregateTags {
		tags = append(tags, k)
	}
	for k, v := range r.Tags {
		tags = append(tags, k+"="+v)
	}
	sort.Strings(tags)

	for _, k := range tags {
		key += " " + k
	}

	return key
}

func dump(v interface{}, name string) error {

	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	err := enc.Encode(v)
	return err
}
