// Package opentsdb defines structures for interacting with an OpenTSDB server.
package opentsdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ResponseSet is a Multi-Set Response:
// http://opentsdb.net/docs/build/html/api_http/query/index.html#example-multi-set-response.
type ResponseSet []*Response

func (r ResponseSet) Copy() ResponseSet {
	newSet := make(ResponseSet, len(r))
	for i, resp := range r {
		newSet[i] = resp.Copy()
	}
	return newSet
}

// Epoch is the timestamp for a datapoint
type Epoch int64

// Point is the Response data point type.
type Point float64

type DPmap map[Epoch]Point

// Response is a query response:
// http://opentsdb.net/docs/build/html/api_http/query/index.html#response.
type Response struct {
	Metric        string            `json:"metric" yaml:"metric"`
	Tags          TagSet            `json:"tags" yaml:"tags"`
	AggregateTags []string          `json:"aggregateTags" yaml:"aggregateTags"`
	Query         Query             `json:"query,omitempty" yaml:"query,omitempty"`
	DPS           DPmap             `json:"dps" yaml:"dps"`
	Stats         *QueryStats       `json:"stats,omitempty" yaml:"stats,omitempty"`
	StatsSummary  QueryStatsSummary `json:"statsSummary,omitempty" yaml:"statsSummary,omitempty"`
	//missing "annotations": [...]
	//missing "annotations": [...]
	//missing "tsuids": [...]

	// fields added by translating proxy
	// SQL string `json:"sql,omitempty"`
}

// StatsSummary is that lastelemt of the json array response when it exists
type QueryStatsSummary map[string]any

// QueryStats are optional stats returned with the response
type QueryStats struct {
	Index                int     `json:"queryIndex" yaml:"queryIndex"`
	EmittedDPS           int     `json:"emittedDPs" yaml:"emittedDPs"`
	AggregationTime      float64 `json:"aggregationTime" yaml:"aggregationTime"`
	GroupByTime          float64 `json:"groupByTime" yaml:"groupByTime"`
	QueryScanTime        float64 `json:"queryScanTime" yaml:"queryScanTime"`
	SaltScannerMergeTime float64 `json:"saltScannerMergeTime" yaml:"saltScannerMergeTime"`
	SerializationTime    float64 `json:"serializationTime" yaml:"serializationTime"`
	UidToStringTime      float64 `json:"uidToStringTime" yaml:"uidToStringTime"`
}

func (r *Response) Copy() *Response {
	newR := Response{}
	newR.Metric = r.Metric
	newR.Tags = r.Tags.Copy()
	copy(newR.AggregateTags, r.AggregateTags)
	newR.DPS = DPmap{}
	for k, v := range r.DPS {
		newR.DPS[k] = v
	}
	return &newR
}

// DataPoint is a data point for the /api/put route:
// http://opentsdb.net/docs/build/html/api_http/put.html#example-single-data-point-put.
type DataPoint struct {
	Metric    string      `json:"metric" yaml:"metric"`
	Timestamp Epoch       `json:"timestamp" yaml:"timestamp"`
	Value     interface{} `json:"value" yaml:"value"`
	Tags      TagSet      `json:"tags" yaml:"tags"`
}

// MarshalJSON verifies d is valid and converts it to JSON.
func (d *DataPoint) MarshalJSON() ([]byte, error) {
	if err := d.Clean(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Metric    string      `json:"metric" yaml:"metric"`
		Timestamp Epoch       `json:"timestamp" yaml:"timestamp"`
		Value     interface{} `json:"value" yaml:"value"`
		Tags      TagSet      `json:"tags" yaml:"tags"`
	}{
		d.Metric,
		d.Timestamp,
		d.Value,
		d.Tags,
	})
}

// Valid returns whether d contains valid data (populated fields, valid tags)
// for submission to OpenTSDB.
func (d *DataPoint) Valid() bool {
	if d.Metric == "" || !ValidTSDBString(d.Metric) || d.Timestamp == 0 || d.Value == nil || !d.Tags.Valid() {
		return false
	}
	f, err := strconv.ParseFloat(fmt.Sprint(d.Value), 64)
	if err != nil || math.IsNaN(f) {
		return false
	}
	return true
}

// MultiDataPoint holds multiple DataPoints:
// http://opentsdb.net/docs/build/html/api_http/put.html#example-multiple-data-point-put.
type MultiDataPoint []*DataPoint

// TagSet is a helper class for tags.
type TagSet map[string]string

// Copy creates a new TagSet from t.
func (t TagSet) Copy() TagSet {
	n := make(TagSet)
	for k, v := range t {
		n[k] = v
	}
	return n
}

// Merge adds or overwrites everything from o into t and returns t.
func (t TagSet) Merge(o TagSet) TagSet {
	for k, v := range o {
		t[k] = v
	}
	return t
}

// Equal returns true if t and o contain only the same k=v pairs.
func (t TagSet) Equal(o TagSet) bool {
	if len(t) != len(o) {
		return false
	}
	for k, v := range t {
		if ov, ok := o[k]; !ok || ov != v {
			return false
		}
	}
	return true
}

// Subset returns true if all k=v pairs in o are in t.
func (t TagSet) Subset(o TagSet) bool {
	if len(o) > len(t) {
		return false
	}
	for k, v := range o {
		if tv, ok := t[k]; !ok || tv != v {
			return false
		}
	}
	return true
}

// Compatible returns true if all keys that are in both o and t, have the same value.
func (t TagSet) Compatible(o TagSet) bool {
	for k, v := range o {
		if tv, ok := t[k]; ok && tv != v {
			return false
		}
	}
	return true
}

// Intersection returns the intersection of t and o.
func (t TagSet) Intersection(o TagSet) TagSet {
	r := make(TagSet)
	for k, v := range t {
		if o[k] == v {
			r[k] = v
		}
	}
	return r
}

// String converts t to an OpenTSDB-style {a=b,c=b} string, alphabetized by key.
func (t TagSet) String() string {
	return "{" + t.Tags() + "}"
}

// Tags is identical to String() but without { and }.
func (t TagSet) Tags() string {
	var keys []string
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b := &bytes.Buffer{}
	for i, k := range keys {
		if i > 0 {
			fmt.Fprint(b, ",")
		}
		fmt.Fprintf(b, "%s=%s", k, t[k])
	}
	return b.String()
}

func (t TagSet) AllSubsets() []string {
	var keys []string
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return t.allSubsets("", 0, keys)
}

func (t TagSet) allSubsets(base string, start int, keys []string) []string {
	subs := []string{}
	for i := start; i < len(keys); i++ {
		part := base
		if part != "" {
			part += ","
		}
		part += fmt.Sprintf("%s=%s", keys[i], t[keys[i]])
		subs = append(subs, part)
		subs = append(subs, t.allSubsets(part, i+1, keys)...)
	}
	return subs
}

// Returns true if the two tagsets "overlap".
// Two tagsets overlap if they:
// 1. Have at least one key/value pair that matches
// 2. Have no keys in common where the values do not match
func (a TagSet) Overlaps(b TagSet) bool {
	anyMatch := false
	for k, v := range a {
		v2, ok := b[k]
		if !ok {
			continue
		}
		if v2 != v {
			return false
		}
		anyMatch = true
	}
	return anyMatch
}

// Valid returns whether t contains OpenTSDB-submittable tags.
func (t TagSet) Valid() bool {
	if len(t) == 0 {
		return true
	}
	_, err := ParseTags(t.Tags())
	return err == nil
}

func (d *DataPoint) Clean() error {
	if err := d.Tags.Clean(); err != nil {
		return fmt.Errorf("cleaning tags for metric %s: %s", d.Metric, err)
	}
	m, err := Clean(d.Metric)
	if err != nil {
		return fmt.Errorf("cleaning metric %s: %s", d.Metric, err)
	}
	if d.Metric != m {
		d.Metric = m
	}
	switch v := d.Value.(type) {
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			d.Value = i
		} else if f, err := strconv.ParseFloat(v, 64); err == nil {
			d.Value = f
		} else {
			return fmt.Errorf("Unparseable number %v", v)
		}
	case uint64:
		if v > math.MaxInt64 {
			d.Value = float64(v)
		}
	case *big.Int:
		if bigMaxInt64.Cmp(v) < 0 {
			if f, err := strconv.ParseFloat(v.String(), 64); err == nil {
				d.Value = f
			}
		}
	}
	// if timestamp bigger than 32 bits, likely in milliseconds
	if d.Timestamp > 0xffffffff {
		d.Timestamp /= 1000
	}
	if !d.Valid() {
		return fmt.Errorf("datapoint is invalid")
	}
	return nil
}

var bigMaxInt64 = big.NewInt(math.MaxInt64)

// Clean removes characters from t that are invalid for OpenTSDB metric and tag
// values. An error is returned if a resulting tag is empty.
func (t TagSet) Clean() error {
	for k, v := range t {
		kc, err := Clean(k)
		if err != nil {
			return fmt.Errorf("cleaning tag %s: %s", k, err)
		}
		vc, err := Clean(v)
		if err != nil {
			return fmt.Errorf("cleaning value %s for tag %s: %s", v, k, err)
		}
		if kc == "" || vc == "" {
			return fmt.Errorf("cleaning value [%s] for tag [%s] result in an empty string", v, k)
		}
		if kc != k || vc != v {
			delete(t, k)
			t[kc] = vc
		}
	}
	return nil
}

// Clean is Replace with an empty replacement string.
func Clean(s string) (string, error) {
	return Replace(s, "")
}

// Replace removes characters from s that are invalid for OpenTSDB metric and
// tag values and replaces them.
// See: http://opentsdb.net/docs/build/html/user_guide/writing.html#metrics-and-tags
func Replace(s, replacement string) (string, error) {

	// constructing a name processor isn't too expensive but we need to refactor this file so that it's possible to
	// inject instances so that we don't have to keep newing up.
	// For the moment I prefer to constructing like this to holding onto a global instance
	val, err := NewOpenTsdbNameProcessor(replacement)
	if err != nil {
		//return "", errors.Wrap(err, "Failed to create name processor")
		return "", fmt.Errorf("Failed to create name processor: %w", err)
	}

	result, err := val.FormatName(s)
	if err != nil {
		//return "", errors.Wrap(err, "Failed to format string")
		return "", fmt.Errorf("Failed to format string: %w", err)
	}

	return result, nil
}

// MustReplace is like Replace, but returns an empty string on error.
func MustReplace(s, replacement string) string {
	r, err := Replace(s, replacement)
	if err != nil {
		return ""
	}
	return r
}

// Request holds query objects:
// http://opentsdb.net/docs/build/html/api_http/query/index.html#requests.
type Request struct {
	Start             interface{} `json:"start" yaml:"start"`
	End               interface{} `json:"end,omitempty" yaml:"end,omitempty"`
	Queries           []*Query    `json:"queries" yaml:"queries"`
	NoAnnotations     bool        `json:"noAnnotations,omitempty" yaml:"noAnnotations,omitempty"`
	GlobalAnnotations bool        `json:"globalAnnotations,omitempty" yaml:"globalAnnotations,omitempty"`
	MsResolution      bool        `json:"msResolution,omitempty" yaml:"msResolution,omitempty"`
	ShowTSUIDs        bool        `json:"showTSUIDs,omitempty" yaml:"showTSUIDs,omitempty"`
	ShowSummary       bool        `json:"showSummary,omitempty" yaml:"showSummary,omitempty"`
	ShowStats         bool        `json:"showStats,omitempty" yaml:"showStats,omitempty"`
	ShowQuery         bool        `json:"showQuery,omitempty" yaml:"showQuery,omitempty"`
	Delete            bool        `json:"delete,omitempty" yaml:"delete,omitempty"`
	UseCalendar       bool        `json:"useCalendar,omitempty" yaml:"useCalendar,omitempty"`
	Timezone          string      `json:"timezone,omitempty" yaml:"timezone,omitempty"`
}

// RequestFromJSON creates a new request from JSON.
func RequestFromJSON(b []byte) (*Request, error) {
	var r Request
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	r.Start = TryParseAbsTime(r.Start)
	r.End = TryParseAbsTime(r.End)
	return &r, nil
}

// Query is a query for a request:
// http://opentsdb.net/docs/build/html/api_http/query/index.html#sub-queries.
type Query struct {
	Metric       string       `json:"metric" yaml:"metric"`
	Aggregator   string       `json:"aggregator" yaml:"aggregator"`
	Rate         bool         `json:"rate" yaml:"rate"`
	RateOptions  *RateOptions `json:"rateOptions" yaml:"rateOptions"`
	Downsample   string       `json:"downsample,omitempty" yaml:"downsample,omitempty"`
	Tags         TagSet       `json:"tags,omitempty" yaml:"tags,omitempty"`
	Filters      Filters      `json:"filters,omitempty" yaml:"filters,omitempty"`
	ExplicitTags bool         `json:"explicitTags" yaml:"explicitTags"`
	TSUIDs       []string     `json:"tsuids" yaml:"tsuids"`
	GroupByTags  TagSet       `json:"-" yaml:"-"`
	Index        int          `json:"index" yaml:"index"`
	//HistogramQuery       bool         `json:"histogramQuery" yaml:"histogramQuery"`
	//PreAggregate         bool         `json:"preAggregate" yaml:"preAggregate"`
	//ShowHistogramBuckets bool         `json:"showHistogramBuckets" yaml:"showHistogramBuckets"`
	//"rollupUsage"
	//percentiles
	//rollupUsage
}

type Filter struct {
	Type    string `json:"type" yaml:"type"`
	TagK    string `json:"tagk" yaml:"tagk"`
	Filter  string `json:"filter" yaml:"filter"`
	GroupBy bool   `json:"groupBy" yaml:"groupBy"`
}

func (f Filter) String() string {
	return fmt.Sprintf("%s=%s(%s)", f.TagK, f.Type, f.Filter)
}

type Filters []Filter

func (filters Filters) String() string {
	s := ""
	gb := make(Filters, 0)
	nGb := make(Filters, 0)
	for _, filter := range filters {
		if filter.GroupBy {
			gb = append(gb, filter)
			continue
		}
		nGb = append(nGb, filter)
	}
	s += "{"
	for i, filter := range gb {
		s += filter.String()
		if i != len(gb)-1 {
			s += ","
		}
	}
	s += "}"
	for i, filter := range nGb {
		if i == 0 {
			s += "{"
		}
		s += filter.String()
		if i == len(nGb)-1 {
			s += "}"
		} else {
			s += ","
		}
	}
	return s
}

// RateOptions are rate options for a query.
type RateOptions struct {
	Counter    bool  `json:"counter,omitempty" yaml:"counter,omitempty"`
	CounterMax int64 `json:"counterMax,omitempty" yaml:"counterMax,omitempty"`
	ResetValue int64 `json:"resetValue,omitempty" yaml:"resetValue,omitempty"`
	DropResets bool  `json:"dropResets,omitempty" yaml:"dropResets,omitempty"`
}

// ParseRequest parses OpenTSDB requests of the form: start=1h-ago&m=avg:cpu.
func ParseRequest(req string, version Version) (*Request, error) {
	v, err := url.ParseQuery(req)
	if err != nil {
		return nil, err
	}
	r := Request{}
	s := v.Get("start")
	if s == "" {
		return nil, fmt.Errorf("opentsdb: missing start: %s", req)
	}
	r.Start = TimeSpec(s)

	if e := v.Get("end"); e != "" {
		r.End = TimeSpec(e)
	}
	for _, m := range v["m"] {
		q, err := ParseQuery(m, version)
		if err != nil {
			return nil, err
		}
		r.Queries = append(r.Queries, q)
	}
	if len(r.Queries) == 0 {
		return nil, fmt.Errorf("opentsdb: missing m: %s", req)
	}
	return &r, nil
}

var qRE2_1 = regexp.MustCompile(`^(?P<aggregator>\w+):(?:(?P<downsample>\w+-\w+):)?(?:(?P<rate>rate.*):)?(?P<metric>[\w./-]+)(?:\{([\w./,=*-|]+)\})?$`)
var qRE2_2a = regexp.MustCompile(
	`` +
		`^(?P<aggregator>\w+):` + // aggregation
		`(?:(?P<downsample>\w+-\w+(?:-(?:\w+))?):)?` + // downsampling agg
		`(?:(?P<rate>rate(?:[{].*[}])?):)?` + // rate options
		`(?P<metric>[\w./-]+)` + //metric name
		`(?:\{([^}]+)?\})?` + // groupping tags
		`(?:\{([^}]+)?\})?$` + // non groupping tags
		``)

var qRE2_2b = regexp.MustCompile(
	`` +
		`^(?P<aggregator>\w+):` + // aggregation
		`(?:(?P<rate>rate(?:[{].*[}])?):)?` + // rate options
		`(?:(?P<downsample>\w+-\w+(?:-(?:\w+))?):)?` + // downsampling agg
		`(?P<metric>[\w./-]+)` + //metric name
		`(?:\{([^}]+)?\})?` + // groupping tags
		`(?:\{([^}]+)?\})?$` + // non groupping tags
		``)

// ParseQuery parses OpenTSDB queries of the form: avg:rate:cpu{k=v}. Validation
// errors will be returned along with a valid Query.
func ParseQuery(query string, version Version) (q *Query, err error) {
	var regExp = qRE2_1
	q = new(Query)
	if version.FilterSupport() {
		regExp = qRE2_2a
	}

	m := regExp.FindStringSubmatch(query)
	if len(m) < 1 && version.FilterSupport() {
		regExp = qRE2_2b
		m = regExp.FindStringSubmatch(query)
	}

	if m == nil || len(m) < 1 {
		return nil, fmt.Errorf("opentsdb: bad query format: %s", query)
	}

	result := make(map[string]string)
	for i, name := range regExp.SubexpNames() {
		if i != 0 && i < len(m) {
			result[name] = m[i]
		}
	}

	q.Aggregator = result["aggregator"]
	q.Downsample = result["downsample"]
	q.Rate = strings.HasPrefix(result["rate"], "rate")
	if q.Rate && len(result["rate"]) > 4 {
		if q.RateOptions == nil {
			q.RateOptions = &RateOptions{}
		}
		s := result["rate"][4:]
		if !strings.HasSuffix(s, "}") || !strings.HasPrefix(s, "{") {
			err = fmt.Errorf("opentsdb: invalid rate options")
			return
		}
		sp := strings.Split(s[1:len(s)-1], ",")
		q.RateOptions.Counter = sp[0] == "counter" || sp[0] == "dropcounter"
		q.RateOptions.DropResets = sp[0] == "dropcounter"
		if len(sp) > 1 {
			if sp[1] != "" {
				if q.RateOptions.CounterMax, err = strconv.ParseInt(sp[1], 10, 64); err != nil {
					return
				}
			}
		}
		if len(sp) > 2 {
			if q.RateOptions.ResetValue, err = strconv.ParseInt(sp[2], 10, 64); err != nil {
				return
			}
		}
	}
	q.Metric = result["metric"]

	if !version.FilterSupport() && len(m) > 5 && m[5] != "" {
		tags, e := ParseTags(m[5])
		if e != nil {
			err = e
			if tags == nil {
				return
			}
		}
		q.Tags = tags
	}

	if !version.FilterSupport() {
		return
	}

	// OpenTSDB Greater than 2.2, treating as filters
	q.GroupByTags = make(TagSet)
	q.Filters = make([]Filter, 0)
	if m[5] != "" {
		f, err := ParseFilters(m[5], true, q)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse filter(s): %s", m[5])
		}
		q.Filters = append(q.Filters, f...)
	}
	if m[6] != "" {
		f, err := ParseFilters(m[6], false, q)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse filter(s): %s", m[6])
		}
		q.Filters = append(q.Filters, f...)
	}

	return
}

var filterValueRe = regexp.MustCompile(`([a-z_]+)\((.*)\)$`)

// ParseFilters parses filters in the form of `tagk=filterFunc(...),...`
// It also mimics OpenTSDB's promotion of queries with a * or no
// function to iwildcard and literal_or respectively
func ParseFilters(rawFilters string, grouping bool, q *Query) ([]Filter, error) {
	var filters []Filter
	for _, rawFilter := range strings.Split(rawFilters, ",") {
		splitRawFilter := strings.SplitN(rawFilter, "=", 2)
		if len(splitRawFilter) != 2 {
			return nil, fmt.Errorf("opentsdb: bad filter format: %s", rawFilter)
		}
		filter := Filter{}
		filter.TagK = splitRawFilter[0]
		if grouping {
			q.GroupByTags[filter.TagK] = ""
		}
		// See if we have a filter function, if not we have to use legacy parsing defined in
		// filter conversions of http://opentsdb.net/docs/build/html/api_http/query/index.html
		m := filterValueRe.FindStringSubmatch(splitRawFilter[1])
		if m != nil {
			filter.Type = m[1]
			filter.Filter = m[2]
		} else {
			// Legacy Conversion
			filter.Type = "literal_or"
			if strings.Contains(splitRawFilter[1], "*") {
				filter.Type = "iwildcard"
			}
			if splitRawFilter[1] == "*" {
				filter.Type = "wildcard"
			}
			filter.Filter = splitRawFilter[1]
		}
		filter.GroupBy = grouping
		filters = append(filters, filter)
	}
	return filters, nil
}

// ParseTags parses OpenTSDB tagk=tagv pairs of the form: k=v,m=o. Validation
// errors do not stop processing, and will return a non-nil TagSet.
func ParseTags(t string) (TagSet, error) {
	ts := make(TagSet)
	var err error
	for _, v := range strings.Split(t, ",") {
		sp := strings.SplitN(v, "=", 2)
		if len(sp) != 2 {
			return nil, fmt.Errorf("opentsdb: bad tag: %s", v)
		}
		for i, s := range sp {
			sp[i] = strings.TrimSpace(s)
			if i > 0 {
				continue
			}
			if !ValidTSDBString(sp[i]) {
				err = fmt.Errorf("invalid character in %s", sp[i])
			}
		}
		for _, s := range strings.Split(sp[1], "|") {
			if s == "*" {
				continue
			}
			if !ValidTSDBString(s) {
				err = fmt.Errorf("invalid character in %s", sp[1])
			}
		}
		if _, present := ts[sp[0]]; present {
			return nil, fmt.Errorf("opentsdb: duplicated tag: %s", v)
		}
		ts[sp[0]] = sp[1]
	}
	return ts, err
}

// ValidTSDBString returns true if s is a valid metric or tag.
func ValidTSDBString(s string) bool {

	// constructing a name processor isn't too expensive but we need to refactor this file so that it's possible to
	// inject instances so that we don't have to keep newing up.
	// For the moment I prefer to constructing like this to holding onto a global instance
	val, err := NewOpenTsdbNameProcessor("")
	if err != nil {
		return false
	}

	return val.IsValid(s)
}

var groupRE = regexp.MustCompile("{[^}]+}")

// ReplaceTags replaces all tag-like strings with tags from the given
// group. For example, given the string "test.metric{host=*}" and a TagSet
// with host=test.com, this returns "test.metric{host=test.com}".
func ReplaceTags(text string, group TagSet) string {
	return groupRE.ReplaceAllStringFunc(text, func(s string) string {
		tags, err := ParseTags(s[1 : len(s)-1])
		if err != nil {
			return s
		}
		for k := range tags {
			if group[k] != "" {
				tags[k] = group[k]
			}
		}
		return fmt.Sprintf("{%s}", tags.Tags())
	})
}

func (q Query) String() string {
	s := q.Aggregator + ":"
	if q.Downsample != "" {
		s += q.Downsample + ":"
	}
	if q.Rate {
		s += "rate"
		if q.RateOptions != nil {
			if q.RateOptions.Counter {
				s += "{"
				if q.RateOptions.DropResets {
					s += "dropcounter"
				} else {
					s += "counter"
				}
				if q.RateOptions.CounterMax != 0 {
					s += ","
					s += strconv.FormatInt(q.RateOptions.CounterMax, 10)
				}
				if q.RateOptions.ResetValue != 0 {
					if q.RateOptions.CounterMax == 0 {
						s += ","
					}
					s += ","
					s += strconv.FormatInt(q.RateOptions.ResetValue, 10)
				}
				s += "}"
			}
		}
		s += ":"
	}
	s += q.Metric
	if len(q.Tags) > 0 {
		s += q.Tags.String()
	}
	if len(q.Filters) > 0 {
		s += q.Filters.String()
	}
	return s
}

func (r *Request) String() string {
	s, _ := url.QueryUnescape(r.Encode())
	return s
}

func (r *Request) Encode() string {
	v := make(url.Values)

	if start, err := CanonicalTime(r.Start); err == nil {
		v.Add("start", start)
	}
	if end, err := CanonicalTime(r.End); err == nil {
		v.Add("end", end)
	}

	for _, q := range r.Queries {
		v.Add("m", q.String())
	}
	return v.Encode()
}

// Search returns a string suitable for OpenTSDB's `/` route.
func (r *Request) Search() string {
	// OpenTSDB uses the URL hash, not search parameters, to do this. The values are
	// not URL encoded. So it's the same as a url.Values just left as normal
	// strings.
	v, err := url.ParseQuery(r.Encode())
	if err != nil {
		return ""
	}
	buf := &bytes.Buffer{}
	for k, values := range v {
		for _, value := range values {
			fmt.Fprintf(buf, "%s=%s&", k, value)
		}
	}
	return buf.String()
}

// TSDBTimeFormat is the OpenTSDB-required time format for the time package.
const TSDBTimeFormat = "2006/01/02-15:04:05"

// CanonicalTime converts v to a string for use with OpenTSDB's `/` route.
func CanonicalTime(v interface{}) (string, error) {
	if s, ok := v.(string); ok {
		if strings.HasSuffix(s, "-ago") {
			return s, nil
		}
	}

	if s, ok := v.(TimeSpec); ok {
		if strings.HasSuffix(s.String(), "-ago") {
			return s.String(), nil
		}
	}

	t, err := ParseTime(v)
	if err != nil {
		return "", err
	}
	return t.Format(TSDBTimeFormat), nil
}

// TryParseAbsTime attempts to parse v as an absolute time. It may be a string
// in the format of TSDBTimeFormat or a float64 of seconds since epoch. If so,
// the epoch as an int64 is returned. Otherwise, v is returned.
func TryParseAbsTime(v interface{}) interface{} {
	switch v := v.(type) {
	case TimeSpec:
		d, err := ParseAbsTime(v.String())
		if err == nil {
			return d.Unix()
		}
	case string:
		d, err := ParseAbsTime(v)
		if err == nil {
			return d.Unix()
		}
	case float64:
		if v > 9999999999.0 {
			v = v / 1000
		}
		return int64(v)
	case int64:
		if v > 9999999999 {
			v = v / 1000
		}
		return int64(v)
	}
	return v
}

// ParseAbsTime returns the time of s, which must be of any non-relative (not
// "X-ago") format supported by OpenTSDB.
func ParseAbsTime(s string) (time.Time, error) {
	var t time.Time
	tFormats := [7]string{
		"2006/01/02-15:04:05",
		"2006/01/02 15:04:05",
		"2006/01/02-15:04",
		"2006/01/02 15:04",
		"2006/01/02-15",
		"2006/01/02 15",
		"2006/01/02",
	}
	for _, f := range tFormats {
		if len(f) == len(s) {
			if t, err := time.Parse(f, s); err == nil {
				return t, nil
			}
		}
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return t, err
	}
	if i > 9999999999 {
		i = i / 1000
	}
	return time.Unix(i, 0), nil
}

// ParseTime returns the time of v, which can be of any format supported by
// OpenTSDB.
func ParseTime(v interface{}) (time.Time, error) {
	now := time.Now().UTC()
	const max32 int64 = 9999999999 //0xffffffff
	switch i := v.(type) {
	case TimeSpec:
		if i.String() != "" {
			if strings.HasSuffix(i.String(), "-ago") {
				s := strings.TrimSuffix(i.String(), "-ago")
				d, err := ParseDuration(s)
				if err != nil {
					return now, err
				}
				return now.Add(time.Duration(-d)), nil
			}
			if strings.ToLower(i.String()) == "now" {
				return now, nil
			}
			return ParseAbsTime(i.String())
		}
		return now, nil
	case string:
		if i != "" {
			if strings.HasSuffix(i, "-ago") {
				s := strings.TrimSuffix(i, "-ago")
				d, err := ParseDuration(s)
				if err != nil {
					return now, err
				}
				return now.Add(time.Duration(-d)), nil
			}
			if strings.ToLower(i) == "now" {
				return now, nil
			}
			return ParseAbsTime(i)
		}
		return now, nil
	case int64:
		if i > max32 {
			i /= 1000
		}
		return time.Unix(i, 0).UTC(), nil
	case float64:
		i2 := int64(i)
		if i2 > max32 {
			i2 /= 1000
		}
		return time.Unix(i2, 0).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("type must be string or int64, got: %v", v)
	}
}

func (r *Request) GetDuration() (Duration, error) {
	return GetDuration(r)
}

func (r *Request) GetEnd() (TimeSpec, error) {
	var end time.Time
	var err error
	if r.End != nil && r.End != "" {
		end, err = ParseTime(r.End)
		if err != nil {
			return "", err
		}
	} else {
		end = time.Now().UTC()
	}
	return TimeSpec(strconv.FormatInt(end.Unix(), 10)), nil
}

// GetDuration returns the duration from the request's start to end.
func GetDuration(r *Request) (Duration, error) {
	var t Duration
	if r.Start == "" {
		return t, ErrMissingStartTime
	}
	start, err := ParseTime(r.Start)
	if err != nil {
		return t, err
	}
	var end time.Time
	if r.End != nil && r.End != "" {
		end, err = ParseTime(r.End)
		if err != nil {
			return t, err
		}
	} else {
		end = time.Now()
	}
	t = Duration(end.Sub(start))
	return t, nil
}

// AutoDownsample sets the avg downsample aggregator to produce l points.
func (r *Request) AutoDownsample(l int) error {
	if l == 0 {
		return ErrInvalidAutoDownsample
	}
	cd, err := GetDuration(r)
	if err != nil {
		return err
	}
	d := cd / Duration(l)
	ds := ""
	if d > Duration(time.Second)*15 {
		ds = fmt.Sprintf("%ds-avg", int64(d.Seconds()))
	}
	for _, q := range r.Queries {
		q.Downsample = ds
	}
	return nil
}

// SetTime adjusts the start and end time of the request to assume t is now.
// Relative times ("1m-ago") are changed to absolute times. Existing absolute
// times are adjusted by the difference between time.Now() and t.
func (r *Request) SetTime(t time.Time) error {
	diff := -time.Since(t)
	start, err := ParseTime(r.Start)
	if err != nil {
		return err
	}
	r.Start = TimeSpec(strconv.FormatInt(start.Add(diff).Unix(), 10))
	if r.End != "" {
		end, err := ParseTime(r.End)
		if err != nil {
			return err
		}
		r.End = TimeSpec(strconv.FormatInt(end.Add(diff).Unix(), 10))
	} else {
		r.End = TimeSpec(strconv.FormatInt(t.UTC().Unix(), 10))
	}
	return nil
}

// Query performs a v2 OpenTSDB request to the given host. host should be of the
// form hostname:port. Uses DefaultClient. Can return a RequestError.
func (r *Request) Query(host string) (ResponseSet, error) {
	resp, err := r.QueryResponse(host, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var tr ResponseSet
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, err
	}
	return tr, nil
}

func (r *Request) QueryResponse(host string, client *http.Client) (*http.Response, error) {
	return r.QueryResponseWithHeaders(host, client, nil)
}

// QueryResponse performs a v2 OpenTSDB request to the given host. host should
// be of the form hostname:port. A nil client uses DefaultClient.
func (r *Request) QueryResponseWithHeaders(host string, client *http.Client, headers http.Header) (*http.Response, error) {

	u := url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "/api/query",
	}

	pu, err := url.Parse(host)
	if err == nil && pu.Scheme != "" && pu.Host != "" {
		u.Scheme = pu.Scheme
		u.Host = pu.Host
		if pu.Path != "" {
			u.Path = pu.Path
		}
	}
	u.ForceQuery = true
	u.RawQuery = pu.RawQuery

	b, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = DefaultClient
	}

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	if userAgent != "" {
		req.Header.Add("User-Agent", userAgent)
	}

	for k, a := range headers {
		for _, v := range a {
			req.Header.Add(k, v)
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		e := RequestError{Request: string(b)}
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		if err := json.NewDecoder(bytes.NewBuffer(body)).Decode(&e); err == nil {
			return nil, &e
		}
		te := &TransportError{Code: resp.StatusCode}
		if len(body) > 0 {
			te.Body = body
		}
		return nil, te
	}
	return resp, nil
}

// TransportError is the error structure for errors
type TransportError struct {
	Code int    `json:"code" yaml:"code"`
	Body []byte `json:"body" yaml:"body"`
}

func (r TransportError) Error() string {
	return fmt.Sprintf("opentsdb: status=%d", r.Code)
}

// RequestError is the error structure for request errors.
type RequestError struct {
	Request string `json:"request" yaml:"request"`
	Err     struct {
		Code    int    `json:"code" yaml:"code"`
		Message string `json:"message" yaml:"message"`
		Details string `json:"details" yaml:"details"`
	} `json:"error" yaml:"error"`
}

func (r RequestError) Error() string {
	return fmt.Sprintf("opentsdb: status=%d req='%s' msg=%s", r.Err.Code, r.Request, r.Err.Message)
}

// Context is the interface for querying an OpenTSDB server.
type Context interface {
	Query(*Request) (ResponseSet, error)
	Version() Version
}

// Host is a simple OpenTSDB Context with no additional features.
type Host string

// Query performs the request to the OpenTSDB server.
func (h Host) Query(r *Request) (ResponseSet, error) {
	return r.Query(string(h))
}

// OpenTSDB 2.1 version struct
var Version2_1 = Version{2, 1}

// OpenTSDB 2.2 version struct
var Version2_2 = Version{2, 2}

// OpenTSDB 2.3 version struct
var Version2_3 = Version{2, 3}

// OpenTSDB 2.4 version struct
var Version2_4 = Version{2, 4}

type Version struct {
	Major int64
	Minor int64
}

func (v *Version) UnmarshalText(text []byte) error {
	var err error
	split := strings.Split(string(text), ".")
	if len(split) != 2 {
		return fmt.Errorf("invalid opentsdb version, expected number.number, (i.e 2.2) got %v", text)
	}
	v.Major, err = strconv.ParseInt(split[0], 10, 64)
	if err != nil {
		return fmt.Errorf("could not parse major version number for opentsdb version: %v", split[0])
	}
	v.Minor, err = strconv.ParseInt(split[0], 10, 64)
	if err != nil {
		return fmt.Errorf("could not parse minor version number for opentsdb version: %v", split[1])
	}
	return nil
}

func (v Version) FilterSupport() bool {
	return v.Major >= 2 && v.Minor >= 2
}

// LimitContext is a context that enables limiting response size and filtering tags
type LimitContext struct {
	Host string
	// Limit limits response size in bytes
	Limit int64
	// FilterTags removes tagks from results if that tagk was not in the request
	FilterTags bool
	// Use the version to see if groupby and filters are supported
	TSDBVersion Version
}

// NewLimitContext returns a new context for the given host with response sizes limited
// to limit bytes.
func NewLimitContext(host string, limit int64, version Version) *LimitContext {
	return &LimitContext{
		Host:        host,
		Limit:       limit,
		FilterTags:  true,
		TSDBVersion: version,
	}
}

func (c *LimitContext) Version() Version {
	return c.TSDBVersion
}

// Query returns the result of the request. r may be cached. The request is
// byte-limited and filtered by c's properties.
func (c *LimitContext) Query(r *Request) (tr ResponseSet, err error) {
	resp, err := r.QueryResponse(c.Host, nil)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	lr := &io.LimitedReader{R: resp.Body, N: c.Limit}
	err = json.NewDecoder(lr).Decode(&tr)
	if lr.N == 0 {
		err = fmt.Errorf("TSDB response too large: limited to %E bytes", float64(c.Limit))
		log.Print(err)
		return
	}
	if err != nil {
		return
	}
	if c.FilterTags {
		FilterTags(r, tr)
	}
	return
}

// FilterTags removes tagks in tr not present in r. Does nothing in the event of
// multiple queries in the request.
func FilterTags(r *Request, tr ResponseSet) {
	if len(r.Queries) != 1 {
		return
	}
	for _, resp := range tr {
		for k := range resp.Tags {
			_, inTags := r.Queries[0].Tags[k]
			inGroupBy := false
			for _, filter := range r.Queries[0].Filters {
				if filter.GroupBy && filter.TagK == k {
					inGroupBy = true
					break
				}
			}
			if inTags || inGroupBy {
				continue
			}
			delete(resp.Tags, k)
		}
	}
}

func (dps DPmap) GetSortedTimes() []Epoch {
	times := make([]Epoch, 0, len(dps))
	for k := range dps {
		times = append(times, k)
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	return times
}
