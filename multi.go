package opentsdb

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
)

// SynContext is a context that enables limiting response size and filtering tags
type SynContext struct {
	Host        string
	Limit       int64   // Limit limits response size in bytes
	FilterTags  bool    // FilterTags removes tagks from results if that tagk was not in the request
	TSDBVersion Version // Use the version to see if groupby and filters are supported
	Synth       TagSet  // Synthetic Tags
}

type MultiContext struct {
	Hosts []*SynContext
}

func (_ *SynContext) Version() Version {
	return Version2_4
}

func (_ *MultiContext) Version() Version {
	return Version2_4
}

func NewSynContext(host string, limit int64) *SynContext {
	if limit == -1 {
		limit = math.MaxInt64
	}
	ctx := &SynContext{
		Host:       host,
		Limit:      limit,
		FilterTags: false,
		Synth:      TagSet{},
	}

	return ctx
}

func NewMultiContext(syn ...*SynContext) *MultiContext {
	m := &MultiContext{}
	for _, s := range syn {
		m.Hosts = append(m.Hosts, s)
	}
	return m
}

func (ctx *MultiContext) AddContext(v *SynContext) *MultiContext {
	ctx.Hosts = append(ctx.Hosts, v)
	return ctx
}

func (ctx *SynContext) Query(r *Request) (ResponseSet, error) {
	return ctx.QueryWithHeaders(r, nil)
}

func (ctx *SynContext) QueryWithHeaders(r *Request, headers http.Header) (ResponseSet, error) {

	tr := ResponseSet{}

	resp, err := r.QueryResponseWithHeaders(ctx.Host, nil, headers)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	lr := &io.LimitedReader{R: resp.Body, N: ctx.Limit}
	err = json.NewDecoder(lr).Decode(&tr)
	if lr.N == 0 {
		err = fmt.Errorf("TSDB response too large: limited to %E bytes", float64(ctx.Limit))
		log.Print(err)
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	if ctx.FilterTags {
		FilterTags(r, tr)
	}
	return tr, nil
}

func (ctx *MultiContext) Query(request *Request) (ResponseSet, error) {
	return ctx.QueryWithHeaders(request, nil)
}

func (ctx *MultiContext) QueryWithHeaders(request *Request, headers http.Header) (ResponseSet, error) {

	resultsIdx := map[string]int{}
	result := ResponseSet{}
	responses := []ResponseSet{}

	for _, host := range ctx.Hosts {
		tr, err := host.QueryWithHeaders(request, headers)
		if err != nil {
			return nil, err
		}
		responses = append(responses, tr)
	}

	if len(responses) < 1 {
		return result, nil
	}

	for _, r := range responses[0] {
		resKey := stableKey(r)
		result = append(result, r)
		resultsIdx[resKey] = len(result) - 1
	}

	for i := 1; i < len(responses); i++ {
		for _, r := range responses[i] {
			resKey := stableKey(r)
			idx, ok := resultsIdx[resKey]
			if !ok {
				result = append(result, r)
				resultsIdx[resKey] = len(result)
				continue
			}
			result[idx].DPS.Join(r.DPS, r.Query.Aggregator)
		}
	}

	return result, nil
}
