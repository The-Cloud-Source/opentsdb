package opentsdb

import (
	"errors"
	"regexp"
	"time"
)

var dRexStr = `([+-]?[0-9]+.?[0-9]*(?:ms|s|m|h|d|w|n|y))(?:-[a-z]+)(?:-[a-z]+)?`
var drex = regexp.MustCompile(dRexStr)

func ParseDownsample(d string) (Duration, error) {

	match := drex.FindStringSubmatch(d)
	if len(match) != 2 {
		return 0, errors.New("Invalid downsample")
	}

	return ParseDuration(match[1])
}

const maxDuration = Duration(^uint(0) >> 1)

func (r *Request) GetMinDownsample() (Duration, error) {
	var ds Duration = maxDuration
	if len(r.Queries) < 1 {
		return ds, nil
	}

	for _, q := range r.Queries {
		tmp, err := ParseDownsample(q.Downsample)
		if err == nil {
			if tmp < ds {
				ds = tmp
			}
		}
	}

	if ds == maxDuration {
		return Duration(1 * time.Second), nil
	}
	return ds, nil
}

func (r *Request) EstimateDPS() (dps int64, err error) {

	duration, err := r.GetDuration()
	if err != nil {
		return dps, err
	}

	d := duration.SecondsInt64()
	for _, q := range r.Queries {
		if q.Downsample == "" {
			dps += d // 1 dp per sec
		} else {
			ds, err := ParseDownsample(q.Downsample)
			if err != nil {
				return dps, err
			}
			dps += int64(d / ds.SecondsInt64())
		}
	}
	return dps, nil
}
