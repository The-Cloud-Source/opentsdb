package opentsdb

import (
	"errors"
	"regexp"
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
