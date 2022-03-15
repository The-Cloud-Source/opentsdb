package opentsdb

import (
	"strconv"
	"strings"
	"time"
)

type TimeSpec string

// String returns the literal text of the number.
func (n TimeSpec) String() string { return string(n) }

// Float64 returns the number as a float64.
func (n TimeSpec) Float64() (float64, error) {
	return strconv.ParseFloat(string(n), 64)
}

// Int64 returns the number as an int64.
func (n TimeSpec) Int64() (int64, error) {
	return strconv.ParseInt(string(n), 10, 64)
}

func (t *TimeSpec) UnmarshalJSON(data []byte) error {
	v := TimeSpec(data)
	*t = v
	return nil
}

func (t TimeSpec) MarshalJSON() ([]byte, error) {
	return []byte(t), nil
}

func (v TimeSpec) CanonicalTimeString(defaultNow bool) (string, error) {

	s := string(v)

	if len(s) == 0 {
		if defaultNow {
			return time.Now().UTC().Format(TSDBTimeFormat), nil
		} else {
			return s, nil
		}
	}

	if strings.HasSuffix(s, "-ago") {
		return s, nil
	}

	if s == "now" {
		return s, nil
	}

	if len(s) == 13 || len(s) == 10 {
		i, err := v.Int64()
		if err == nil {
			if len(s) == 13 {
				i = i / 1000
			}
			return time.Unix(i, 0).Format(TSDBTimeFormat), nil
		}
	}

	return s, nil
}
