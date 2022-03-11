package opentsdb

import (
	"strconv"
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
