package opentsdb

import (
	"log"
	"strconv"
)

type TimeTSDB string

// String returns the literal text of the number.
func (n TimeTSDB) String() string { return string(n) }

// Float64 returns the number as a float64.
func (n TimeTSDB) Float64() (float64, error) {
	return strconv.ParseFloat(string(n), 64)
}

// Int64 returns the number as an int64.
func (n TimeTSDB) Int64() (int64, error) {
	return strconv.ParseInt(string(n), 10, 64)
}

func (t *TimeTSDB) UnmarshalJSON(data []byte) error {
	v := TimeTSDB(data)
	*t = v
	log.Printf("%s", string(data))
	return nil
}

func (t TimeTSDB) MarshalJSON() ([]byte, error) {
	return []byte(t), nil
}
