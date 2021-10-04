package opentsdb

import (
	"strconv"
)

func (v *Epoch) UnmarshalText(text []byte) error {
	n, err := strconv.ParseInt(string(text), 10, 64)
	*v = Epoch(n)
	return err
}

func (v Epoch) MarshalText() (text []byte, err error) {
	text = strconv.AppendInt(text, int64(v), 10)
	return text, err
}

func (v Epoch) String() string {
	return strconv.FormatInt(int64(v), 10)
}
