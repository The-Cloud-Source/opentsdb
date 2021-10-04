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

type EpochSlice []Epoch

func (x EpochSlice) Len() int           { return len(x) }
func (x EpochSlice) Less(i, j int) bool { return x[i] < x[j] }
func (x EpochSlice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func (x EpochSlice) Sort() { Sort(x) }
