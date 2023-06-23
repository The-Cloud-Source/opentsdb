package opentsdb

import (
	"errors"

	"github.com/the-cloud-source/opentsdb/name"
)

type DurationError struct{ error }

var (
	ErrMissingStartTime      = errors.New("start time must be provided")
	ErrInvalidAutoDownsample = errors.New("opentsdb: target length must be > 0")

	ErrInvalidRuneCheck = errInvalidRuneCheck()
	ErrInvalidPatern    = errInvalidPatern()
	ErrNameLeftEmpty    = errors.New("Name left empty after formatting")

	ErrLeadingInt = errors.New("time: bad [0-9]*")
)

func errInvalidRuneCheck() error {
	return name.ErrInvalidRuneCheck
}

func errInvalidPatern() error {
	return name.ErrInvalidPatern
}
