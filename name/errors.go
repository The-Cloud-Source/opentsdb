package name

import "errors"

var (
	ErrInvalidRuneCheck = errors.New("no isValidRuneCheck provided")
	ErrInvalidPatern    = errors.New("no validPattern provided")
)
