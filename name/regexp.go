package name

import (
	"regexp"
)

type regexpValidationConfig struct {
	matcher *regexp.Regexp
}

// NewRegexpValidator constructs a new Validator which verifies that a name matches a specific regular expression
func NewRegexpValidator(validPattern string) (Validator, error) {
	if len(validPattern) == 0 {
		return nil, ErrInvalidPatern
	}

	matcher, err := regexp.Compile(validPattern)
	if err != nil {
		return nil, err
	}

	result := &regexpValidationConfig{matcher: matcher}
	return result, nil
}

// IsValid returns true if the given name matches the Validators regular expression
func (c *regexpValidationConfig) IsValid(name string) bool {
	return c.matcher.MatchString(name)
}
