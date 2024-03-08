package squealx

import (
	"regexp"
)

var (
	namedRE = regexp.MustCompile(`\b[^:]+:[^:]+\b`)
)

func IsNamedQuery(query string) bool {
	return namedRE.MatchString(query)
}
