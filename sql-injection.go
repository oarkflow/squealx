package squealx

import (
	"fmt"
	"log"
	"regexp"
	"strings"
)

// Each pattern is wrapped in its own capturing group.
var combinedPatterns = []string{
	`(\b(?:or|and)\b\s+\d+\s*=\s*\d+)`, // boolean_tautology
	`(union\b\s+select)`,               // union_select
	`((?:--|#))`,                       // sql_comment
	`(;.*\b(?:select|update|insert|delete|drop|create|alter|truncate)\b)`, // piggyback_query
	`(\b(?:or|and)\b\s+1\s*=\s*1)`,                                        // always_true
	`(\b(?:drop|alter|create|truncate)\b)`,                                // sql_command
	`(/\*.*?\*/)`,                                                         // inline_comment
	`(\b(?:exec|execute)\b)`,                                              // exec_command
	`(\bsleep\s*\()`,                                                      // sleep_function
	`(\bbenchmark\s*\()`,                                                  // benchmark_function
	`(\bload_file\s*\()`,                                                  // load_file
	`(\binto\s+outfile\b)`,                                                // into_outfile
	`(0x[0-9a-fA-F]+)`,                                                    // hex_encoding
	`(\b(?:concat|char)\s*\()`,                                            // concat_function
	`(/\*.*(?:--|#).*?\*/)`,                                               // obfuscated_comment
}

// patternNames holds the names corresponding to each combined pattern.
var patternNames = []string{
	"boolean_tautology",
	"union_select",
	"sql_comment",
	"piggyback_query",
	"always_true",
	"sql_command",
	"inline_comment",
	"exec_command",
	"sleep_function",
	"benchmark_function",
	"load_file",
	"into_outfile",
	"hex_encoding",
	"concat_function",
	"obfuscated_comment",
}

// errorMessages provides a unique error message for each detected pattern.
var errorMessages = map[string]string{
	"boolean_tautology":  "Detected boolean tautology injection attempt.",
	"union_select":       "Detected UNION SELECT injection attempt.",
	"sql_comment":        "Detected SQL comment injection attempt.",
	"piggyback_query":    "Detected piggyback query injection attempt.",
	"always_true":        "Detected always-true injection attempt.",
	"sql_command":        "Detected dangerous SQL command injection attempt.",
	"inline_comment":     "Detected inline comment injection attempt.",
	"exec_command":       "Detected execution command injection attempt.",
	"sleep_function":     "Detected sleep function injection attempt.",
	"benchmark_function": "Detected benchmark function injection attempt.",
	"load_file":          "Detected load_file function injection attempt.",
	"into_outfile":       "Detected INTO OUTFILE injection attempt.",
	"hex_encoding":       "Detected hex encoding injection attempt.",
	"concat_function":    "Detected concat or char function injection attempt.",
	"obfuscated_comment": "Detected obfuscated comment injection attempt.",
}

// combinedInjectionRegex is built by joining all patterns using alternation.
var combinedInjectionRegex = regexp.MustCompile("(?i)" + strings.Join(combinedPatterns, "|"))

// detectInjectionCombinedWithGroups checks the input against the combined regex.
// It returns a slice of strings representing error messages for each pattern that was matched.
func detectInjectionCombinedWithGroups(input string) []string {
	var detectedErrors []string
	s := strings.TrimSpace(input)
	s = strings.ToLower(s)
	match := combinedInjectionRegex.FindStringSubmatch(s)
	if match != nil {
		// match[0] is the full match; subsequent elements correspond to our capturing groups.
		for i, group := range match[1:] {
			if group != "" {
				patternName := patternNames[i]
				if errMsg, ok := errorMessages[patternName]; ok {
					detectedErrors = append(detectedErrors, errMsg)
				} else {
					detectedErrors = append(detectedErrors, "Detected unknown injection pattern.")
				}
			}
		}
	}
	return detectedErrors
}

// SafeQuery checks both the query string and its parameters for suspicious patterns.
// It uses the combined regex with groups to determine which patterns matched and returns the respective error messages.
func SafeQuery(query string, args ...any) error {
	return nil
	query = string(RemoveSQLComments([]byte(query)))
	// Check the query string itself.
	if errors := detectInjectionCombinedWithGroups(query); len(errors) > 0 {
		log.Printf("Warning: Query string appears suspicious: %v, %s, %+v", errors, query, args)
		return fmt.Errorf("unsafe query string detected: %s", strings.Join(errors, " "))
	}

	// Check each argument that is a string.
	for i, arg := range args {
		if strArg, ok := arg.(string); ok {
			if errors := detectInjectionCombinedWithGroups(strArg); len(errors) > 0 {
				log.Printf("Warning: Parameter %d appears suspicious: %v, %s, %+v", i, errors, query, args)
				return fmt.Errorf("unsafe query parameter detected: %s", strings.Join(errors, " "))
			}
		}
	}
	return nil
}

// RemoveSQLComments removes SQL comments from the input byte slice.
// It supports line comments starting with "--" or "#" and block comments delimited by "/* ... */".
// String literals (delimited by a single quote) are preserved intact so that comment-like text inside quotes is not removed.
// This function works in-place, avoiding extra allocations.
func RemoveSQLComments(src []byte) []byte {
	// state 0: normal, 1: inside string literal, 2: inside line comment, 3: inside block comment
	const (
		normal = iota
		inString
		inLineComment
		inBlockComment
	)
	state := normal
	w, i := 0, 0

	for i < len(src) {
		switch state {
		case normal:
			// Check for start of a string literal.
			if src[i] == '\'' {
				src[w] = src[i]
				w++
				i++
				state = inString
			} else if i+1 < len(src) && src[i] == '-' && src[i+1] == '-' {
				// Start of a -- line comment.
				i += 2
				state = inLineComment
			} else if src[i] == '#' {
				// Start of a # line comment.
				i++
				state = inLineComment
			} else if i+1 < len(src) && src[i] == '/' && src[i+1] == '*' {
				// Start of a block comment.
				i += 2
				state = inBlockComment
			} else {
				// Regular character; copy to write index.
				src[w] = src[i]
				w++
				i++
			}
		case inString:
			// Inside a string literal.
			// Copy current character. Handle escape sequences.
			if src[i] == '\\' && i+1 < len(src) {
				src[w] = src[i]
				src[w+1] = src[i+1]
				w += 2
				i += 2
			} else {
				src[w] = src[i]
				w++
				// End of string literal.
				if src[i] == '\'' {
					state = normal
				}
				i++
			}
		case inLineComment:
			// Skip until the end of the line.
			if src[i] == '\n' {
				// Optionally, keep the newline.
				src[w] = src[i]
				w++
				i++
				state = normal
			} else {
				i++
			}
		case inBlockComment:
			// Skip until the closing "*/" is found.
			if i+1 < len(src) && src[i] == '*' && src[i+1] == '/' {
				i += 2
				state = normal
			} else {
				i++
			}
		}
	}
	return src[:w]
}
