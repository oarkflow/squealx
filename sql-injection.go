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
	`(information_schema)`,                                                // information_schema
	`(\bload_file\s*\()`,                                                  // load_file
	`(\binto\s+outfile\b)`,                                                // into_outfile
	`(0x[0-9a-fA-F]+)`,                                                    // hex_encoding
	`(\b(?:if\s*\(|case\s+when\b))`,                                       // blind_injection
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
	"information_schema",
	"load_file",
	"into_outfile",
	"hex_encoding",
	"blind_injection",
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
	"information_schema": "Detected attempt to access information_schema.",
	"load_file":          "Detected load_file function injection attempt.",
	"into_outfile":       "Detected INTO OUTFILE injection attempt.",
	"hex_encoding":       "Detected hex encoding injection attempt.",
	"blind_injection":    "Detected blind SQL injection attempt.",
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
	// Check the query string itself.
	if errors := detectInjectionCombinedWithGroups(query); len(errors) > 0 {
		log.Printf("Warning: Query string appears suspicious: %v", errors)
		return fmt.Errorf("unsafe query string detected: %s", strings.Join(errors, " "))
	}

	// Check each argument that is a string.
	for i, arg := range args {
		if strArg, ok := arg.(string); ok {
			if errors := detectInjectionCombinedWithGroups(strArg); len(errors) > 0 {
				log.Printf("Warning: Parameter %d appears suspicious: %v", i, errors)
				return fmt.Errorf("unsafe query parameter detected: %s", strings.Join(errors, " "))
			}
		}
	}
	return nil
}
