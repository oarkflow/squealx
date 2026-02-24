package jsonbq

type SQLTemplate struct {
	Raw       string
	Tokens    []SQLToken
	Statement *SQLStatement
}

func NewSQLTemplate(query string) (*SQLTemplate, error) {
	tokens, err := LexSQL(query)
	if err != nil {
		return nil, err
	}
	return &SQLTemplate{
		Raw:       query,
		Tokens:    tokens,
		Statement: ParseTokens(tokens),
	}, nil
}

func (t *SQLTemplate) Compile(vars map[string]any) (string, []any, error) {
	return compileSQLTemplateTokens(t.Tokens, vars)
}
