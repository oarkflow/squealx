package squealx

// Named Query Support
//
//  * BindMap - bind query bindvars to map/struct args
//	* NamedExec, NamedQuery - named query w/ struct or map
//  * NamedStmt - a pre-compiled named query which is a prepared statement
//
// Internal Interfaces:
//
//  * compileNamedQuery - rebind a named query, returning a query and list of names
//  * bindArgs, bindMapArgs, bindAnyArgs - given a list of names, return an arglist
//
import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/oarkflow/squealx/reflectx"
	"github.com/oarkflow/squealx/sqltoken"
)

// NamedStmt is a prepared statement that executes named queries.  Prepare it
// how you would execute a NamedQuery, but pass in a struct or map when executing.
type NamedStmt struct {
	Params      []string
	QueryString string
	Stmt        *Stmt
}

// Close closes the named statement.
func (n *NamedStmt) Close() error {
	return n.Stmt.Close()
}

// Exec executes a named statement using the struct passed.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Exec(arg any) (sql.Result, error) {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return *new(sql.Result), err
	}
	return n.Stmt.Exec(args...)
}

// Query executes a named statement using the struct argument, returning rows.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Query(arg any) (SQLRows, error) {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return nil, err
	}
	return n.Stmt.Query(args...)
}

// QueryRow executes a named statement against the database.  Because sqlx cannot
// create a *sql.Row with an error condition pre-set for binding errors, sqlx
// returns a *sqlx.Row instead.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryRow(arg any) *Row {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return &Row{err: err}
	}
	return n.Stmt.QueryRowx(args...)
}

// MustExec execs a NamedStmt, panicing on error
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) MustExec(arg any) sql.Result {
	res, err := n.Exec(arg)
	if err != nil {
		panic(err)
	}
	return res
}

// Queryx using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Queryx(arg any) (*Rows, error) {
	r, err := n.Query(arg)
	if err != nil {
		return nil, err
	}
	return &Rows{SQLRows: r, Mapper: n.Stmt.Mapper, unsafe: isUnsafe(n)}, err
}

// QueryRowx this NamedStmt.  Because of limitations with QueryRow, this is
// an alias for QueryRow.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryRowx(arg any) *Row {
	return n.QueryRow(arg)
}

// Select using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Select(dest any, arg any) error {
	rows, err := n.Queryx(arg)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return ScannAll(rows, dest, false)
}

// Get using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Get(dest any, arg any) error {
	r := n.QueryRowx(arg)
	return r.scanAny(dest, false)
}

// Unsafe creates an unsafe version of the NamedStmt
func (n *NamedStmt) Unsafe() *NamedStmt {
	r := &NamedStmt{Params: n.Params, Stmt: n.Stmt, QueryString: n.QueryString}
	r.Stmt.unsafe = true
	return r
}

// A union interface of preparer and binder, required to be able to prepare
// named statements (as the bindtype must be determined).
type namedPreparer interface {
	Preparer
	binder
}

func prepareNamed(p namedPreparer, query string) (*NamedStmt, error) {
	bindType := BindType(p.DriverName())
	q, args, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return nil, err
	}
	stmt, err := Preparex(p, q)
	if err != nil {
		return nil, err
	}
	return &NamedStmt{
		QueryString: q,
		Params:      args,
		Stmt:        stmt,
	}, nil
}

// convertMapStringInterface attempts to convert v to map[string]any.
// Unlike v.(map[string]any), this function works on named types that
// are convertible to map[string]any as well.
func convertMapStringInterface(v any) (map[string]any, bool) {
	val := reflect.ValueOf(v)
	if !val.IsValid() {
		return nil, false
	}

	// If it's a pointer, dereference it
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, false
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Map {
		return nil, false
	}

	// Check if it's a map[string]any
	mapType := reflect.TypeOf(map[string]any{})
	if !val.Type().ConvertibleTo(mapType) {
		return nil, false
	}

	return val.Convert(mapType).Interface().(map[string]any), true
}

func bindAnyArgs(names []string, arg any, m *reflectx.Mapper) ([]any, error) {
	if maparg, ok := convertMapStringInterface(arg); ok {
		return bindMapArgs(names, maparg)
	}
	return bindArgs(names, arg, m)
}

// private interface to generate a list of interfaces from a given struct
// type, given a list of names to pull out of the struct.  Used by public
// BindStruct interface.
func bindArgs(names []string, arg any, m *reflectx.Mapper) ([]any, error) {
	arglist := make([]any, 0, len(names))

	// grab the indirected value of arg
	var v reflect.Value
	for v = reflect.ValueOf(arg); v.Kind() == reflect.Ptr; {
		v = v.Elem()
	}

	err := m.TraversalsByNameFunc(v.Type(), names, func(i int, t []int) error {
		if len(t) == 0 {
			return fmt.Errorf("could not find name %s in %#v", names[i], arg)
		}

		val := reflectx.FieldByIndexesReadOnly(v, t)
		arglist = append(arglist, val.Interface())

		return nil
	})

	return arglist, err
}

// like bindArgs, but for maps.
func bindMapArgs(names []string, arg map[string]any) ([]any, error) {
	arglist := make([]any, 0, len(names))

	for _, name := range names {
		val, ok := arg[name]
		if !ok {
			return arglist, fmt.Errorf("could not find name %s in %#v", name, arg)
		}
		arglist = append(arglist, val)
	}
	return arglist, nil
}

// bindStruct binds a named parameter query with fields from a struct argument.
// The rules for binding field names to parameter names follow the same
// conventions as for StructScan, including obeying the `db` struct tags.
func bindStruct(bindType int, query string, arg any, m *reflectx.Mapper) (string, []any, error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", []any{}, err
	}

	arglist, err := bindAnyArgs(names, arg, m)
	if err != nil {
		return "", []any{}, err
	}

	return bound, arglist, nil
}

var valuesReg = regexp.MustCompile(`(?:\)|AS\s*\(|FROM\s*\()\s*(?i)VALUES\s*\(`)

func findMatchingClosingBracketIndex(s string) int {
	count := 0
	for i, ch := range s {
		if ch == '(' {
			count++
		}
		if ch == ')' {
			count--
			if count == 0 {
				return i
			}
		}
	}
	return 0
}

func fixBound(bound string, loop int) string {
	loc := valuesReg.FindStringIndex(bound)
	// defensive guard when "VALUES (...)" not found
	if len(loc) < 2 {
		return bound
	}

	openingBracketIndex := loc[1] - 1
	index := findMatchingClosingBracketIndex(bound[openingBracketIndex:])
	// defensive guard. must have closing bracket
	if index == 0 {
		return bound
	}
	closingBracketIndex := openingBracketIndex + index + 1

	length := closingBracketIndex + (loop-1)*(closingBracketIndex-openingBracketIndex+1) + len(bound) - closingBracketIndex
	buffer := bytes.NewBuffer(make([]byte, 0, length))

	buffer.WriteString(bound[0:closingBracketIndex])
	for i := 0; i < loop-1; i++ {
		buffer.WriteString(",")
		buffer.WriteString(bound[openingBracketIndex:closingBracketIndex])
	}
	buffer.WriteString(bound[closingBracketIndex:])
	return buffer.String()
}

// bindArray binds a named parameter query with fields from an array or slice of
// structs argument.
func bindArray(bindType int, query string, arg any, m *reflectx.Mapper) (string, []any, error) {
	// do the initial binding with QUESTION;  if bindType is not question,
	// we can rebind it at the end.
	bound, names, err := compileNamedQuery([]byte(query), QUESTION)
	if err != nil {
		return "", []any{}, err
	}
	arrayValue := reflect.ValueOf(arg)
	arrayLen := arrayValue.Len()
	if arrayLen == 0 {
		return "", []any{}, fmt.Errorf("length of array is 0: %#v", arg)
	}
	var arglist = make([]any, 0, len(names)*arrayLen)
	for i := 0; i < arrayLen; i++ {
		elemArglist, err := bindAnyArgs(names, arrayValue.Index(i).Interface(), m)
		if err != nil {
			return "", []any{}, err
		}
		arglist = append(arglist, elemArglist...)
	}
	if arrayLen > 1 {
		bound = fixBound(bound, arrayLen)
	}
	// adjust binding type if we weren't on question
	if bindType != QUESTION {
		bound = Rebind(bindType, bound)
	}
	return bound, arglist, nil
}

// bindMap binds a named parameter query with a map of arguments.
func bindMap(bindType int, query string, args map[string]any) (string, []any, error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", []any{}, err
	}
	arglist, err := bindMapArgs(names, args)
	return bound, arglist, err
}

var namedParseConfigs = func() []sqltoken.Config {
	configs := make([]sqltoken.Config, AT+1)
	pg := sqltoken.PostgreSQLConfig()
	pg.NoticeColonWord = true
	pg.ColonWordIncludesUnicode = true
	pg.NoticeDollarNumber = false
	configs[DOLLAR] = pg

	ora := sqltoken.OracleConfig()
	ora.ColonWordIncludesUnicode = true
	configs[NAMED] = ora

	ssvr := sqltoken.SQLServerConfig()
	ssvr.NoticeColonWord = true
	ssvr.ColonWordIncludesUnicode = true
	ssvr.NoticeAtWord = false
	configs[AT] = ssvr

	mysql := sqltoken.MySQLConfig()
	mysql.NoticeColonWord = true
	mysql.ColonWordIncludesUnicode = true
	mysql.NoticeQuestionMark = false
	configs[QUESTION] = mysql
	configs[UNKNOWN] = mysql
	return configs
}()

type parseNamedState int

const (
	parseStateConsumingIdent parseNamedState = iota
	parseStateQuery
	parseStateQuotedIdent
	parseStateStringConstant
	parseStateLineComment
	parseStateBlockComment
	parseStateSkipThenTransition
	parseStateDollarQuoteLiteral
)

type parseNamedContext struct {
	state parseNamedState
	data  map[string]interface{}
}

const (
	colon        = ':'
	backSlash    = '\\'
	forwardSlash = '/'
	singleQuote  = '\''
	dash         = '-'
	star         = '*'
	newLine      = '\n'
	dollarSign   = '$'
	doubleQuote  = '"'
)

// -- Compilation of Named Queries

// Allow digits and letters in bind params;  additionally runes are
// checked against underscores, meaning that bind params can have be
// alphanumeric with underscores.  Mind the difference between unicode
// digits and numbers, where '5' is a digit but '五' is not.
var allowedBindRunes = []*unicode.RangeTable{unicode.Letter, unicode.Digit}

// FIXME: this function isn't safe for unicode named params, as a failing test
// can testify.  This is not a regression but a failure of the original code
// as well.  It should be modified to range over runes in a string rather than
// bytes, even though this is less convenient and slower.  Hopefully the
// addition of the prepared NamedStmt (which will only do this once) will make
// up for the slightly slower ad-hoc NamedExec/NamedQuery.

// compile a NamedQuery into an unbound query (using the '?' bindvar) and
// a list of names.
func compileNamedQuery(qs []byte, bindType int) (query string, names []string, err error) {
	var result strings.Builder
	var params []string

	addParam := func(paramName string) {
		params = append(params, paramName)

		switch bindType {
		// oracle only supports named type bind vars even for positional
		case NAMED:
			result.WriteByte(':')
			result.WriteString(paramName)
		case QUESTION, UNKNOWN:
			result.WriteByte('?')
		case DOLLAR:
			result.WriteByte('$')
			result.WriteString(strconv.Itoa(len(params)))
		case AT:
			result.WriteString("@p")
			result.WriteString(strconv.Itoa(len(params)))
		}
	}

	isRuneStartOfIdent := func(r rune) bool {
		return unicode.In(r, unicode.Letter) || r == '_'
	}

	isRunePartOfIdent := func(r rune) bool {
		return isRuneStartOfIdent(r) || unicode.In(r, allowedBindRunes...) || r == '_' || r == '.'
	}

	ctx := parseNamedContext{state: parseStateQuery}

	setState := func(s parseNamedState, d map[string]interface{}) {
		ctx.data = d
		ctx.state = s
	}

	var previousRune rune
	maxIndex := len(qs)

	for byteIndex := 0; byteIndex < maxIndex; {
		currentRune, runeWidth := utf8.DecodeRune(qs[byteIndex:])
		nextRuneByteIndex := byteIndex + runeWidth

		nextRune := utf8.RuneError
		if nextRuneByteIndex < maxIndex {
			nextRune, _ = utf8.DecodeRune(qs[nextRuneByteIndex:])
		}

		writeCurrentRune := true
		switch ctx.state {
		case parseStateQuery:
			if currentRune == colon && previousRune != colon && isRuneStartOfIdent(nextRune) {
				// :foo
				writeCurrentRune = false
				setState(parseStateConsumingIdent, map[string]interface{}{
					"ident": &strings.Builder{},
				})
			} else if currentRune == singleQuote && previousRune != backSlash {
				// \'
				setState(parseStateStringConstant, nil)
			} else if currentRune == dash && nextRune == dash {
				// -- single line comment
				setState(parseStateLineComment, nil)
			} else if currentRune == forwardSlash && nextRune == star {
				// /*
				setState(parseStateSkipThenTransition, map[string]interface{}{
					"state": parseStateBlockComment,
					"data": map[string]interface{}{
						"depth": 1,
					},
				})
			} else if currentRune == dollarSign && previousRune == dollarSign {
				// $$
				setState(parseStateDollarQuoteLiteral, nil)
			} else if currentRune == doubleQuote {
				// "foo"."bar"
				setState(parseStateQuotedIdent, nil)
			}
		case parseStateConsumingIdent:
			if isRunePartOfIdent(currentRune) {
				ctx.data["ident"].(*strings.Builder).WriteRune(currentRune)
				writeCurrentRune = false
			} else {
				addParam(ctx.data["ident"].(*strings.Builder).String())
				setState(parseStateQuery, nil)
			}
		case parseStateBlockComment:
			if previousRune == star && currentRune == forwardSlash {
				newDepth := ctx.data["depth"].(int) - 1
				if newDepth == 0 {
					setState(parseStateQuery, nil)
				} else {
					ctx.data["depth"] = newDepth
				}
			}
		case parseStateLineComment:
			if currentRune == newLine {
				setState(parseStateQuery, nil)
			}
		case parseStateStringConstant:
			if currentRune == singleQuote && previousRune != backSlash {
				setState(parseStateQuery, nil)
			}
		case parseStateDollarQuoteLiteral:
			if currentRune == dollarSign && previousRune != dollarSign {
				setState(parseStateQuery, nil)
			}
		case parseStateQuotedIdent:
			if currentRune == doubleQuote {
				setState(parseStateQuery, nil)
			}
		case parseStateSkipThenTransition:
			setState(ctx.data["state"].(parseNamedState), ctx.data["data"].(map[string]interface{}))
		default:
			setState(parseStateQuery, nil)
		}

		if writeCurrentRune {
			result.WriteRune(currentRune)
		}

		previousRune = currentRune
		byteIndex = nextRuneByteIndex
	}

	// If parsing left off while consuming an ident, add that ident to params
	if ctx.state == parseStateConsumingIdent {
		addParam(ctx.data["ident"].(*strings.Builder).String())
	}

	return result.String(), params, nil
}

// kept for benchmarking purposes
func oldCmpileNamedQuery(qs []byte, bindType int) (query string, names []string, err error) {
	names = make([]string, 0, 10)
	rebound := make([]byte, 0, len(qs))

	inName := false
	last := len(qs) - 1
	currentVar := 1
	name := make([]byte, 0, 10)

	for i, b := range qs {
		// a ':' while we're in a name is an error
		if b == ':' {
			// if this is the second ':' in a '::' escape sequence, append a ':'
			if inName && i > 0 && qs[i-1] == ':' {
				rebound = append(rebound, ':')
				inName = false
				continue
			} else if inName {
				err = errors.New("unexpected `:` while reading named param at " + strconv.Itoa(i))
				return query, names, err
			}
			inName = true
			name = []byte{}
		} else if inName && i > 0 && b == '=' && len(name) == 0 {
			rebound = append(rebound, ':', '=')
			inName = false
			continue
			// if we're in a name, and this is an allowed character, continue
		} else if inName && (unicode.IsOneOf(allowedBindRunes, rune(b)) || b == '_' || b == '.') && i != last {
			// append the byte to the name if we are in a name and not on the last byte
			name = append(name, b)
			// if we're in a name and it's not an allowed character, the name is done
		} else if inName {
			inName = false
			// if this is the final byte of the string and it is part of the name, then
			// make sure to add it to the name
			if i == last && unicode.IsOneOf(allowedBindRunes, rune(b)) {
				name = append(name, b)
			}
			// add the string representation to the names list
			names = append(names, string(name))
			// add a proper bindvar for the bindType
			switch bindType {
			// oracle only supports named type bind vars even for positional
			case NAMED:
				rebound = append(rebound, ':')
				rebound = append(rebound, name...)
			case QUESTION, UNKNOWN:
				rebound = append(rebound, '?')
			case DOLLAR:
				rebound = append(rebound, '$')
				for _, b := range strconv.Itoa(currentVar) {
					rebound = append(rebound, byte(b))
				}
				currentVar++
			case AT:
				rebound = append(rebound, '@', 'p')
				for _, b := range strconv.Itoa(currentVar) {
					rebound = append(rebound, byte(b))
				}
				currentVar++
			}
			// add this byte to string unless it was not part of the name
			if i != last {
				rebound = append(rebound, b)
			} else if !unicode.IsOneOf(allowedBindRunes, rune(b)) {
				rebound = append(rebound, b)
			}
		} else {
			// this is a normal byte and should just go onto the rebound query
			rebound = append(rebound, b)
		}
	}

	return string(rebound), names, err
}

// BindNamed binds a struct or a map to a query with named parameters.
// DEPRECATED: use sqlx.Named` instead of this, it may be removed in future.
func BindNamed(bindType int, query string, arg any) (string, []any, error) {
	return bindNamedMapper(bindType, query, arg, mapper())
}

// Named takes a query using named parameters and an argument and
// returns a new query with a list of args that can be executed by
// a database.  The return value uses the `?` bindvar.
func Named(query string, arg any) (string, []any, error) {
	return bindNamedMapper(QUESTION, query, arg, mapper())
}

// NamedDollar takes a query using named parameters and an argument and
// returns a new query with a list of args that can be executed by
// a database.  The return value uses the `$` bindvar.
func NamedDollar(query string, arg any) (string, []any, error) {
	return bindNamedMapper(DOLLAR, query, arg, mapper())
}

func bindNamedMapper(bindType int, query string, arg any, m *reflectx.Mapper) (string, []any, error) {
	t := reflect.TypeOf(arg)
	k := t.Kind()
	switch {
	case k == reflect.Map && t.Key().Kind() == reflect.String:
		m, ok := convertMapStringInterface(arg)
		if !ok {
			return "", nil, fmt.Errorf("sqlx.bindNamedMapper: unsupported map type: %T", arg)
		}
		return bindMap(bindType, query, m)
	case k == reflect.Array || k == reflect.Slice:
		return bindArray(bindType, query, arg, m)
	default:
		return bindStruct(bindType, query, arg, m)
	}
}

// NamedQuery binds a named query and then runs Query on the result using the
// provided Ext (sqlx.Tx, sqlx.Db).  It works with both structs and with
// map[string]any types.
func NamedQuery(e Ext, query string, arg any) (*Rows, error) {
	query, err := SanitizeQuery(query, arg)
	if err != nil {
		return nil, err
	}
	matches := InReg.FindAllStringSubmatch(query, -1)
	if len(matches) > 0 {
		return NamedIn(e, query, arg)
	}
	q, args, err := bindNamedMapper(BindType(e.DriverName()), query, arg, mapperFor(e))
	if err != nil {
		return nil, err
	}
	return e.Queryx(q, args...)
}

// NamedExec uses BindStruct to get a query executable by the driver and
// then runs Exec on the result.  Returns an error from the binding
// or the query execution itself.
func NamedExec(e Ext, query string, arg any) (sql.Result, error) {
	query, err := SanitizeQuery(query, arg)
	if err != nil {
		return nil, err
	}
	query, arg = prepareNamedInQuery(query, arg)
	q, args, err := bindNamedMapper(BindType(e.DriverName()), query, arg, mapperFor(e))
	if err != nil {
		return nil, err
	}
	return e.Exec(q, args...)
}

var (
	InReg = regexp.MustCompile(`IN\s*?\(\s*?((:\w+)|(\?))\s*?\)`)
)

// NamedIn expands slice values in args, returning the modified query string
// and a new arg list that can be executed by a database. The `query` should
// use the `?` bindVar.  The return value uses the `?` bindVar.
func NamedIn(e Ext, query string, args any) (*Rows, error) {
	query, err := SanitizeQuery(query, args)
	if err != nil {
		return nil, err
	}
	query, args = prepareNamedInQuery(query, args)
	q, p, err := bindNamedMapper(BindType(e.DriverName()), query, args, mapperFor(e))
	if err != nil {
		return nil, err
	}

	return e.Queryx(q, p...)
}

func prepareNamedInQuery(query string, args any) (string, any) {
	matches := InReg.FindAllStringSubmatch(query, -1)
	switch args := args.(type) {
	case map[string]any:
		for _, match := range matches {
			key := strings.TrimPrefix(match[1], ":")
			switch reflect.TypeOf(args[key]).Kind() {
			case reflect.Slice:
				s := reflect.ValueOf(args[key])
				var keys []string
				for i := 0; i < s.Len(); i++ {
					keyToStore := fmt.Sprintf("%s_%d", key, i)
					args[keyToStore] = s.Index(i).Interface()
					keys = append(keys, ":"+keyToStore)
				}
				keyReplace := strings.Join(keys, ",")
				query = strings.ReplaceAll(query, match[1], keyReplace)
			}
		}
	}
	return query, args
}
