package jsonbq

type SQLExpr interface {
	isSQLExpr()
}

type StatementAST struct {
	SelectItems []SQLExpr
	Where       SQLExpr
	Having      SQLExpr
	GroupBy     []SQLExpr
	OrderBy     []SQLExpr
	Limit       SQLExpr
	Offset      SQLExpr
	Returning   []SQLExpr
	Assignments []SetAssignment
	Values      [][]SQLExpr
}

type SetAssignment struct {
	Column SQLExpr
	Value  SQLExpr
}

type IdentifierExpr struct {
	Name string
}

func (IdentifierExpr) isSQLExpr() {}

type LiteralExpr struct {
	Value string
	Kind  string // string, number, keyword, unknown
}

func (LiteralExpr) isSQLExpr() {}

type PlaceholderExpr struct {
	Raw  string
	Kind string
	Key  string
}

func (PlaceholderExpr) isSQLExpr() {}

type UnaryExpr struct {
	Op   string
	Expr SQLExpr
}

func (UnaryExpr) isSQLExpr() {}

type BinaryExpr struct {
	Left  SQLExpr
	Op    string
	Right SQLExpr
}

func (BinaryExpr) isSQLExpr() {}

type FunctionCallExpr struct {
	Name string
	Args []SQLExpr
}

func (FunctionCallExpr) isSQLExpr() {}

type CaseWhenBranch struct {
	Cond SQLExpr
	Then SQLExpr
}

type CaseExpr struct {
	Operand SQLExpr
	When    []CaseWhenBranch
	Else    SQLExpr
}

func (CaseExpr) isSQLExpr() {}

type CastExpr struct {
	Expr SQLExpr
	Type string
}

func (CastExpr) isSQLExpr() {}

type AliasExpr struct {
	Expr  SQLExpr
	Alias string
}

func (AliasExpr) isSQLExpr() {}

type TupleExpr struct {
	Items []SQLExpr
}

func (TupleExpr) isSQLExpr() {}
