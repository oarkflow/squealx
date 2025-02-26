package sqlstr

import (
	"regexp"
	"strings"
	"unicode"
)

var (
	qRE1     = regexp.MustCompile(`/\*(.*)\*/|\-\-(.*)`)
	qRE2     = regexp.MustCompile(`\s+`)
	qRE3     = regexp.MustCompile(`\s'(.*?)'|\s(true|TRUE)|\s(false|FALSE)|\s[0-9]+\.[0-9]+|\s[0-9]+`)
	indexRE  = regexp.MustCompile("from([^()]*?)(left|inner|right|outer|full)|from([^()]*?)join|from([^()]*?)where|from([^()]*?);|from([^()]*?)$")
	insertRE = regexp.MustCompile("insert(.*?)into")
	deleteRE = regexp.MustCompile("delete(.*?)from")
)

func Clean(query string) string {
	query = qRE1.ReplaceAllString(query, "")
	query = qRE2.ReplaceAllString(query, " ")
	return strings.TrimSpace(query)
}

func Obscure(query string) string {
	query = qRE3.ReplaceAllString(query, " ?")
	return query
}

func TableNames(query string) []string {
	return newQueryString(query).TableNames()
}

type queryString struct {
	query   string
	lowered string
}

func RemoveRepeatedAndEmpty(arr []string) (newArr []string) {
	newArr = make([]string, 0)
	for i := 0; i < len(arr); i++ {
		repeat := false
		if len(arr[i]) > 0 {
			for j := i + 1; j < len(arr); j++ {
				if arr[i] == arr[j] {
					repeat = true
					break
				}
			}
			if !repeat {
				newArr = append(newArr, arr[i])
			}
		}
	}
	return newArr
}

func newQueryString(query string) *queryString {
	query = Clean(query)
	return &queryString{
		query:   query,
		lowered: strings.ToLower(query),
	}
}

func (p queryString) After(word string) string {
	iWord := strings.Index(p.lowered, strings.ToLower(word)) + len(word) + 1
	return p.after(iWord)
}

func (p queryString) AfterAll(word string) (atAfters []string) {
	indices := regexp.MustCompile(strings.ToLower(word)).FindAllStringIndex(p.lowered, -1)
	for _, index := range indices {
		atAfters = append(atAfters, p.after(index[1]))
	}
	return
}

func (p queryString) TableNames() (names []string) {
	firstSyntax := p.lowered[:strings.IndexRune(p.lowered, ' ')]
	switch firstSyntax {
	case "update":
		names = append(names, cleanName(p.After("update")))
		return
	case "insert":
		index := insertRE.FindStringIndex(p.lowered)
		names = append(names, cleanName(p.after(index[1])))
		return
	case "delete":
		index := deleteRE.FindStringIndex(p.lowered)
		names = append(names, cleanName(p.after(index[1])))
		return
	}
	names = append(names, p.tableNamesByFROM()...)
	names = append(names, p.AfterAll("join[^(]")...)
	names = RemoveRepeatedAndEmpty(names)
	return
}

func (p queryString) tableNamesByFROM() (names []string) {
	indices := indexRE.FindAllStringIndex(p.lowered, -1)
	for _, index := range indices {
		fromStmt := p.lowered[index[0]:index[1]]
		lastSyntax := fromStmt[strings.LastIndex(fromStmt, " ")+1:]
		var tableStmt string
		if lastSyntax == "from" || lastSyntax == "where" || lastSyntax == "left" ||
			lastSyntax == "right" || lastSyntax == "join" || lastSyntax == "inner" ||
			lastSyntax == "outer" || lastSyntax == "full" {
			tableStmt = p.query[index[0]+len("from")+1 : index[1]-len(lastSyntax)-1]
		} else {
			tableStmt = p.query[index[0]+len("from")+1:]
		}

		for _, name := range strings.Split(tableStmt, ",") {
			names = append(names, cleanName(name))
		}
	}
	return
}

func cleanName(name string) string {
	name = strings.Fields(name)[0]
	name = strings.TrimSpace(name)
	lastRune := name[len(name)-1]
	if lastRune == ';' {
		name = name[:len(name)-1]
	}
	return name
}

func (p queryString) after(iWord int) (atAfter string) {
	iAfter := 0
	for i := iWord; i < len(p.lowered); i++ {
		r := rune(p.lowered[i])
		if unicode.IsLetter(r) && iAfter <= 0 {
			iAfter = i
		}
		if (unicode.IsSpace(r) || (unicode.IsPunct(r) && r != '.' && r != '_')) && iAfter > 0 {
			atAfter = p.query[iAfter:i]
			if strings.ToLower(atAfter) == "select" {
				atAfter = ""
				return
			} else {
				break
			}
		}
	}
	if atAfter == "" {
		atAfter = p.query[iAfter:]
	}
	return
}
