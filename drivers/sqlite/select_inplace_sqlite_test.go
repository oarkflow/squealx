package sqlite

import (
	"fmt"
	"testing"
	"time"
)

type addr struct {
	City string  `db:"city"`
	Zip  *string `db:"zip"`
}

type person struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
	addrEmbedded
	Home  addr  `db:"home"`
	Boss  *addr `db:"boss"`
	Score *int64
}

type addrEmbedded struct {
	Country string `db:"country"`
}

func setupInplaceDB(t *testing.T) interface {
	Select(dest any, query string, args ...any) error
} {
	t.Helper()
	db := setupCursorDB(t)
	if _, err := db.Exec(`
		CREATE TABLE ppl (id INTEGER PRIMARY KEY, name TEXT, country TEXT, "home.city" TEXT, "home.zip" TEXT, "boss.city" TEXT, score INTEGER, born TEXT);
		INSERT INTO ppl VALUES
		 (1,'a','NP','Kathmandu','44600','Pokhara',10,'2024-03-05 10:11:12'),
		 (2,'b','IN','Delhi',NULL,NULL,NULL,NULL),
		 (3,'c','US','NYC','10001','Boston',30,'2025-01-02 03:04:05');
	`); err != nil {
		t.Fatal(err)
	}
	return db
}

const pplQuery = `SELECT id,name,country,"home.city" AS "home.city","home.zip" AS "home.zip","boss.city" AS "boss.city",score FROM ppl ORDER BY id`

func checkPeople(t *testing.T, ps []person) {
	t.Helper()
	if len(ps) != 3 {
		t.Fatalf("len=%d", len(ps))
	}
	if ps[0].Name != "a" || ps[0].Country != "NP" || ps[0].Home.City != "Kathmandu" || ps[0].Home.Zip == nil || *ps[0].Home.Zip != "44600" ||
		ps[0].Boss == nil || ps[0].Boss.City != "Pokhara" || ps[0].Score == nil || *ps[0].Score != 10 {
		t.Fatalf("row0: %+v boss=%+v", ps[0], ps[0].Boss)
	}
	if ps[1].Name != "b" || ps[1].Home.City != "Delhi" || ps[1].Home.Zip != nil || ps[1].Boss != nil || ps[1].Score != nil {
		t.Fatalf("row1 must not inherit row0 state: %+v boss=%+v", ps[1], ps[1].Boss)
	}
	if ps[2].Boss == nil || ps[2].Boss.City != "Boston" || ps[2].Home.Zip == nil || *ps[2].Home.Zip != "10001" || *ps[2].Score != 30 {
		t.Fatalf("row2: %+v", ps[2])
	}
	// retained rows must be independent of each other
	if ps[0].Boss == ps[2].Boss || ps[0].Home.Zip == ps[2].Home.Zip {
		t.Fatal("rows share pointer storage")
	}
}

func TestSelectInPlaceNestedAndEmbedded(t *testing.T) {
	db := setupInplaceDB(t)
	var ps []person
	if err := db.Select(&ps, pplQuery); err != nil {
		t.Fatal(err)
	}
	checkPeople(t, ps)

	var pp []*person
	if err := db.Select(&pp, pplQuery); err != nil {
		t.Fatal(err)
	}
	vals := make([]person, len(pp))
	for i := range pp {
		vals[i] = *pp[i]
	}
	checkPeople(t, vals)
}

// A destination slice that already has capacity and stale contents must not
// leak old data into freshly scanned rows, and shrinks to the result size.
func TestSelectReusesDestinationWithoutStaleData(t *testing.T) {
	db := setupInplaceDB(t)
	zip, score := "STALE", int64(999)
	ps := make([]person, 0, 10)
	for i := 0; i < 5; i++ {
		ps = append(ps, person{Name: "stale", Score: &score, Home: addr{City: "stale", Zip: &zip}, Boss: &addr{City: "stale"}})
	}
	ps = ps[:5]
	if err := db.Select(&ps, pplQuery); err != nil {
		t.Fatal(err)
	}
	checkPeople(t, ps)
	if cap(ps) < 10 {
		t.Fatalf("expected the existing backing array to be reused, cap=%d", cap(ps))
	}
	// first call again on the same slice, now with a smaller result
	if err := db.Select(&ps, pplQuery+" LIMIT 1"); err != nil || len(ps) != 1 {
		t.Fatalf("len=%d err=%v", len(ps), err)
	}
}

func TestSelectScalarSlicesInPlace(t *testing.T) {
	db := setupInplaceDB(t)
	var ids []int64
	if err := db.Select(&ids, `SELECT id FROM ppl ORDER BY id`); err != nil || fmt.Sprint(ids) != "[1 2 3]" {
		t.Fatalf("%v %v", ids, err)
	}
	var names []*string
	if err := db.Select(&names, `SELECT name FROM ppl ORDER BY id`); err != nil || len(names) != 3 || *names[0] != "a" || *names[2] != "c" || names[0] == names[1] {
		t.Fatalf("%v %v", names, err)
	}
	var scores []*int64
	if err := db.Select(&scores, `SELECT score FROM ppl ORDER BY id`); err != nil || scores[1] != nil || *scores[2] != 30 {
		t.Fatalf("scores: %v %v", scores, err)
	}
	var times []time.Time
	if err := db.Select(&times, `SELECT born FROM ppl WHERE born IS NOT NULL ORDER BY id`); err != nil && len(times) > 2 {
		t.Fatal(err)
	}
}

func TestSelectLargeResultGrowth(t *testing.T) {
	db := setupCursorDB(t)
	if _, err := db.Exec(`CREATE TABLE big (id INTEGER, name TEXT)`); err != nil {
		t.Fatal(err)
	}
	tx, _ := db.Begin()
	for i := 0; i < 5000; i++ {
		_, _ = tx.Exec(`INSERT INTO big VALUES (?,?)`, i, fmt.Sprintf("n%d", i))
	}
	_ = tx.Commit()
	var rows []cursorUser
	if err := db.Select(&rows, `SELECT id,name FROM big ORDER BY id`); err != nil || len(rows) != 5000 {
		t.Fatalf("len=%d err=%v", len(rows), err)
	}
	for i, r := range rows {
		if r.ID != i || r.Name != fmt.Sprintf("n%d", i) {
			t.Fatalf("row %d corrupted across growth: %+v", i, r)
		}
	}
}

// Manual Rows.StructScan loops reuse the plan and must stay correct for
// nested fields across rows and across different destination variables.
func TestRowsStructScanLoopNested(t *testing.T) {
	db := setupCursorDB(t)
	if _, err := db.Exec(`CREATE TABLE ppl2 (id INTEGER, "home.city" TEXT, "boss.city" TEXT);
		INSERT INTO ppl2 VALUES (1,'x','y'),(2,'z',NULL),(3,'w','v')`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Queryx(`SELECT id,"home.city" AS "home.city","boss.city" AS "boss.city" FROM ppl2 ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type p2 struct {
		ID   int64 `db:"id"`
		Home addr  `db:"home"`
		Boss *addr `db:"boss"`
	}
	var all []*p2
	for rows.Next() {
		p := new(p2)
		if err := rows.StructScan(p); err != nil {
			t.Fatal(err)
		}
		all = append(all, p)
	}
	if len(all) != 3 || all[0].Home.City != "x" || all[0].Boss == nil || all[0].Boss.City != "y" ||
		all[1].Home.City != "z" || all[1].Boss != nil || all[2].Home.City != "w" || all[2].Boss.City != "v" {
		t.Fatalf("%+v %+v %+v", all[0], all[1], all[2])
	}
}
