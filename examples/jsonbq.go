package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/oarkflow/squealx/jsonbq"
)

// Example domain models
type Athlete struct {
	ID        int       `db:"id"`
	Data      string    `db:"data"` // JSONB column
	CreatedAt time.Time `db:"created_at"`
}

type AthleteData struct {
	Name   string         `json:"name"`
	Email  string         `json:"email,omitempty"`
	Sport  string         `json:"sport"`
	Age    int            `json:"age"`
	Stats  map[string]any `json:"stats"`
	Active bool           `json:"active"`
}

type DecryptedAthleteContact struct {
	Email string `json:"email"`
	Sport string `json:"sport"`
}

func main() {
	// Connect to database
	db := jsonbq.MustOpen("postgres://postgres:postgres@localhost/pipeline_platform?sslmode=disable",
		"data", // JSONB column name
		"test",
	)
	db.EnableEncryptedMode("demo-encryption-key", "demo-hmac-key")
	db.EnableEncryptedIntegrityAutoRepair() // use EnableEncryptedIntegrityStrict() to fail writes on mismatch
	db.EncryptFields("email:search")
	defer db.Close()

	ctx := context.Background()

	// Ensure example table exists with a JSONB column
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS athletes (
			id BIGSERIAL PRIMARY KEY,
			data JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Migration strategy for encrypted mode: ensure columns, extension, and backfill.
	err = db.MigrateEncryptedMode("athletes")
	if err != nil {
		log.Fatal(err)
	}

	health, err := db.CheckEncryptedHealth("athletes")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Encrypted health -> total=%d missing_encrypted=%d missing_hmac=%d mismatches=%d\n",
		health.TotalRows, health.MissingEncrypted, health.MissingHMAC, health.HMACMismatchCount)

	err = db.DefaultJSONBIndexesFor("athletes",
		"sport",
		"name",
		"active:bool",
		"age:int",
		"stats.height:int",
		"stats.ppg:numeric",
	)
	if err != nil {
		log.Fatal(err)
	}

	// Optional deterministic cleanup for repeated runs.
	if shouldResetData() {
		_, err = db.Exec("TRUNCATE TABLE athletes RESTART IDENTITY")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Reset enabled: truncated athletes table")
	}

	heightExpr := jsonbq.At("stats", "height").Int()
	ppgExpr := jsonbq.At("stats", "ppg").Numeric()
	ageExpr := jsonbq.At("age").Int()
	activeExpr := jsonbq.At("active").Bool()

	// ========================================
	// INSERT Examples
	// ========================================

	fmt.Println("=== INSERT ===")

	// Simple insert
	athleteData := AthleteData{
		Name:   "LeBron James",
		Email:  "lebron@example.com",
		Sport:  "Basketball",
		Age:    39,
		Active: true,
		Stats: map[string]any{
			"height": 206,
			"weight": 113,
			"ppg":    25.7,
		},
	}

	var insertedID int
	err = db.Insert("athletes").
		Data(athleteData).
		Returning("id").
		Get(&insertedID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Inserted athlete with ID: %d\n", insertedID)

	// Batch insert
	athletes := []any{
		AthleteData{Name: "Stephen Curry", Email: "steph@example.com", Sport: "Basketball", Age: 35, Active: true},
		AthleteData{Name: "Kevin Durant", Email: "kd@example.com", Sport: "Basketball", Age: 35, Active: true},
		AthleteData{Name: "Giannis Antetokounmpo", Email: "giannis@example.com", Sport: "Basketball", Age: 29, Active: true},
	}

	var ids []struct{ ID int `db:"id"` }
	err = db.BatchInsert("athletes").
		Data(athletes).
		Returning("id").
		Select(&ids)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Batch inserted %d athletes\n", len(ids))

	// ========================================
	// SELECT Examples
	// ========================================

	fmt.Println("\n=== SELECT ===")

	// Simple field query
	var basketballPlayers []Athlete
	err = db.Query().
		Select("id", "data", "created_at").
		From("athletes").
		Where(jsonbq.At("sport").Eq("Basketball")).
		Exec(&basketballPlayers)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d basketball players\n", len(basketballPlayers))

	// Nested path query
	var tallAthletes []Athlete
	err = db.Query().
		From("athletes").
		Where(
			heightExpr.Gt(200),
		).
		Exec(&tallAthletes)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d tall athletes (>200cm)\n", len(tallAthletes))

	// Multiple conditions with AND
	var specificAthletes []Athlete
	err = db.Query().
		From("athletes").
		Where(
			jsonbq.At("sport").Eq("Basketball"),
			activeExpr.Eq(true),
			ppgExpr.Gt(20),
		).
		OrderByDesc("data->>'name'").
		Limit(10).
		Exec(&specificAthletes)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d specific athletes\n", len(specificAthletes))

	// OR conditions
	var multiSportAthletes []Athlete
	err = db.Query().
		From("athletes").
		Where(
			jsonbq.Or(
				jsonbq.At("sport").Eq("Basketball"),
				jsonbq.At("sport").Eq("Football"),
			),
		).
		Exec(&multiSportAthletes)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d athletes in basketball or football\n", len(multiSportAthletes))

	// Complex logical combinations
	var complexQuery []Athlete
	err = db.Query().
		From("athletes").
		Where(
			jsonbq.And(
				activeExpr.Eq(true),
				jsonbq.Or(
					heightExpr.Gt(200),
					ppgExpr.Gt(25),
				),
			),
		).
		Exec(&complexQuery)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d athletes matching complex criteria\n", len(complexQuery))

	// IN query
	var inQuery []Athlete
	err = db.Query().
		From("athletes").
		Where(
			jsonbq.At("sport").In("Basketball", "Football", "Baseball"),
		).
		Exec(&inQuery)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d athletes in multiple sports\n", len(inQuery))

	// Containment query (@>)
	var containsQuery []Athlete
	err = db.Query().
		From("athletes").
		Where(
			jsonbq.Contains(map[string]any{
				"sport":  "Basketball",
				"active": true,
			}),
		).
		Exec(&containsQuery)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d athletes with exact match\n", len(containsQuery))

	// Key existence
	var hasStatsAthletes []Athlete
	err = db.Query().
		From("athletes").
		Where(jsonbq.HasKey("stats")).
		Exec(&hasStatsAthletes)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d athletes with stats key\n", len(hasStatsAthletes))

	// Count
	count, err := db.Query().
		From("athletes").
		Where(jsonbq.At("sport").Eq("Basketball")).
		Count()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Total basketball players: %d\n", count)

	// Exists
	exists, err := db.Query().
		From("athletes").
		Where(jsonbq.At("name").Eq("LeBron James")).
		Exists()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("LeBron exists: %v\n", exists)

	// Search encrypted email via blind index helper (_secure_idx.email)
	emailCond, err := db.SearchableEquals("email", "lebron@example.com")
	if err != nil {
		log.Fatal(err)
	}
	emailExists, err := db.Query().
		From("athletes").
		Where(emailCond).
		Exists()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("LeBron email exists (blind index): %v\n", emailExists)

	// Get single row
	var singleAthlete Athlete
	err = db.Query().
		From("athletes").
		Where(jsonbq.At("name").Eq("Stephen Curry")).
		Get(&singleAthlete)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found athlete ID: %d\n", singleAthlete.ID)

	decryptedEmail, err := db.DecryptFieldFromJSON(singleAthlete.Data, "email")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Decrypted email for Stephen Curry: %v\n", decryptedEmail)

	decryptedFields, err := db.DecryptFieldsFromJSON(singleAthlete.Data, "email", "sport")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Decrypted fields (email,sport): %v\n", decryptedFields)

	var decryptedContact DecryptedAthleteContact
	err = db.DecryptIntoStructFromJSON(singleAthlete.Data, &decryptedContact, "email", "sport")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Decrypted typed contact: %+v\n", decryptedContact)

	var decryptRows []Athlete
	decryptedRows, err := db.Query().
		Select("id", "data", "created_at").
		From("athletes").
		Where(jsonbq.At("name").Eq("Stephen Curry")).
		DecryptSelect(&decryptRows, "email", "sport")
	if err != nil {
		log.Fatal(err)
	}
	if len(decryptedRows) > 0 {
		fmt.Printf("DecryptSelect helper result: %v\n", decryptedRows[0])
	}

	var decryptTypedRows []Athlete
	decryptedTypedRows, err := jsonbq.DecryptSelectTyped[DecryptedAthleteContact](db.Query().
		Select("id", "data", "created_at").
		From("athletes").
		Where(jsonbq.At("name").Eq("Stephen Curry")), &decryptTypedRows, "email", "sport")
	if err != nil {
		log.Fatal(err)
	}
	if len(decryptedTypedRows) > 0 {
		fmt.Printf("DecryptSelectTyped helper result: %+v\n", decryptedTypedRows[0])
	}

	// Pagination demo: seed 100 records and fetch one page
	pagingSeed := make([]any, 0, 100)
	for i := 1; i <= 100; i++ {
		pagingSeed = append(pagingSeed, AthleteData{
			Name:   fmt.Sprintf("Page Player %03d", i),
			Email:  fmt.Sprintf("page.player.%03d@example.com", i),
			Sport:  "Basketball",
			Age:    18 + (i % 20),
			Active: i%2 == 0,
			Stats: map[string]any{
				"height": 180 + (i % 30),
				"weight": 70 + (i % 25),
				"ppg":    8 + float64(i%30)/2,
			},
		})
	}

	_, err = db.BatchInsert("athletes").
		Data(pagingSeed).
		Exec()
	if err != nil {
		log.Fatal(err)
	}

	typedPagedResponse := jsonbq.PaginateTypedResponse[Athlete](db.Query().
		Select("id", "data", "created_at").
		From("athletes").
		Where(jsonbq.At("name").ILike("Page Player %")).
		OrderByAsc("id"), 2, 10)
	if typedPagedResponse.Error != nil {
		log.Fatal(typedPagedResponse.Error)
	}
	fmt.Printf("Pagination test -> page=%d limit=%d total=%d prev=%d next=%d items=%d\n",
		typedPagedResponse.Pagination.Page,
		typedPagedResponse.Pagination.Limit,
		typedPagedResponse.Pagination.TotalRecords,
		typedPagedResponse.Pagination.PrevPage,
		typedPagedResponse.Pagination.NextPage,
		len(typedPagedResponse.Items),
	)

	// ========================================
	// UPDATE Examples
	// ========================================

	fmt.Println("\n=== UPDATE ===")

	// Update nested field
	result, err := db.Update("athletes").
		Set(
			jsonbq.Set("stats", "ppg").To(26.5),
			jsonbq.Set("stats", "updated").To(time.Now()),
		).
		Where(jsonbq.At("name").Eq("LeBron James")).
		Exec()
	if err != nil {
		log.Fatal(err)
	}
	rowsAffected, _ := result.RowsAffected()
	fmt.Printf("Updated %d rows\n", rowsAffected)

	// Replace entire data
	newData := AthleteData{
		Name:   "LeBron James",
		Sport:  "Basketball",
		Age:    40,
		Active: true,
		Stats: map[string]any{
			"height": 206,
			"weight": 113,
			"ppg":    27.0,
		},
	}
	_, err = db.Update("athletes").
		SetData(newData).
		Where(jsonbq.At("name").Eq("LeBron James")).
		Exec()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Replaced entire athlete data")

	// Update with RETURNING
	var updatedAthletes []Athlete
	err = db.Update("athletes").
		Set(jsonbq.Set("active").To(false)).
		Where(ageExpr.Gt(35)).
		Returning("id", "data").
		Select(&updatedAthletes)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Deactivated %d athletes over 35\n", len(updatedAthletes))

	// ========================================
	// DELETE Examples
	// ========================================

	fmt.Println("\n=== DELETE ===")

	// Simple delete
	result, err = db.Delete("athletes").
		Where(activeExpr.Eq(false)).
		Exec()
	if err != nil {
		log.Fatal(err)
	}
	rowsAffected, _ = result.RowsAffected()
	fmt.Printf("Deleted %d inactive athletes\n", rowsAffected)

	// Delete with RETURNING
	var deletedAthletes []Athlete
	err = db.Delete("athletes").
		Where(ppgExpr.Lt(10)).
		Returning("id", "data").
		Select(&deletedAthletes)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Deleted %d low-scoring athletes\n", len(deletedAthletes))

	// ========================================
	// REMOVE Fields Examples
	// ========================================

	fmt.Println("\n=== REMOVE FIELDS ===")

	// Remove top-level key
	_, err = db.Remove("athletes").
		Fields(jsonbq.RemoveKey("temporary")).
		Where(jsonbq.At("name").Eq("LeBron James")).
		Exec()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Removed temporary field")

	// Remove nested path
	_, err = db.Remove("athletes").
		Fields(jsonbq.RemovePath("stats", "updated")).
		Where(jsonbq.At("sport").Eq("Basketball")).
		Exec()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Removed stats.updated from all basketball players")

	// ========================================
	// TRANSACTION Examples
	// ========================================

	fmt.Println("\n=== TRANSACTIONS ===")

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Insert in transaction
	var txID int
	err = tx.Insert("athletes").
		Data(AthleteData{Name: "New Player", Sport: "Basketball", Age: 25, Active: true}).
		Returning("id").
		Get(&txID)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}

	// Update in transaction
	_, err = tx.Update("athletes").
		Set(jsonbq.Set("verified").To(true)).
		Where(jsonbq.At("name").Eq("New Player")).
		Exec()
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}

	// Query in transaction
	var txAthletes []Athlete
	err = tx.Query().
		From("athletes").
		Where(jsonbq.At("verified").Bool().Eq(true)).
		Exec(&txAthletes)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Transaction committed with %d verified athletes\n", len(txAthletes))

	// ========================================
	// Advanced Examples
	// ========================================

	fmt.Println("\n=== ADVANCED ===")

	// LIKE query
	var likeQuery []Athlete
	err = db.Query().
		From("athletes").
		Where(jsonbq.At("name").ILike("%james%")).
		Exec(&likeQuery)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d athletes with 'james' in name\n", len(likeQuery))

	// NOT condition
	var notQuery []Athlete
	err = db.Query().
		From("athletes").
		Where(
			jsonbq.Not(jsonbq.At("sport").Eq("Basketball")),
		).
		Exec(&notQuery)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d non-basketball athletes\n", len(notQuery))

	// IsNull / IsNotNull
	var hasStatsQuery []Athlete
	err = db.Query().
		From("athletes").
		Where(jsonbq.At("stats").JSON().IsNotNull()).
		Exec(&hasStatsQuery)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d athletes with stats\n", len(hasStatsQuery))

	// Group By and Having (requires regular columns)
	type SportCount struct {
		Sport string `db:"sport"`
		Count int    `db:"count"`
	}
	var sportCounts []SportCount
	err = db.Query().
		Select("data->>'sport' as sport", "COUNT(*) as count").
		From("athletes").
		GroupBy("data->>'sport'").
		Having(jsonbq.Raw("COUNT(*)").Gt(5)).
		Exec(&sportCounts)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d sports with >5 athletes\n", len(sportCounts))

	fmt.Println("\n=== Examples completed successfully ===")
}

func shouldResetData() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("JSONBQ_RESET")))
	if v == "" {
		return true
	}
	if v == "0" || v == "false" || v == "no" || v == "n" {
		return false
	}
	return v == "1" || v == "true" || v == "yes" || v == "y"
}
