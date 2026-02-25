package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/jsonbq"
)

const (
	defaultJSONBQDSNEnv   = "JSONBQ_DSN"
	defaultJSONBQDSNValue = "postgres://postgres:postgres@localhost/processgate_demo?sslmode=disable"
	encryptionKey         = "demo-encryption-key"
	hmacKey               = "demo-hmac-key"

	providersTable = "jsonbq_demo_adv_providers"
	patientsTable  = "jsonbq_demo_adv_patients"
	entriesTable   = "jsonbq_demo_adv_entries"
	claimsTable    = "jsonbq_demo_adv_claims"
	paymentsTable  = "jsonbq_demo_adv_payments"
)

type ProviderSummaryRow struct {
	ProviderID    int64     `db:"provider_id"`
	ProviderName  string    `db:"provider_name"`
	ClaimCount    int       `db:"claim_count"`
	BilledTotal   float64   `db:"billed_total"`
	PaidTotal     float64   `db:"paid_total"`
	UrgentClaims  int       `db:"urgent_claims"`
	LatestClaimAt time.Time `db:"latest_claim_at"`
}

type ClaimSettlementRow struct {
	ClaimID           int64   `db:"claim_id"`
	ClaimStatus       string  `db:"claim_status"`
	ClaimAmount       float64 `db:"claim_amount"`
	PaidAmount        float64 `db:"paid_amount"`
	SettlementState   string  `db:"settlement_state"`
	ProcedureCode     string  `db:"procedure_code"`
	ProcedureCategory string  `db:"procedure_category"`
}

type PatientRankRow struct {
	PatientID   int64   `db:"patient_id"`
	PatientName string  `db:"patient_name"`
	TotalClaims int     `db:"total_claims"`
	BilledTotal float64 `db:"billed_total"`
	SpendRank   int     `db:"spend_rank"`
}

type EncryptedPatientRow struct {
	ID   int64  `db:"id"`
	Data string `db:"data"`
}

type DeepJSONCheckRow struct {
	ClaimsWithDeepArray   int `db:"claims_with_deep_array"`
	PatientsWithDeepPath  int `db:"patients_with_deep_path"`
	PatientsWithDeepArray int `db:"patients_with_deep_array"`
}

type baseIDs struct {
	ProviderIDs []int64
	PatientIDs  []int64
	EntryIDs    []int64
}

func main() {
	db := openDB()
	defer db.Close()

	tenantID, err := setupDemo(db)
	if err != nil {
		log.Fatal(err)
	}

	if err := runQuerySuite(db, tenantID); err != nil {
		log.Fatal(err)
	}
	if err := runDeepNestedUpdateExamples(db, tenantID); err != nil {
		log.Fatal(err)
	}
	if err := verifyDeepNestedExamples(db, tenantID); err != nil {
		log.Fatal(err)
	}

	fmt.Println("All JSONB + relational demo queries executed successfully.")
}

func openDB() *jsonbq.DB {
	dsn := os.Getenv(defaultJSONBQDSNEnv)
	if dsn == "" {
		dsn = defaultJSONBQDSNValue
	}
	db := jsonbq.MustOpen(dsn, "data", "jsonbq-report-minimal")
	db.EnableEncryptedMode(encryptionKey, hmacKey)
	db.EnableEncryptedIntegrityAutoRepair() // use EnableEncryptedIntegrityStrict() to fail writes on mismatch
	db.EncryptFields("email:search")
	return db
}

func setupDemo(db *jsonbq.DB) (int64, error) {
	tenantID := time.Now().UnixNano()
	if err := ensureSchema(db); err != nil {
		return 0, err
	}
	ids, err := seedBaseData(db, tenantID)
	if err != nil {
		return 0, err
	}
	if err := seedClaimsAndPayments(db, tenantID, ids); err != nil {
		return 0, err
	}
	if err := setupEncryptedMode(db); err != nil {
		return 0, err
	}
	if err := encryptPatientEmails(db, tenantID); err != nil {
		return 0, err
	}
	if err := verifyEncryptedPatientLookup(db, tenantID); err != nil {
		return 0, err
	}
	return tenantID, nil
}

func setupEncryptedMode(db *jsonbq.DB) error {
	tables := []string{
		providersTable,
		patientsTable,
		entriesTable,
		claimsTable,
	}
	for _, table := range tables {
		if err := db.MigrateEncryptedMode(table); err != nil {
			return err
		}
	}
	if err := db.EnsureEncryptedFieldIndexes(patientsTable); err != nil {
		return err
	}
	return nil
}

func ensureSchema(db *jsonbq.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ` + providersTable + ` (
			id BIGSERIAL PRIMARY KEY,
			tenant_id BIGINT NOT NULL,
			data JSONB NOT NULL DEFAULT jsonb_build_object()
		)`,
		`CREATE TABLE IF NOT EXISTS ` + patientsTable + ` (
			id BIGSERIAL PRIMARY KEY,
			tenant_id BIGINT NOT NULL,
			data JSONB NOT NULL DEFAULT jsonb_build_object()
		)`,
		`CREATE TABLE IF NOT EXISTS ` + entriesTable + ` (
			id BIGSERIAL PRIMARY KEY,
			tenant_id BIGINT NOT NULL,
			patient_id BIGINT NOT NULL REFERENCES ` + patientsTable + `(id),
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			data JSONB NOT NULL DEFAULT jsonb_build_object()
		)`,
		`CREATE TABLE IF NOT EXISTS ` + claimsTable + ` (
			id BIGSERIAL PRIMARY KEY,
			tenant_id BIGINT NOT NULL,
			provider_id BIGINT NOT NULL REFERENCES ` + providersTable + `(id),
			patient_id BIGINT NOT NULL REFERENCES ` + patientsTable + `(id),
			entry_id BIGINT NOT NULL REFERENCES ` + entriesTable + `(id),
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			data JSONB NOT NULL DEFAULT jsonb_build_object()
		)`,
		`CREATE TABLE IF NOT EXISTS ` + paymentsTable + ` (
			id BIGSERIAL PRIMARY KEY,
			tenant_id BIGINT NOT NULL,
			claim_id BIGINT NOT NULL REFERENCES ` + claimsTable + `(id),
			amount NUMERIC(12,2) NOT NULL,
			paid_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			status TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func seedBaseData(db *jsonbq.DB, tenantID int64) (*baseIDs, error) {
	var providerIDs []int64
	if err := runPositionalSelect(db, &providerIDs, `
		INSERT INTO `+providersTable+` (tenant_id, data)
		VALUES
			($1, jsonb_build_object('name','Acme Health','region','west')),
			($1, jsonb_build_object('name','Beacon Labs','region','east'))
		RETURNING id
	`, tenantID); err != nil {
		return nil, err
	}

	var patientIDs []int64
	if err := runPositionalSelect(db, &patientIDs, `
		INSERT INTO `+patientsTable+` (tenant_id, data)
		VALUES
			($1, jsonb_build_object(
				'name','John Carter',
				'risk_score','82',
				'profile', jsonb_build_object(
					'coverage', jsonb_build_object(
						'policy', jsonb_build_object(
							'plan', jsonb_build_object(
								'tier','gold',
								'limits', jsonb_build_object('annual','5000')
							)
						)
					)
				)
			)),
			($1, jsonb_build_object(
				'name','Diana Prince',
				'risk_score','61',
				'care_team', jsonb_build_array(
					jsonb_build_object(
						'role','pcp',
						'contacts', jsonb_build_array(
							jsonb_build_object('type','phone','value','555-0101'),
							jsonb_build_object('type','pager','value','P-889')
						)
					)
				)
			)),
			($1, jsonb_build_object('name','Bruce Wayne','risk_score','45'))
		RETURNING id
	`, tenantID); err != nil {
		return nil, err
	}

	var entryIDs []int64
	if err := runPositionalSelect(db, &entryIDs, `
		INSERT INTO `+entriesTable+` (tenant_id, patient_id, created_at, data)
		VALUES
			($1, $2, NOW() - INTERVAL '3 days', jsonb_build_object('source','portal')),
			($1, $3, NOW() - INTERVAL '2 days', jsonb_build_object('source','api')),
			($1, $4, NOW() - INTERVAL '1 day', jsonb_build_object('source','batch'))
		RETURNING id
	`, tenantID, patientIDs[0], patientIDs[1], patientIDs[2]); err != nil {
		return nil, err
	}

	return &baseIDs{ProviderIDs: providerIDs, PatientIDs: patientIDs, EntryIDs: entryIDs}, nil
}

func seedClaimsAndPayments(db *jsonbq.DB, tenantID int64, ids *baseIDs) error {
	var claimIDs []int64
	if err := runPositionalSelect(db, &claimIDs, `
		INSERT INTO `+claimsTable+` (tenant_id, provider_id, patient_id, entry_id, created_at, data)
		VALUES
			($1, $2, $4, $7, NOW() - INTERVAL '3 days',
				jsonb_build_object(
					'status','approved',
					'amount','120.50',
					'urgent',true,
					'procedure',jsonb_build_object('code','99213','category','office'),
					'clinical', jsonb_build_object(
						'encounter', jsonb_build_object(
							'provider', jsonb_build_object(
								'department', jsonb_build_object(
									'unit', jsonb_build_object(
										'room', jsonb_build_object(
											'bed', jsonb_build_object('label','A-17')
										)
									)
								)
							)
						)
					),
					'audit', jsonb_build_object(
						'journey', jsonb_build_object(
							'steps', jsonb_build_array(
								jsonb_build_object(
									'name','ingest',
									'events', jsonb_build_array(
										jsonb_build_object(
											'ts','2026-01-01T09:00:00Z',
											'meta', jsonb_build_object(
												'code','M1',
												'actor', jsonb_build_object('id','svc-1')
											)
										),
										jsonb_build_object(
											'ts','2026-01-01T09:05:00Z',
											'meta', jsonb_build_object(
												'code','M2',
												'actor', jsonb_build_object('id','svc-2')
											)
										)
									)
								)
							)
						)
					)
				)),
			($1, $2, $5, $8, NOW() - INTERVAL '2 days',
				jsonb_build_object('status','paid','amount','79.25','urgent',false,'procedure',jsonb_build_object('code','93000','category','diagnostic'))),
			($1, $3, $6, $9, NOW() - INTERVAL '1 day',
				jsonb_build_object('status','approved','amount','33.00','urgent',true,'procedure',jsonb_build_object('code','80053','category','lab')))
		RETURNING id
	`, tenantID,
		ids.ProviderIDs[0], ids.ProviderIDs[1],
		ids.PatientIDs[0], ids.PatientIDs[1], ids.PatientIDs[2],
		ids.EntryIDs[0], ids.EntryIDs[1], ids.EntryIDs[2],
	); err != nil {
		return err
	}

	_, err := db.Exec(`
		INSERT INTO `+paymentsTable+` (tenant_id, claim_id, amount, paid_at, status)
		VALUES
			($1, $2, 100.00, NOW() - INTERVAL '2 days', 'posted'),
			($1, $2, 20.50, NOW() - INTERVAL '1 day', 'posted'),
			($1, $3, 79.25, NOW() - INTERVAL '1 day', 'posted'),
			($1, $4, 10.00, NOW(), 'pending')
	`, tenantID, claimIDs[0], claimIDs[1], claimIDs[2])
	return err
}

func runQuerySuite(db *jsonbq.DB, tenantID int64) error {
	from := time.Now().Add(-14 * 24 * time.Hour)
	to := time.Now()

	providerSQL := `
WITH eligible_claims AS (
	SELECT
		c.id,
		c.provider_id,
		c.patient_id,
		c.created_at,
		c.data.amount AS claim_amount,
		c.data.procedure.code AS procedure_code,
		CASE WHEN c.data.urgent = 'true' THEN 'urgent' ELSE 'routine' END AS urgency
	FROM ` + claimsTable + ` c
	WHERE c.tenant_id = :tenant_id
	  AND c.created_at BETWEEN :from_ts AND :to_ts
	  AND c.data.status IN ('approved', 'paid')
),
payment_rollup AS (
	SELECT p.claim_id, SUM(p.amount) AS paid_total, MAX(p.paid_at) AS last_paid_at
	FROM ` + paymentsTable + ` p
	WHERE p.tenant_id = :tenant_id
	GROUP BY p.claim_id
)
SELECT
	pr.id AS provider_id,
	pr.data.name AS provider_name,
	COUNT(*) AS claim_count,
	COALESCE(SUM(ec.claim_amount), 0) AS billed_total,
	COALESCE(SUM(COALESCE(py.paid_total, 0)), 0) AS paid_total,
	SUM(CASE WHEN ec.urgency = 'urgent' THEN 1 ELSE 0 END) AS urgent_claims,
	MAX(ec.created_at) AS latest_claim_at
FROM eligible_claims ec
JOIN ` + providersTable + ` pr ON pr.id = ec.provider_id AND pr.tenant_id = :tenant_id
LEFT JOIN payment_rollup py ON py.claim_id = ec.id
GROUP BY pr.id, pr.data.name
HAVING COUNT(*) > :min_claims
ORDER BY paid_total DESC, provider_name ASC
LIMIT :limit_rows
`
	var providerRows []ProviderSummaryRow
	err := runNormalSelect(db, &providerRows, providerSQL, map[string]any{
		"tenant_id":  tenantID,
		"from_ts":    from,
		"to_ts":      to,
		"min_claims": 0,
		"limit_rows": 50,
	})
	if err != nil {
		return fmt.Errorf("provider summary query failed: %w", err)
	}
	if len(providerRows) == 0 {
		return fmt.Errorf("provider summary query returned no rows")
	}
	fmt.Printf("Q1 Provider Summary (%d rows)\n", len(providerRows))
	for _, row := range providerRows {
		fmt.Printf("%+v\n", row)
	}

	settlementSQL := `
SELECT
	c.id AS claim_id,
	c.data.status AS claim_status,
	c.data.amount AS claim_amount,
	(
		SELECT COALESCE(SUM(p.amount), 0)
		FROM ` + paymentsTable + ` p
		WHERE p.claim_id = c.id
	) AS paid_amount,
	IF(
		(
			SELECT COALESCE(SUM(p.amount), 0)
			FROM ` + paymentsTable + ` p
			WHERE p.claim_id = c.id
		) >= c.data.amount,
		'settled',
		'open'
	) AS settlement_state,
	c.data.procedure.code AS procedure_code,
	c.data.procedure.category AS procedure_category
FROM ` + claimsTable + ` c
WHERE c.tenant_id = :tenant_id
ORDER BY c.id
LIMIT :limit_rows
`
	var settlementRows []ClaimSettlementRow
	err = runNormalSelect(db, &settlementRows, settlementSQL, map[string]any{
		"tenant_id":  tenantID,
		"limit_rows": 100,
	})
	if err != nil {
		return fmt.Errorf("claim settlement query failed: %w", err)
	}
	if len(settlementRows) == 0 {
		return fmt.Errorf("claim settlement query returned no rows")
	}
	fmt.Printf("Q2 Claim Settlement (%d rows)\n", len(settlementRows))
	for _, row := range settlementRows {
		fmt.Printf("%+v\n", row)
	}

	rankSQL := `
SELECT * FROM (
	SELECT
		pt.id AS patient_id,
		pt.data.name AS patient_name,
		COUNT(c.id) AS total_claims,
		COALESCE(SUM(c.data.amount), 0) AS billed_total,
		RANK() OVER (
			ORDER BY COALESCE(SUM(c.data.amount), 0) DESC
		) AS spend_rank
	FROM ` + patientsTable + ` pt
	LEFT JOIN ` + claimsTable + ` c
		ON c.patient_id = pt.id AND c.tenant_id = pt.tenant_id
	WHERE pt.tenant_id = :tenant_id
	  AND EXISTS (
		SELECT 1
		FROM ` + entriesTable + ` e
		WHERE e.patient_id = pt.id AND e.tenant_id = pt.tenant_id
	  )
	GROUP BY pt.id, pt.data.name
) ranked
WHERE ranked.spend_rank <= :top_n
ORDER BY ranked.spend_rank, ranked.patient_id
`
	var rankRows []PatientRankRow
	err = runNormalSelect(db, &rankRows, rankSQL, map[string]any{
		"tenant_id": tenantID,
		"top_n":     10,
	})
	if err != nil {
		return fmt.Errorf("patient ranking query failed: %w", err)
	}
	if len(rankRows) == 0 {
		return fmt.Errorf("patient ranking query returned no rows")
	}
	fmt.Printf("Q3 Patient Ranking (%d rows)\n", len(rankRows))
	for _, row := range rankRows {
		fmt.Printf("%+v\n", row)
	}

	return nil
}

func encryptPatientEmails(db *jsonbq.DB, tenantID int64) error {
	type patientEmail struct {
		name  string
		email string
	}
	patientEmails := []patientEmail{
		{name: "John Carter", email: "john.carter@demo.test"},
		{name: "Diana Prince", email: "diana.prince@demo.test"},
		{name: "Bruce Wayne", email: "bruce.wayne@demo.test"},
	}

	for _, item := range patientEmails {
		res, err := db.Update(patientsTable).
			Set(jsonbq.Set("email").To(item.email)).
			Where(
				jsonbq.Col("tenant_id").Eq(tenantID),
				jsonbq.At("name").Eq(item.name),
			).
			Exec()
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			return fmt.Errorf("expected one updated patient for %q, got %d", item.name, affected)
		}
	}
	return nil
}

func verifyEncryptedPatientLookup(db *jsonbq.DB, tenantID int64) error {
	var rows []EncryptedPatientRow
	if err := runNormalSelect(db, &rows, `
		SELECT id, data
		FROM `+patientsTable+` p
		WHERE tenant_id = :tenant_id
		  AND p.data.email = :email
	`, map[string]any{
		"tenant_id": tenantID,
		"email":     "john.carter@demo.test",
	}); err != nil {
		return err
	}
	if len(rows) != 1 {
		return fmt.Errorf("expected exactly one patient row by encrypted email lookup, got %d", len(rows))
	}

	email, err := db.DecryptFieldFromJSON(rows[0].Data, "email")
	if err != nil {
		return err
	}
	if email != "john.carter@demo.test" {
		return fmt.Errorf("unexpected decrypted email: %v", email)
	}

	health, err := db.CheckEncryptedHealth(patientsTable)
	if err != nil {
		return err
	}
	fmt.Printf("Encrypted mode ok: table=%s total=%d missing_encrypted=%d missing_hmac=%d mismatches=%d\n",
		patientsTable, health.TotalRows, health.MissingEncrypted, health.MissingHMAC, health.HMACMismatchCount)
	fmt.Printf("Encrypted email lookup ok: patient_id=%d email=%v\n", rows[0].ID, email)
	return nil
}

func runNormalSelect(db *jsonbq.DB, dest any, query string, vars map[string]any) error {
	sqlText, args, err := db.ParseNormalSQL(query, vars)
	if err != nil {
		return err
	}
	// Use positional query execution directly to avoid accidental named-query
	// detection on PostgreSQL casts like ::jsonb / ::text[].
	if err := squealx.Select(db.DB, dest, sqlText, args...); err != nil {
		return err
	}
	return nil
}

func runDeepNestedUpdateExamples(db *jsonbq.DB, tenantID int64) error {
	// Nested object update via fluent update builder.
	objRes, err := db.Update(patientsTable).
		Set(jsonbq.Set("profile.coverage.policy.plan.limits.annual").To("7500")).
		Where(
			jsonbq.Col("tenant_id").Eq(tenantID),
			jsonbq.At("name").Eq("John Carter"),
		).
		Exec()
	if err != nil {
		return err
	}
	objAffected, err := objRes.RowsAffected()
	if err != nil {
		return err
	}
	if objAffected != 1 {
		return fmt.Errorf("expected one deep object update row, got %d", objAffected)
	}

	// Nested array-of-objects update with SQL transform (no fixed array indexes).
	arrRes, err := runNormalExec(db, `
		UPDATE `+patientsTable+` p
		SET data = jsonb_set(
			p.data,
			ARRAY['care_team']::text[],
			COALESCE((
					SELECT jsonb_agg(
						jsonb_set(
							team.data,
							ARRAY['contacts']::text[],
						COALESCE((
							SELECT jsonb_agg(
								CASE
									WHEN contact.data.type = 'phone' THEN
										jsonb_set(contact.data, ARRAY['verified']::text[], to_jsonb('true'::text), true)
									ELSE contact.data
								END
							)
							FROM jsonb_array_elements(COALESCE(team.data.contacts::jsonb, '[]'::jsonb)) AS contact(data)
						), '[]'::jsonb),
						true
					)
				)
				FROM jsonb_array_elements(COALESCE(p.data.care_team::jsonb, '[]'::jsonb)) AS team(data)
			), '[]'::jsonb),
			true
		)
		WHERE p.tenant_id = :tenant_id
		  AND p.data.name = 'Diana Prince'
	`, map[string]any{
		"tenant_id": tenantID,
	})
	if err != nil {
		return err
	}
	arrAffected, err := arrRes.RowsAffected()
	if err != nil {
		return err
	}
	if arrAffected != 1 {
		return fmt.Errorf("expected one deep array update row, got %d", arrAffected)
	}

	fmt.Printf("Q4 Deep nested updates ok: object_rows=%d array_rows=%d\n", objAffected, arrAffected)
	return nil
}

func verifyDeepNestedExamples(db *jsonbq.DB, tenantID int64) error {
	var rows []DeepJSONCheckRow
	err := runNormalSelect(db, &rows, `
		SELECT
			(
				SELECT COUNT(DISTINCT c.id)
				FROM `+claimsTable+` c
				JOIN LATERAL jsonb_array_elements(COALESCE(c.data.audit.journey.steps::jsonb, '[]'::jsonb)) AS step(data) ON TRUE
				JOIN LATERAL jsonb_array_elements(COALESCE(step.data.events::jsonb, '[]'::jsonb)) AS event(data) ON TRUE
				WHERE c.tenant_id = :tenant_id
				  AND c.data.clinical.encounter.provider.department.unit.room.bed.label IS NOT NULL
				  AND event.data.meta.code IS NOT NULL
			) AS claims_with_deep_array,
			COUNT(DISTINCT p.id) FILTER (
				WHERE p.data.profile.coverage.policy.plan.limits.annual IS NOT NULL
			) AS patients_with_deep_path,
				COUNT(DISTINCT p.id) FILTER (
					WHERE EXISTS (
						SELECT 1
						FROM jsonb_array_elements(COALESCE(p.data.care_team::jsonb, '[]'::jsonb)) AS team(data)
						JOIN LATERAL jsonb_array_elements(COALESCE(team.data.contacts::jsonb, '[]'::jsonb)) AS contact(data) ON TRUE
						WHERE contact.data.type IS NOT NULL
						  AND contact.data.verified = 'true'
					)
				) AS patients_with_deep_array
		FROM `+patientsTable+` p
		WHERE p.tenant_id = :tenant_id
		`, map[string]any{
		"tenant_id": tenantID,
	})
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return fmt.Errorf("expected one deep-json verification row, got %d", len(rows))
	}
	if rows[0].ClaimsWithDeepArray == 0 {
		return fmt.Errorf("no claim rows found with deep nested array-of-objects payload")
	}
	if rows[0].PatientsWithDeepPath == 0 {
		return fmt.Errorf("no patient rows found with deep nested object payload")
	}
	if rows[0].PatientsWithDeepArray == 0 {
		return fmt.Errorf("no patient rows found with deep nested array-of-objects payload")
	}
	fmt.Printf("Q5 Deep Nested JSON checks ok: claims=%d patients_with_object=%d patients_with_array=%d\n",
		rows[0].ClaimsWithDeepArray, rows[0].PatientsWithDeepPath, rows[0].PatientsWithDeepArray)
	return nil
}

func runNormalExec(db *jsonbq.DB, query string, vars map[string]any) (sql.Result, error) {
	sqlText, args, err := db.ParseNormalSQL(query, vars)
	if err != nil {
		return nil, err
	}
	res, err := db.Exec(sqlText, args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func runPositionalSelect(db *jsonbq.DB, dest any, query string, args ...any) error {
	return squealx.Select(db.DB, dest, query, args...)
}
