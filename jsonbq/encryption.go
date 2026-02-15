package jsonbq

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

type encryptedIntegrityMode string

const (
	encryptedIntegrityOff       encryptedIntegrityMode = "off"
	encryptedIntegrityStrict    encryptedIntegrityMode = "strict"
	encryptedIntegrityAutoRepair encryptedIntegrityMode = "auto_repair"
)

type encryptedModeConfig struct {
	EncryptedColumn string
	HMACColumn      string
	EncryptionKey   string
	HMACKey         string
	IntegrityMode   encryptedIntegrityMode
	FieldSpecs      map[string]bool // dot path -> searchable
}

// EncryptionHealth summarizes encrypted shadow-column integrity for a table.
type EncryptionHealth struct {
	TotalRows         int64 `json:"total_rows"`
	MissingEncrypted  int64 `json:"missing_encrypted"`
	MissingHMAC       int64 `json:"missing_hmac"`
	HMACMismatchCount int64 `json:"hmac_mismatch_count"`
}

// EnableEncryptedMode enables dual-write mode: plain JSON in data column for queries,
// plus encrypted payload and HMAC for secure retrieval/integrity checks.
func (db *DB) EnableEncryptedMode(encryptionKey, hmacKey string) *DB {
	return db.EnableEncryptedModeWithColumns(encryptionKey, hmacKey, "encrypted_data", "data_hmac")
}

// EnableEncryptedModeWithColumns enables encrypted mode with custom shadow column names.
func (db *DB) EnableEncryptedModeWithColumns(encryptionKey, hmacKey, encryptedColumn, hmacColumn string) *DB {
	encryptionKey = strings.TrimSpace(encryptionKey)
	hmacKey = strings.TrimSpace(hmacKey)
	encryptedColumn = strings.TrimSpace(encryptedColumn)
	hmacColumn = strings.TrimSpace(hmacColumn)
	if encryptedColumn == "" {
		encryptedColumn = "encrypted_data"
	}
	if hmacColumn == "" {
		hmacColumn = "data_hmac"
	}

	db.encrypted = &encryptedModeConfig{
		EncryptedColumn: encryptedColumn,
		HMACColumn:      hmacColumn,
		EncryptionKey:   encryptionKey,
		HMACKey:         hmacKey,
		IntegrityMode:   encryptedIntegrityOff,
		FieldSpecs:      map[string]bool{},
	}
	return db
}

// EncryptFields enables field-level encryption inside JSON data.
// Spec format:
//   "name"          -> encrypt field
//   "email:search"  -> encrypt field and add blind index under _secure_idx.email
// Nested paths use dot notation (for example: "profile.email:search").
func (db *DB) EncryptFields(specs ...string) *DB {
	if db.encrypted == nil {
		return db
	}
	if db.encrypted.FieldSpecs == nil {
		db.encrypted.FieldSpecs = map[string]bool{}
	}
	for _, spec := range specs {
		path, searchable, ok := parseEncryptedFieldSpec(spec)
		if !ok {
			continue
		}
		db.encrypted.FieldSpecs[path] = searchable
	}
	return db
}

// EnableEncryptedIntegrityStrict blocks writes when encrypted shadow data is inconsistent.
func (db *DB) EnableEncryptedIntegrityStrict() *DB {
	if db.encrypted != nil {
		db.encrypted.IntegrityMode = encryptedIntegrityStrict
	}
	return db
}

// EnableEncryptedIntegrityAutoRepair auto-repairs encrypted shadow data before writes.
func (db *DB) EnableEncryptedIntegrityAutoRepair() *DB {
	if db.encrypted != nil {
		db.encrypted.IntegrityMode = encryptedIntegrityAutoRepair
	}
	return db
}

// DisableEncryptedIntegrityGuard disables strict/repair pre-write checks.
func (db *DB) DisableEncryptedIntegrityGuard() *DB {
	if db.encrypted != nil {
		db.encrypted.IntegrityMode = encryptedIntegrityOff
	}
	return db
}

// DisableEncryptedMode turns off encrypted shadow writes.
func (db *DB) DisableEncryptedMode() *DB {
	db.encrypted = nil
	return db
}

// MigrateEncryptedMode ensures schema columns/extensions and backfills encrypted data.
func (db *DB) MigrateEncryptedMode(table string) error {
	return db.MigrateEncryptedModeContext(context.Background(), table)
}

// MigrateEncryptedModeContext ensures schema columns/extensions and backfills encrypted data.
func (db *DB) MigrateEncryptedModeContext(ctx context.Context, table string) error {
	if err := db.requireEncryptedMode(); err != nil {
		return err
	}
	if strings.TrimSpace(table) == "" {
		return fmt.Errorf("table is required")
	}

	// pgcrypto is required for pgp_sym_encrypt + hmac.
	if _, err := db.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS pgcrypto`); err != nil {
		return err
	}

	alterSQL := fmt.Sprintf(
		`ALTER TABLE %s
		 ADD COLUMN IF NOT EXISTS %s BYTEA,
		 ADD COLUMN IF NOT EXISTS %s TEXT`,
		quoteIdentifier(table), quoteIdentifier(db.encrypted.EncryptedColumn), quoteIdentifier(db.encrypted.HMACColumn),
	)
	if _, err := db.ExecContext(ctx, alterSQL); err != nil {
		return err
	}

	backfillSQL := fmt.Sprintf(
		`UPDATE %s
		 SET %s = pgp_sym_encrypt((%s)::text, $1),
		     %s = encode(hmac((%s)::text, $2, 'sha256'), 'hex')
		 WHERE %s IS NOT NULL
		   AND (%s IS NULL OR %s IS NULL)`,
		quoteIdentifier(table),
		quoteIdentifier(db.encrypted.EncryptedColumn), quoteIdentifier(db.columnName),
		quoteIdentifier(db.encrypted.HMACColumn), quoteIdentifier(db.columnName),
		quoteIdentifier(db.columnName),
		quoteIdentifier(db.encrypted.EncryptedColumn), quoteIdentifier(db.encrypted.HMACColumn),
	)
	if _, err := db.ExecContext(ctx, backfillSQL, db.encrypted.EncryptionKey, db.encrypted.HMACKey); err != nil {
		return err
	}

	hmacIndexSQL := fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS %s ON %s ((%s))`,
		quoteIdentifier("idx_"+sanitizeIndexPart(table)+"_"+sanitizeIndexPart(db.encrypted.HMACColumn)),
		quoteIdentifier(table), quoteIdentifier(db.encrypted.HMACColumn),
	)
	if _, err := db.ExecContext(ctx, hmacIndexSQL); err != nil {
		return err
	}

	return db.EnsureEncryptedFieldIndexesContext(ctx, table)
}

// CheckEncryptedHealth computes encrypted-mode integrity statistics.
func (db *DB) CheckEncryptedHealth(table string) (*EncryptionHealth, error) {
	return db.CheckEncryptedHealthContext(context.Background(), table)
}

// CheckEncryptedHealthContext computes encrypted-mode integrity statistics.
func (db *DB) CheckEncryptedHealthContext(ctx context.Context, table string) (*EncryptionHealth, error) {
	if err := db.requireEncryptedMode(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(table) == "" {
		return nil, fmt.Errorf("table is required")
	}

	query := fmt.Sprintf(
		`SELECT
			COUNT(*) AS total_rows,
			COUNT(*) FILTER (WHERE %s IS NULL) AS missing_encrypted,
			COUNT(*) FILTER (WHERE %s IS NULL OR %s = '') AS missing_hmac,
			COUNT(*) FILTER (
				WHERE %s IS NOT NULL
				  AND %s IS NOT NULL
				  AND %s <> encode(hmac((%s)::text, $1, 'sha256'), 'hex')
			) AS hmac_mismatch_count
		 FROM %s`,
		quoteIdentifier(db.encrypted.EncryptedColumn),
		quoteIdentifier(db.encrypted.HMACColumn), quoteIdentifier(db.encrypted.HMACColumn),
		quoteIdentifier(db.encrypted.EncryptedColumn),
		quoteIdentifier(db.encrypted.HMACColumn),
		quoteIdentifier(db.encrypted.HMACColumn), quoteIdentifier(db.columnName),
		quoteIdentifier(table),
	)

	var health EncryptionHealth
	if err := db.GetContext(ctx, &health, query, db.encrypted.HMACKey); err != nil {
		return nil, err
	}
	return &health, nil
}

// RepairEncryptedData backfills/repairs encrypted shadow columns for missing/mismatched rows.
func (db *DB) RepairEncryptedData(table string) error {
	return db.RepairEncryptedDataContext(context.Background(), table)
}

// RepairEncryptedDataContext backfills/repairs encrypted shadow columns for missing/mismatched rows.
func (db *DB) RepairEncryptedDataContext(ctx context.Context, table string) error {
	if err := db.requireEncryptedMode(); err != nil {
		return err
	}
	if strings.TrimSpace(table) == "" {
		return fmt.Errorf("table is required")
	}

	repairSQL := fmt.Sprintf(
		`UPDATE %s
		 SET %s = pgp_sym_encrypt((%s)::text, $1),
		     %s = encode(hmac((%s)::text, $2, 'sha256'), 'hex')
		 WHERE %s IS NOT NULL
		   AND (
			%s IS NULL
			OR %s IS NULL
			OR %s = ''
			OR %s <> encode(hmac((%s)::text, $2, 'sha256'), 'hex')
		   )`,
		quoteIdentifier(table),
		quoteIdentifier(db.encrypted.EncryptedColumn), quoteIdentifier(db.columnName),
		quoteIdentifier(db.encrypted.HMACColumn), quoteIdentifier(db.columnName),
		quoteIdentifier(db.columnName),
		quoteIdentifier(db.encrypted.EncryptedColumn),
		quoteIdentifier(db.encrypted.HMACColumn),
		quoteIdentifier(db.encrypted.HMACColumn),
		quoteIdentifier(db.encrypted.HMACColumn), quoteIdentifier(db.columnName),
	)

	_, err := db.ExecContext(ctx, repairSQL, db.encrypted.EncryptionKey, db.encrypted.HMACKey)
	return err
}

// RotateEncryptedModeKeys rotates encryption and HMAC keys by regenerating shadow columns from data.
func (db *DB) RotateEncryptedModeKeys(table, newEncryptionKey, newHMACKey string) error {
	return db.RotateEncryptedModeKeysContext(context.Background(), table, newEncryptionKey, newHMACKey)
}

// RotateEncryptedModeKeysContext rotates encryption and HMAC keys with context.
func (db *DB) RotateEncryptedModeKeysContext(ctx context.Context, table, newEncryptionKey, newHMACKey string) error {
	if err := db.requireEncryptedMode(); err != nil {
		return err
	}
	if strings.TrimSpace(table) == "" {
		return fmt.Errorf("table is required")
	}
	newEncryptionKey = strings.TrimSpace(newEncryptionKey)
	newHMACKey = strings.TrimSpace(newHMACKey)
	if newEncryptionKey == "" || newHMACKey == "" {
		return fmt.Errorf("new encryption and hmac keys are required")
	}

	rotateSQL := fmt.Sprintf(
		`UPDATE %s
		 SET %s = pgp_sym_encrypt((%s)::text, $1),
		     %s = encode(hmac((%s)::text, $2, 'sha256'), 'hex')
		 WHERE %s IS NOT NULL`,
		quoteIdentifier(table),
		quoteIdentifier(db.encrypted.EncryptedColumn), quoteIdentifier(db.columnName),
		quoteIdentifier(db.encrypted.HMACColumn), quoteIdentifier(db.columnName),
		quoteIdentifier(db.columnName),
	)
	if _, err := db.ExecContext(ctx, rotateSQL, newEncryptionKey, newHMACKey); err != nil {
		return err
	}

	db.encrypted.EncryptionKey = newEncryptionKey
	db.encrypted.HMACKey = newHMACKey
	return nil
}

func (db *DB) requireEncryptedMode() error {
	if db.encrypted == nil {
		return fmt.Errorf("encrypted mode is not enabled")
	}
	if strings.TrimSpace(db.encrypted.EncryptionKey) == "" {
		return fmt.Errorf("encryption key is required")
	}
	if strings.TrimSpace(db.encrypted.HMACKey) == "" {
		return fmt.Errorf("hmac key is required")
	}
	if strings.TrimSpace(db.encrypted.EncryptedColumn) == "" || strings.TrimSpace(db.encrypted.HMACColumn) == "" {
		return fmt.Errorf("encrypted mode columns are required")
	}
	return nil
}

// BlindIndex computes the searchable HMAC value for a plaintext field value.
// Use with encrypted searchable fields (for example _secure_idx.email).
func (db *DB) BlindIndex(value any) (string, error) {
	if err := db.requireEncryptedMode(); err != nil {
		return "", err
	}
	plainBytes, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	h := hmac.New(sha256.New, []byte(db.encrypted.HMACKey))
	h.Write(plainBytes)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// SearchableEquals creates a condition for encrypted searchable field lookup.
// path uses dot notation (for example: "email" or "profile.email").
func (db *DB) SearchableEquals(path string, value any) (Condition, error) {
	if err := db.requireEncryptedMode(); err != nil {
		return nil, err
	}
	normalized := normalizeDotPath(path)
	if normalized == "" {
		return nil, fmt.Errorf("path is required")
	}
	idx, err := db.BlindIndex(value)
	if err != nil {
		return nil, err
	}
	return At("_secure_idx", strings.ReplaceAll(normalized, ".", "_")).Eq(idx), nil
}

// EnsureEncryptedFieldIndexes creates expression indexes for searchable encrypted field blind indexes.
func (db *DB) EnsureEncryptedFieldIndexes(table string) error {
	return db.EnsureEncryptedFieldIndexesContext(context.Background(), table)
}

// EnsureEncryptedFieldIndexesContext creates expression indexes for searchable encrypted field blind indexes.
func (db *DB) EnsureEncryptedFieldIndexesContext(ctx context.Context, table string) error {
	if err := db.requireEncryptedMode(); err != nil {
		return err
	}
	table = strings.TrimSpace(table)
	if table == "" {
		return fmt.Errorf("table is required")
	}

	for dotPath, searchable := range db.encrypted.FieldSpecs {
		if !searchable {
			continue
		}
		key := strings.ReplaceAll(dotPath, ".", "_")
		idxName := "idx_" + sanitizeIndexPart(table) + "_secure_idx_" + sanitizeIndexPart(key)
		idxExpr := At("_secure_idx", key).Text()
		if err := db.AddIndexContext(ctx, table, Index(idxName, idxExpr)); err != nil {
			return err
		}
	}

	return nil
}

type encryptedIntegrityExecutor interface {
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func ensureEncryptedIntegrityBeforeWrite(ctx context.Context, exec encryptedIntegrityExecutor, table, columnName string, cfg *encryptedModeConfig) error {
	if cfg == nil {
		return nil
	}
	if cfg.IntegrityMode == encryptedIntegrityOff {
		return nil
	}
	if strings.TrimSpace(table) == "" {
		return fmt.Errorf("table is required for encrypted integrity checks")
	}

	checkQuery := fmt.Sprintf(
		`SELECT
			COUNT(*) AS total_rows,
			COUNT(*) FILTER (WHERE %s IS NULL) AS missing_encrypted,
			COUNT(*) FILTER (WHERE %s IS NULL OR %s = '') AS missing_hmac,
			COUNT(*) FILTER (
				WHERE %s IS NOT NULL
				  AND %s IS NOT NULL
				  AND %s <> encode(hmac((%s)::text, $1, 'sha256'), 'hex')
			) AS hmac_mismatch_count
		 FROM %s`,
		quoteIdentifier(cfg.EncryptedColumn),
		quoteIdentifier(cfg.HMACColumn), quoteIdentifier(cfg.HMACColumn),
		quoteIdentifier(cfg.EncryptedColumn),
		quoteIdentifier(cfg.HMACColumn),
		quoteIdentifier(cfg.HMACColumn), quoteIdentifier(columnName),
		quoteIdentifier(table),
	)

	var health EncryptionHealth
	if err := exec.GetContext(ctx, &health, checkQuery, cfg.HMACKey); err != nil {
		return err
	}

	issues := health.MissingEncrypted + health.MissingHMAC + health.HMACMismatchCount
	if issues == 0 {
		return nil
	}

	if cfg.IntegrityMode == encryptedIntegrityStrict {
		return fmt.Errorf("encrypted integrity check failed for table %s: missing_encrypted=%d missing_hmac=%d hmac_mismatch=%d", table, health.MissingEncrypted, health.MissingHMAC, health.HMACMismatchCount)
	}

	if cfg.IntegrityMode == encryptedIntegrityAutoRepair {
		repairSQL := fmt.Sprintf(
			`UPDATE %s
			 SET %s = pgp_sym_encrypt((%s)::text, $1),
			     %s = encode(hmac((%s)::text, $2, 'sha256'), 'hex')
			 WHERE %s IS NOT NULL
			   AND (
				%s IS NULL
				OR %s IS NULL
				OR %s = ''
				OR %s <> encode(hmac((%s)::text, $2, 'sha256'), 'hex')
			   )`,
			quoteIdentifier(table),
			quoteIdentifier(cfg.EncryptedColumn), quoteIdentifier(columnName),
			quoteIdentifier(cfg.HMACColumn), quoteIdentifier(columnName),
			quoteIdentifier(columnName),
			quoteIdentifier(cfg.EncryptedColumn),
			quoteIdentifier(cfg.HMACColumn),
			quoteIdentifier(cfg.HMACColumn),
			quoteIdentifier(cfg.HMACColumn), quoteIdentifier(columnName),
		)
		if _, err := exec.ExecContext(ctx, repairSQL, cfg.EncryptionKey, cfg.HMACKey); err != nil {
			return err
		}
	}

	return nil
}

func encryptionSetClause(q *Query, dataExpr string, cfg *encryptedModeConfig) string {
	encExpr, hmacExpr := encryptionValueExprs(q, dataExpr, cfg)
	return quoteIdentifier(cfg.EncryptedColumn) + " = " + encExpr + ", " +
		quoteIdentifier(cfg.HMACColumn) + " = " + hmacExpr
}

func encryptionValueExprs(q *Query, dataExpr string, cfg *encryptedModeConfig) (string, string) {
	encKey := q.addArg(cfg.EncryptionKey)
	hmacKey := q.addArg(cfg.HMACKey)
	encExpr := "pgp_sym_encrypt((" + dataExpr + ")::text, " + encKey + ")"
	hmacExpr := "encode(hmac((" + dataExpr + ")::text, " + hmacKey + ", 'sha256'), 'hex')"
	return encExpr, hmacExpr
}

func parseEncryptedFieldSpec(spec string) (string, bool, bool) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return "", false, false
	}
	parts := strings.SplitN(spec, ":", 2)
	path := normalizeDotPath(parts[0])
	if path == "" {
		return "", false, false
	}
	searchable := false
	if len(parts) == 2 {
		mode := strings.ToLower(strings.TrimSpace(parts[1]))
		searchable = mode == "search" || mode == "indexed" || mode == "hmac"
	}
	return path, searchable, true
}

func normalizeDotPath(path string) string {
	parts := splitDotPath(path)
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ".")
}

func splitDotPath(path string) []string {
	raw := strings.Split(strings.TrimSpace(path), ".")
	parts := make([]string, 0, len(raw))
	for _, p := range raw {
		p = strings.TrimSpace(p)
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

func (cfg *encryptedModeConfig) hasFieldEncryption() bool {
	return cfg != nil && len(cfg.FieldSpecs) > 0
}

func (cfg *encryptedModeConfig) fieldSpecForPath(path []string) (searchable bool, ok bool) {
	if cfg == nil || len(cfg.FieldSpecs) == 0 {
		return false, false
	}
	searchable, ok = cfg.FieldSpecs[strings.Join(path, ".")]
	return searchable, ok
}

func (cfg *encryptedModeConfig) transformDataForFieldEncryption(data any) (any, error) {
	if !cfg.hasFieldEncryption() {
		return data, nil
	}
	payload, err := anyToMap(data)
	if err != nil {
		return nil, err
	}

	for dotPath, searchable := range cfg.FieldSpecs {
		path := splitDotPath(dotPath)
		if len(path) == 0 {
			continue
		}
		value, ok := getNested(payload, path)
		if !ok {
			continue
		}
		encryptedValue, indexValue, err := cfg.encryptFieldValue(value)
		if err != nil {
			return nil, err
		}
		setNested(payload, path, encryptedValue)
		if searchable {
			setNested(payload, []string{"_secure_idx", strings.ReplaceAll(dotPath, ".", "_")}, indexValue)
		}
	}

	return payload, nil
}

func (cfg *encryptedModeConfig) encryptFieldValue(value any) (string, string, error) {
	plainBytes, err := json.Marshal(value)
	if err != nil {
		return "", "", err
	}

	key := sha256.Sum256([]byte(cfg.EncryptionKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return "", "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", "", err
	}
	ciphertext := gcm.Seal(nil, nonce, plainBytes, nil)
	payload := append(nonce, ciphertext...)
	enc := "enc:v1:" + base64.StdEncoding.EncodeToString(payload)

	h := hmac.New(sha256.New, []byte(cfg.HMACKey))
	h.Write(plainBytes)
	idx := hex.EncodeToString(h.Sum(nil))

	return enc, idx, nil
}

func anyToMap(data any) (map[string]any, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]any{}
	}
	return out, nil
}

func getNested(root map[string]any, path []string) (any, bool) {
	var cur any = root
	for i, part := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		v, exists := m[part]
		if !exists {
			return nil, false
		}
		if i == len(path)-1 {
			return v, true
		}
		cur = v
	}
	return nil, false
}

func setNested(root map[string]any, path []string, value any) {
	if len(path) == 0 {
		return
	}
	cur := root
	for i, part := range path {
		if i == len(path)-1 {
			cur[part] = value
			return
		}
		next, ok := cur[part].(map[string]any)
		if !ok || next == nil {
			next = map[string]any{}
			cur[part] = next
		}
		cur = next
	}
}

// DecryptFieldFromJSON decrypts a field from a JSON payload string using dot-path notation.
func (db *DB) DecryptFieldFromJSON(dataJSON string, path string) (any, error) {
	if err := db.requireEncryptedMode(); err != nil {
		return nil, err
	}
	return decryptFieldFromJSONWithConfig(db.encrypted, dataJSON, path)
}

// DecryptField decrypts a field from a JSON map using dot-path notation.
func (db *DB) DecryptField(payload map[string]any, path string) (any, error) {
	if err := db.requireEncryptedMode(); err != nil {
		return nil, err
	}
	return decryptFieldFromMapWithConfig(db.encrypted, payload, path)
}

// DecryptFieldsFromJSON decrypts multiple fields from a JSON payload string.
// Returns a map keyed by the requested path.
func (db *DB) DecryptFieldsFromJSON(dataJSON string, paths ...string) (map[string]any, error) {
	if err := db.requireEncryptedMode(); err != nil {
		return nil, err
	}
	return decryptFieldsFromJSONWithConfig(db.encrypted, dataJSON, paths...)
}

// DecryptFields decrypts multiple fields from a JSON map.
// Returns a map keyed by the requested path.
func (db *DB) DecryptFields(payload map[string]any, paths ...string) (map[string]any, error) {
	if err := db.requireEncryptedMode(); err != nil {
		return nil, err
	}
	return decryptFieldsFromMapWithConfig(db.encrypted, payload, paths...)
}

// DecryptIntoStructFromJSON decrypts selected fields from JSON payload and unmarshals into a typed struct.
// The target must be a pointer to struct/map compatible with json.Unmarshal.
func (db *DB) DecryptIntoStructFromJSON(dataJSON string, target any, paths ...string) error {
	if err := db.requireEncryptedMode(); err != nil {
		return err
	}
	fields, err := db.DecryptFieldsFromJSON(dataJSON, paths...)
	if err != nil {
		return err
	}
	return decodeToTarget(fields, target)
}

// DecryptIntoStruct decrypts selected fields from payload map and unmarshals into a typed struct.
// The target must be a pointer to struct/map compatible with json.Unmarshal.
func (db *DB) DecryptIntoStruct(payload map[string]any, target any, paths ...string) error {
	if err := db.requireEncryptedMode(); err != nil {
		return err
	}
	fields, err := db.DecryptFields(payload, paths...)
	if err != nil {
		return err
	}
	return decodeToTarget(fields, target)
}

func decodeToTarget(data map[string]any, target any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, target)
}

func (db *DB) decryptFieldValue(value string) (any, error) {
	return decryptFieldValueWithConfig(db.encrypted, value)
}

func decryptFieldsFromJSONWithConfig(cfg *encryptedModeConfig, dataJSON string, paths ...string) (map[string]any, error) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(dataJSON), &payload); err != nil {
		return nil, err
	}
	return decryptFieldsFromMapWithConfig(cfg, payload, paths...)
}

func decryptFieldsFromMapWithConfig(cfg *encryptedModeConfig, payload map[string]any, paths ...string) (map[string]any, error) {
	if len(paths) == 0 {
		return map[string]any{}, nil
	}
	out := make(map[string]any, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		val, err := decryptFieldFromMapWithConfig(cfg, payload, path)
		if err != nil {
			return nil, err
		}
		out[path] = val
	}
	return out, nil
}

func decryptFieldFromJSONWithConfig(cfg *encryptedModeConfig, dataJSON string, path string) (any, error) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(dataJSON), &payload); err != nil {
		return nil, err
	}
	return decryptFieldFromMapWithConfig(cfg, payload, path)
}

func decryptFieldFromMapWithConfig(cfg *encryptedModeConfig, payload map[string]any, path string) (any, error) {
	parts := splitDotPath(path)
	if len(parts) == 0 {
		return nil, fmt.Errorf("path is required")
	}
	raw, ok := getNested(payload, parts)
	if !ok {
		return nil, fmt.Errorf("field not found: %s", path)
	}
	encText, ok := raw.(string)
	if !ok {
		return raw, nil
	}
	return decryptFieldValueWithConfig(cfg, encText)
}

func decryptFieldValueWithConfig(cfg *encryptedModeConfig, value string) (any, error) {
	const prefix = "enc:v1:"
	if !strings.HasPrefix(value, prefix) {
		return value, nil
	}

	encoded := strings.TrimPrefix(value, prefix)
	payload, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}

	key := sha256.Sum256([]byte(cfg.EncryptionKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(payload) < gcm.NonceSize() {
		return nil, fmt.Errorf("invalid encrypted payload")
	}

	nonce := payload[:gcm.NonceSize()]
	ciphertext := payload[gcm.NonceSize():]
	plainBytes, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	var out any
	if err := json.Unmarshal(plainBytes, &out); err != nil {
		return nil, err
	}
	return out, nil
}
