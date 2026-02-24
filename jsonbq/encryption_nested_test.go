package jsonbq

import (
	"strings"
	"testing"
)

func TestParseNormalSQLEncryptedSearchRewriteMultipleNestedFields(t *testing.T) {
	email := "john.carter@demo.test"
	contactEmail := "john.contact@demo.test"
	phone := "+1-555-1111"

	emailIdx, err := blindIndexWithKey(email, "demo-hmac-key")
	if err != nil {
		t.Fatalf("blind index email err: %v", err)
	}
	contactEmailIdx, err := blindIndexWithKey(contactEmail, "demo-hmac-key")
	if err != nil {
		t.Fatalf("blind index contact email err: %v", err)
	}
	phoneIdx, err := blindIndexWithKey(phone, "demo-hmac-key")
	if err != nil {
		t.Fatalf("blind index phone err: %v", err)
	}

	sql, args, err := ParseNormalSQLWithOptions(
		`SELECT id
		 FROM patients p
		 WHERE p.tenant_id = :tenant_id
		   AND p.data.email = :email
		   AND p.data.profile.contact.email = :contact_email
		   AND :phone = p.data.profile.contact.phone`,
		map[string]any{
			"tenant_id":     int64(77),
			"email":         email,
			"contact_email": contactEmail,
			"phone":         phone,
		},
		SQLParseOptions{
			JSONColumns: []string{"data"},
			EncryptedSearchPaths: map[string]bool{
				"email":                 true,
				"profile.contact.email": true,
				"profile.contact.phone": true,
			},
			EncryptedHMACKey: "demo-hmac-key",
		},
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if !strings.Contains(sql, "jsonb_extract_path_text(p.data, '_secure_idx', 'email') = '"+emailIdx+"'") {
		t.Fatalf("expected top-level encrypted rewrite, got: %s", sql)
	}
	if !strings.Contains(sql, "jsonb_extract_path_text(p.data, '_secure_idx', 'profile_contact_email') = '"+contactEmailIdx+"'") {
		t.Fatalf("expected nested encrypted rewrite for contact email, got: %s", sql)
	}
	if !strings.Contains(sql, "'"+phoneIdx+"' = jsonb_extract_path_text(p.data, '_secure_idx', 'profile_contact_phone')") {
		t.Fatalf("expected reversed encrypted rewrite for phone, got: %s", sql)
	}
	if len(args) != 1 || args[0] != int64(77) {
		t.Fatalf("expected only tenant_id arg after encrypted rewrites, got: %v", args)
	}
}

func TestTransformDataForFieldEncryptionMultipleNested(t *testing.T) {
	cfg := &encryptedModeConfig{
		EncryptionKey: "demo-encryption-key",
		HMACKey:       "demo-hmac-key",
		FieldSpecs: map[string]bool{
			"email":                 true,
			"profile.contact.email": true,
			"profile.contact.phone": true,
		},
	}
	in := map[string]any{
		"name":  "John",
		"email": "john.carter@demo.test",
		"profile": map[string]any{
			"contact": map[string]any{
				"email": "john.contact@demo.test",
				"phone": "+1-555-1111",
			},
		},
	}

	outAny, err := cfg.transformDataForFieldEncryption(in)
	if err != nil {
		t.Fatalf("transform err: %v", err)
	}
	out, ok := outAny.(map[string]any)
	if !ok {
		t.Fatalf("expected map output, got %T", outAny)
	}

	topEmail, _ := getNested(out, []string{"email"})
	if s, ok := topEmail.(string); !ok || !strings.HasPrefix(s, "enc:v1:") {
		t.Fatalf("expected encrypted top-level email, got: %#v", topEmail)
	}
	nestedEmail, _ := getNested(out, []string{"profile", "contact", "email"})
	if s, ok := nestedEmail.(string); !ok || !strings.HasPrefix(s, "enc:v1:") {
		t.Fatalf("expected encrypted nested email, got: %#v", nestedEmail)
	}
	nestedPhone, _ := getNested(out, []string{"profile", "contact", "phone"})
	if s, ok := nestedPhone.(string); !ok || !strings.HasPrefix(s, "enc:v1:") {
		t.Fatalf("expected encrypted nested phone, got: %#v", nestedPhone)
	}

	topIdx, _ := getNested(out, []string{"_secure_idx", "email"})
	if s, ok := topIdx.(string); !ok || len(s) == 0 {
		t.Fatalf("expected searchable top-level index, got: %#v", topIdx)
	}
	nestedEmailIdx, _ := getNested(out, []string{"_secure_idx", "profile_contact_email"})
	if s, ok := nestedEmailIdx.(string); !ok || len(s) == 0 {
		t.Fatalf("expected searchable nested email index, got: %#v", nestedEmailIdx)
	}
	nestedPhoneIdx, _ := getNested(out, []string{"_secure_idx", "profile_contact_phone"})
	if s, ok := nestedPhoneIdx.(string); !ok || len(s) == 0 {
		t.Fatalf("expected searchable nested phone index, got: %#v", nestedPhoneIdx)
	}

	decEmail, err := decryptFieldFromMapWithConfig(cfg, out, "email")
	if err != nil || decEmail != "john.carter@demo.test" {
		t.Fatalf("unexpected decrypted top-level email: val=%v err=%v", decEmail, err)
	}
	decNestedEmail, err := decryptFieldFromMapWithConfig(cfg, out, "profile.contact.email")
	if err != nil || decNestedEmail != "john.contact@demo.test" {
		t.Fatalf("unexpected decrypted nested email: val=%v err=%v", decNestedEmail, err)
	}
	decNestedPhone, err := decryptFieldFromMapWithConfig(cfg, out, "profile.contact.phone")
	if err != nil || decNestedPhone != "+1-555-1111" {
		t.Fatalf("unexpected decrypted nested phone: val=%v err=%v", decNestedPhone, err)
	}
}

func TestUpdateBuildEncryptedMultipleNestedFields(t *testing.T) {
	u := &UpdateQuery{
		table:      "patients",
		columnName: "data",
		encrypted: &encryptedModeConfig{
			EncryptedColumn: "encrypted_data",
			HMACColumn:      "data_hmac",
			EncryptionKey:   "demo-encryption-key",
			HMACKey:         "demo-hmac-key",
			FieldSpecs: map[string]bool{
				"email":                 true,
				"profile.contact.email": true,
				"profile.contact.phone": true,
			},
		},
	}
	u.Set(
		Set("email").To("john.carter@demo.test"),
		Set("profile", "contact", "email").To("john.contact@demo.test"),
		Set("profile", "contact", "phone").To("+1-555-1111"),
	)

	sql, args, err := u.Build()
	if err != nil {
		t.Fatalf("unexpected build err: %v", err)
	}
	if !strings.Contains(sql, "ARRAY['_secure_idx', 'email']::text[]") {
		t.Fatalf("expected _secure_idx.email write in SQL, got: %s", sql)
	}
	if !strings.Contains(sql, "ARRAY['_secure_idx', 'profile_contact_email']::text[]") {
		t.Fatalf("expected _secure_idx.profile_contact_email write in SQL, got: %s", sql)
	}
	if !strings.Contains(sql, "ARRAY['_secure_idx', 'profile_contact_phone']::text[]") {
		t.Fatalf("expected _secure_idx.profile_contact_phone write in SQL, got: %s", sql)
	}
	if !strings.Contains(sql, "ARRAY['profile', 'contact', 'email']::text[]") {
		t.Fatalf("expected nested path write for profile.contact.email, got: %s", sql)
	}
	if !strings.Contains(sql, "ARRAY['profile', 'contact', 'phone']::text[]") {
		t.Fatalf("expected nested path write for profile.contact.phone, got: %s", sql)
	}
	if len(args) == 0 {
		t.Fatalf("expected bind args for encrypted updates")
	}
}
