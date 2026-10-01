package datatypes

import (
	"bytes"
	"strings"
	"testing"
)

func TestGzippedTextScanLimit(t *testing.T) {
	old := MaxGzipDecompressedSize
	defer func() { MaxGzipDecompressedSize = old }()

	v, _ := GzippedText(bytes.Repeat([]byte("a"), 100)).Value()
	gz := v.([]byte)

	MaxGzipDecompressedSize = 100
	var g GzippedText
	if err := g.Scan(gz); err != nil || len(g) != 100 {
		t.Fatalf("at limit: %v len %d", err, len(g))
	}
	MaxGzipDecompressedSize = 99
	if err := g.Scan(gz); err == nil || !strings.Contains(err.Error(), "MaxGzipDecompressedSize") {
		t.Fatalf("expected limit error, got %v", err)
	}
	MaxGzipDecompressedSize = 0
	if err := g.Scan(gz); err != nil || len(g) != 100 {
		t.Fatalf("unlimited: %v", err)
	}
}
