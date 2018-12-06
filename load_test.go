package tables

import (
	"testing"
)

func TestLoad(t *testing.T) {
	tbl, err := Load()
	if err != nil {
		t.Fatal(err)
	}

	if len(tbl) == 0 {
		t.Fatalf("error loading config: %s", "missing data")
	}
}
