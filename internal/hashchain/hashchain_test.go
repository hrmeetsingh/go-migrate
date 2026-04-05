package hashchain_test

import (
	"testing"

	"github.com/hrmeetsingh/go-migrate/internal/hashchain"
)

func TestComputeChecksum(t *testing.T) {
	c1 := hashchain.ComputeChecksum([]byte("CREATE TABLE users (id SERIAL);"))
	c2 := hashchain.ComputeChecksum([]byte("CREATE TABLE users (id SERIAL);"))
	c3 := hashchain.ComputeChecksum([]byte("DROP TABLE users;"))

	if c1 != c2 {
		t.Fatal("identical content should produce identical checksum")
	}
	if c1 == c3 {
		t.Fatal("different content should produce different checksum")
	}
}

func TestBuildExpectedChain(t *testing.T) {
	files := []hashchain.MigrationFile{
		{Version: 1, Content: []byte("CREATE TABLE a;")},
		{Version: 2, Content: []byte("CREATE TABLE b;")},
		{Version: 3, Content: []byte("CREATE TABLE c;")},
	}

	chain := hashchain.BuildExpectedChain(files)

	if len(chain) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(chain))
	}

	if chain[0].ParentHash != hashchain.GenesisHash() {
		t.Fatal("first entry should have genesis parent hash")
	}

	for i := 1; i < len(chain); i++ {
		if chain[i].ParentHash != chain[i-1].EntryHash {
			t.Fatalf("entry %d parent hash should equal entry %d entry hash", i, i-1)
		}
	}

	for i, e := range chain {
		if e.EntryHash == "" {
			t.Fatalf("entry %d has empty hash", i)
		}
		if e.Version != int64(i+1) {
			t.Fatalf("entry %d version = %d, want %d", i, e.Version, i+1)
		}
	}
}

func TestVerifyChain_Valid(t *testing.T) {
	files := []hashchain.MigrationFile{
		{Version: 1, Content: []byte("CREATE TABLE a;")},
		{Version: 2, Content: []byte("CREATE TABLE b;")},
	}
	chain := hashchain.BuildExpectedChain(files)

	valid, _ := hashchain.VerifyChain(chain)
	if !valid {
		t.Fatal("valid chain should pass verification")
	}
}

func TestVerifyChain_Tampered(t *testing.T) {
	files := []hashchain.MigrationFile{
		{Version: 1, Content: []byte("CREATE TABLE a;")},
		{Version: 2, Content: []byte("CREATE TABLE b;")},
	}
	chain := hashchain.BuildExpectedChain(files)

	chain[1].Checksum = hashchain.ComputeChecksum([]byte("TAMPERED CONTENT"))

	valid, failedAt := hashchain.VerifyChain(chain)
	if valid {
		t.Fatal("tampered chain should fail verification")
	}
	if failedAt != 2 {
		t.Fatalf("expected failure at version 2, got %d", failedAt)
	}
}

func TestFindDivergence_Clean(t *testing.T) {
	files := []hashchain.MigrationFile{
		{Version: 1, Content: []byte("CREATE TABLE a;")},
		{Version: 2, Content: []byte("CREATE TABLE b;")},
	}
	chain := hashchain.BuildExpectedChain(files)

	div := hashchain.FindDivergence(chain, chain)
	if div != nil {
		t.Fatal("identical chains should not diverge")
	}
}

func TestFindDivergence_DifferentContent(t *testing.T) {
	dbFiles := []hashchain.MigrationFile{
		{Version: 1, Content: []byte("CREATE TABLE a;")},
		{Version: 2, Content: []byte("CREATE TABLE b_branch;")},
		{Version: 3, Content: []byte("CREATE TABLE c_branch;")},
	}
	mainFiles := []hashchain.MigrationFile{
		{Version: 1, Content: []byte("CREATE TABLE a;")},
		{Version: 2, Content: []byte("CREATE TABLE b_main;")},
	}

	dbChain := hashchain.BuildExpectedChain(dbFiles)
	mainChain := hashchain.BuildExpectedChain(mainFiles)

	div := hashchain.FindDivergence(dbChain, mainChain)
	if div == nil {
		t.Fatal("divergent chains should be detected")
	}
	if div.DivergentAtVersion != 2 {
		t.Fatalf("expected divergence at version 2, got %d", div.DivergentAtVersion)
	}
	if div.RevertCount != 2 {
		t.Fatalf("expected 2 reverts, got %d", div.RevertCount)
	}
}

func TestFindDivergence_DBHasExtra(t *testing.T) {
	files := []hashchain.MigrationFile{
		{Version: 1, Content: []byte("CREATE TABLE a;")},
		{Version: 2, Content: []byte("CREATE TABLE b;")},
	}
	mainFiles := []hashchain.MigrationFile{
		{Version: 1, Content: []byte("CREATE TABLE a;")},
	}

	dbChain := hashchain.BuildExpectedChain(files)
	mainChain := hashchain.BuildExpectedChain(mainFiles)

	div := hashchain.FindDivergence(dbChain, mainChain)
	if div == nil {
		t.Fatal("extra DB migrations should be detected as divergence")
	}
	if div.RevertCount != 1 {
		t.Fatalf("expected 1 revert, got %d", div.RevertCount)
	}
}
