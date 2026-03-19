package hashchain

import (
	"crypto/sha256"
	"fmt"
	"time"
)

type Entry struct {
	Version       int64
	GitCommitHash string
	ParentHash    string
	EntryHash     string
	Checksum      string
	AppliedBranch string
	AppliedAt     time.Time
}

const genesisHash = "0000000000000000000000000000000000000000000000000000000000000000"

func ComputeChecksum(content []byte) string {
	h := sha256.Sum256(content)
	return fmt.Sprintf("%x", h)
}

func ComputeEntryHash(parentHash, checksum string, version int64) string {
	data := fmt.Sprintf("%s:%s:%d", parentHash, checksum, version)
	h := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", h)
}

func GenesisHash() string {
	return genesisHash
}

// BuildExpectedChain computes the expected hash chain from a list of
// migration file contents ordered by version.
type MigrationFile struct {
	Version  int64
	Content  []byte
}

func BuildExpectedChain(files []MigrationFile) []Entry {
	chain := make([]Entry, 0, len(files))
	parentHash := genesisHash

	for _, f := range files {
		checksum := ComputeChecksum(f.Content)
		entryHash := ComputeEntryHash(parentHash, checksum, f.Version)
		chain = append(chain, Entry{
			Version:    f.Version,
			ParentHash: parentHash,
			EntryHash:  entryHash,
			Checksum:   checksum,
		})
		parentHash = entryHash
	}

	return chain
}

type Divergence struct {
	DivergentAtVersion int64
	RevertCount        int
	DBVersions         []int64
	ExpectedVersions   []int64
	AppliedFromBranch  string
}

// FindDivergence compares the applied DB chain against an expected chain
// (e.g. from the main branch). Returns nil if chains match up to the
// shorter chain's length.
func FindDivergence(dbChain, expectedChain []Entry) *Divergence {
	minLen := len(dbChain)
	if len(expectedChain) < minLen {
		minLen = len(expectedChain)
	}

	for i := 0; i < minLen; i++ {
		if dbChain[i].EntryHash != expectedChain[i].EntryHash {
			revertCount := len(dbChain) - i
			dbVersions := make([]int64, revertCount)
			for j := 0; j < revertCount; j++ {
				dbVersions[j] = dbChain[len(dbChain)-1-j].Version
			}
			expectedVersions := make([]int64, 0)
			for j := i; j < len(expectedChain); j++ {
				expectedVersions = append(expectedVersions, expectedChain[j].Version)
			}
			return &Divergence{
				DivergentAtVersion: dbChain[i].Version,
				RevertCount:        revertCount,
				DBVersions:         dbVersions,
				ExpectedVersions:   expectedVersions,
				AppliedFromBranch:  dbChain[i].AppliedBranch,
			}
		}
	}

	if len(dbChain) > len(expectedChain) {
		revertCount := len(dbChain) - len(expectedChain)
		dbVersions := make([]int64, revertCount)
		for j := 0; j < revertCount; j++ {
			dbVersions[j] = dbChain[len(dbChain)-1-j].Version
		}
		return &Divergence{
			DivergentAtVersion: dbChain[len(expectedChain)].Version,
			RevertCount:        revertCount,
			DBVersions:         dbVersions,
			AppliedFromBranch:  dbChain[len(expectedChain)].AppliedBranch,
		}
	}

	return nil
}

// VerifyChain walks a chain and re-computes each entry hash to detect tampering.
func VerifyChain(chain []Entry) (valid bool, failedAtVersion int64) {
	parentHash := genesisHash
	for _, e := range chain {
		expected := ComputeEntryHash(parentHash, e.Checksum, e.Version)
		if expected != e.EntryHash {
			return false, e.Version
		}
		parentHash = e.EntryHash
	}
	return true, 0
}
