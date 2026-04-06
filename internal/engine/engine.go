package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pressly/goose/v3"

	"github.com/hrmeetsingh/go-migrate/internal/gitstate"
	"github.com/hrmeetsingh/go-migrate/internal/hashchain"
	"github.com/hrmeetsingh/go-migrate/internal/sqlparse"
)

type Engine struct {
	db            *sql.DB
	git           *gitstate.GitState
	migrationsDir string
	mainBranch    string
	confirmFunc   func(prompt string) bool
}

type Config struct {
	DB            *sql.DB
	RepoPath      string
	MigrationsDir string
	MainBranch    string
	ConfirmFunc   func(prompt string) bool
}

type ReconcileResult struct {
	Verified   []int64
	Foreign    []ForeignEntry
	Pending    []hashchain.MigrationFile
	Collisions []VersionCollision
}

type ForeignEntry struct {
	Version       int64
	AppliedBranch string
}

type VersionCollision struct {
	Version      int64
	LocalFile    hashchain.MigrationFile
	DBEntry      hashchain.Entry
	ConflictKind sqlparse.ConflictKind
}

type ResequenceAction struct {
	OldFilename string
	NewFilename string
	OldVersion  int64
	NewVersion  int64
}

func New(cfg Config) (*Engine, error) {
	gs, err := gitstate.Open(cfg.RepoPath)
	if err != nil {
		return nil, fmt.Errorf("init git state: %w", err)
	}

	if cfg.MainBranch == "" {
		cfg.MainBranch = "main"
	}

	return &Engine{
		db:            cfg.DB,
		git:           gs,
		migrationsDir: cfg.MigrationsDir,
		mainBranch:    cfg.MainBranch,
		confirmFunc:   cfg.ConfirmFunc,
	}, nil
}

func (e *Engine) Init(ctx context.Context) error {
	query := `CREATE TABLE IF NOT EXISTS migration_hash_chain (
		version        BIGINT PRIMARY KEY,
		git_commit     VARCHAR(40) NOT NULL,
		parent_hash    VARCHAR(64) NOT NULL,
		entry_hash     VARCHAR(64) NOT NULL,
		checksum       VARCHAR(64) NOT NULL,
		applied_branch VARCHAR(255) NOT NULL DEFAULT '',
		applied_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`
	_, err := e.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create hash chain table: %w", err)
	}

	_, err = e.db.ExecContext(ctx,
		`ALTER TABLE migration_hash_chain ADD COLUMN IF NOT EXISTS applied_branch VARCHAR(255) NOT NULL DEFAULT ''`)
	if err != nil {
		return fmt.Errorf("add applied_branch column: %w", err)
	}

	return nil
}

func (e *Engine) Up(ctx context.Context) error {
	if err := e.Init(ctx); err != nil {
		return err
	}

	goose.SetDialect("postgres")

	localFiles, err := e.readLocalMigrationFiles()
	if err != nil {
		return fmt.Errorf("read local migrations: %w", err)
	}

	dbChain, err := e.loadDBChain(ctx)
	if err != nil {
		return fmt.Errorf("load chain: %w", err)
	}

	result := e.reconcile(localFiles, dbChain)

	if len(result.Collisions) > 0 {
		if err := e.classifyCollisions(result); err != nil {
			return err
		}

		var hardConflicts, softConflicts []VersionCollision
		for _, c := range result.Collisions {
			if c.ConflictKind == sqlparse.HardConflict {
				hardConflicts = append(hardConflicts, c)
			} else {
				softConflicts = append(softConflicts, c)
			}
		}

		if len(hardConflicts) > 0 {
			return e.reportHardConflicts(hardConflicts, dbChain)
		}

		var dbMaxVersion int64
		if len(dbChain) > 0 {
			dbMaxVersion = dbChain[len(dbChain)-1].Version
		}
		actions := e.buildResequencePlan(localFiles, softConflicts, dbMaxVersion)

		e.printSoftConflictSummary(softConflicts, actions)

		if e.confirmFunc == nil || !e.confirmFunc("Proceed with resequencing? [y/N]: ") {
			return fmt.Errorf("resequencing declined; resolve version conflicts manually")
		}

		if err := e.applyResequence(actions); err != nil {
			return err
		}
		fmt.Printf("\nResequenced %d file(s).\n", len(actions))

		localFiles, err = e.readLocalMigrationFiles()
		if err != nil {
			return fmt.Errorf("re-read local migrations: %w", err)
		}
		result = e.reconcile(localFiles, dbChain)
		if len(result.Collisions) > 0 {
			return fmt.Errorf("unexpected collisions remain after resequencing")
		}
	}

	if len(result.Foreign) > 0 {
		fmt.Printf("\nNote: DB contains %d migration(s) from other branches (adopted)\n", len(result.Foreign))
	}

	pending := result.Pending
	if len(pending) == 0 {
		fmt.Println("No pending migrations.")
		return nil
	}

	parentHash := hashchain.GenesisHash()
	if len(dbChain) > 0 {
		parentHash = dbChain[len(dbChain)-1].EntryHash
	}

	commitHash, err := e.git.HeadCommitHash()
	if err != nil {
		return fmt.Errorf("get HEAD: %w", err)
	}

	currentBranch, _ := e.git.CurrentBranch()

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set dialect: %w", err)
	}

	for _, mf := range pending {
		filePath := e.migrationFilePath(mf.Version)
		if err := goose.UpByOneContext(ctx, e.db, filepath.Dir(filePath)); err != nil {
			return fmt.Errorf("apply migration %d: %w", mf.Version, err)
		}

		checksum := hashchain.ComputeChecksum(mf.Content)
		entryHash := hashchain.ComputeEntryHash(parentHash, checksum, mf.Version)

		_, err := e.db.ExecContext(ctx,
			`INSERT INTO migration_hash_chain (version, git_commit, parent_hash, entry_hash, checksum, applied_branch, applied_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			mf.Version, commitHash, parentHash, entryHash, checksum, currentBranch, time.Now(),
		)
		if err != nil {
			return fmt.Errorf("record chain entry for version %d: %w", mf.Version, err)
		}

		fmt.Printf("Applied migration %d (hash: %s..)\n", mf.Version, entryHash[:12])
		parentHash = entryHash
	}

	return nil
}

// reconcile performs a version-aware comparison between local migration
// files and the DB chain, identifying verified matches, foreign entries
// (from other branches), pending files, and version collisions.
func (e *Engine) reconcile(localFiles []hashchain.MigrationFile, dbChain []hashchain.Entry) *ReconcileResult {
	result := &ReconcileResult{}
	localMap := make(map[int64]hashchain.MigrationFile)
	for _, f := range localFiles {
		localMap[f.Version] = f
	}

	for _, dbEntry := range dbChain {
		if localFile, exists := localMap[dbEntry.Version]; exists {
			localChecksum := hashchain.ComputeChecksum(localFile.Content)
			if localChecksum == dbEntry.Checksum {
				result.Verified = append(result.Verified, dbEntry.Version)
			} else {
				result.Collisions = append(result.Collisions, VersionCollision{
					Version:   dbEntry.Version,
					LocalFile: localFile,
					DBEntry:   dbEntry,
				})
			}
			delete(localMap, dbEntry.Version)
		} else {
			result.Foreign = append(result.Foreign, ForeignEntry{
				Version:       dbEntry.Version,
				AppliedBranch: dbEntry.AppliedBranch,
			})
		}
	}

	for _, f := range localFiles {
		if _, ok := localMap[f.Version]; ok {
			result.Pending = append(result.Pending, f)
		}
	}
	sort.Slice(result.Pending, func(i, j int) bool {
		return result.Pending[i].Version < result.Pending[j].Version
	})

	return result
}

func (e *Engine) classifyCollisions(result *ReconcileResult) error {
	mainFiles, err := e.git.MigrationFilesAtRef(e.mainBranch, e.migrationsDir)
	if err != nil {
		for i := range result.Collisions {
			result.Collisions[i].ConflictKind = sqlparse.HardConflict
		}
		return nil
	}

	mainEntities := sqlparse.ExtractDefinedEntities(mainFiles)

	for i := range result.Collisions {
		ops := sqlparse.ParseOperations(result.Collisions[i].LocalFile.Content)
		result.Collisions[i].ConflictKind = sqlparse.ClassifyConflict(ops, mainEntities)
	}
	return nil
}

func (e *Engine) buildResequencePlan(localFiles []hashchain.MigrationFile, collisions []VersionCollision, dbMaxVersion int64) []ResequenceAction {
	earliestCollision := collisions[0].Version
	for _, c := range collisions[1:] {
		if c.Version < earliestCollision {
			earliestCollision = c.Version
		}
	}

	var toShift []hashchain.MigrationFile
	for _, f := range localFiles {
		if f.Version >= earliestCollision {
			toShift = append(toShift, f)
		}
	}

	nextVersion := dbMaxVersion + 1
	var actions []ResequenceAction
	for _, f := range toShift {
		newFilename := fmt.Sprintf("%03d_%s", nextVersion, filenameDescPart(f.Filename))
		actions = append(actions, ResequenceAction{
			OldFilename: f.Filename,
			NewFilename: newFilename,
			OldVersion:  f.Version,
			NewVersion:  nextVersion,
		})
		nextVersion++
	}
	return actions
}

func (e *Engine) applyResequence(actions []ResequenceAction) error {
	// Rename in reverse order to avoid overwriting files when shifting
	// forward (e.g. 003→004 before 004→005 would clobber the original 004).
	for i := len(actions) - 1; i >= 0; i-- {
		a := actions[i]
		oldPath := filepath.Join(e.migrationsDir, a.OldFilename)
		newPath := filepath.Join(e.migrationsDir, a.NewFilename)
		if err := os.Rename(oldPath, newPath); err != nil {
			return fmt.Errorf("rename %s -> %s: %w", a.OldFilename, a.NewFilename, err)
		}
	}
	for _, a := range actions {
		fmt.Printf("  Renamed: %s -> %s\n", a.OldFilename, a.NewFilename)
	}
	return nil
}

func (e *Engine) reportHardConflicts(conflicts []VersionCollision, dbChain []hashchain.Entry) error {
	fmt.Println("\n!! HARD CONFLICT DETECTED !!")
	for _, c := range conflicts {
		ops := sqlparse.ParseOperations(c.LocalFile.Content)
		var affected []string
		for _, op := range ops {
			affected = append(affected, fmt.Sprintf("%s %q", op.Kind, op.EntityName))
		}
		fmt.Printf("\n  Version %03d: LOCAL %s (%s)\n", c.Version, strings.Join(affected, ", "), c.LocalFile.Filename)
		if c.DBEntry.AppliedBranch != "" {
			fmt.Printf("               DB has different migration applied from branch %q\n", c.DBEntry.AppliedBranch)
		} else {
			fmt.Printf("               DB has different migration with same version\n")
		}
		fmt.Printf("               Conflict: HARD (modifies entities defined in main)\n")
	}

	earliestVersion := conflicts[0].Version
	revertCount := 0
	for _, entry := range dbChain {
		if entry.Version >= earliestVersion {
			revertCount++
		}
	}

	branch := conflicts[0].DBEntry.AppliedBranch
	fmt.Println("\nCannot auto-resolve. Manual steps:")
	if branch != "" {
		fmt.Printf("  1. Switch to branch '%s' and run 'migrate down %d' to revert\n", branch, revertCount)
		fmt.Println("  2. Switch back and re-run 'migrate up'")
	} else {
		fmt.Printf("  1. Run 'migrate down %d' to revert the conflicting migrations\n", revertCount)
		fmt.Println("  2. Re-run 'migrate up'")
	}

	return fmt.Errorf("cannot apply migrations: hard conflict at version %d", earliestVersion)
}

func (e *Engine) printSoftConflictSummary(conflicts []VersionCollision, actions []ResequenceAction) {
	fmt.Printf("\nReconciliation found %d version collision(s):\n", len(conflicts))
	for _, c := range conflicts {
		ops := sqlparse.ParseOperations(c.LocalFile.Content)
		var desc []string
		for _, op := range ops {
			desc = append(desc, fmt.Sprintf("%s %q", op.Kind, op.EntityName))
		}
		fmt.Printf("\n  Version %03d: LOCAL %s (%s)\n", c.Version, strings.Join(desc, ", "), c.LocalFile.Filename)
		if c.DBEntry.AppliedBranch != "" {
			fmt.Printf("               DB has different migration applied from branch %q\n", c.DBEntry.AppliedBranch)
		} else {
			fmt.Printf("               DB has different migration with same version\n")
		}
		fmt.Printf("               Conflict: SOFT (independent new entity)\n")
	}

	fmt.Println("\nSuggested resequencing:")
	for _, a := range actions {
		fmt.Printf("  %s  ->  %s\n", a.OldFilename, a.NewFilename)
	}
	fmt.Println()
}

func filenameDescPart(filename string) string {
	parts := strings.SplitN(filename, "_", 2)
	if len(parts) < 2 {
		return filename
	}
	return parts[1]
}

func (e *Engine) Down(ctx context.Context, steps int) error {
	if err := e.Init(ctx); err != nil {
		return err
	}

	goose.SetDialect("postgres")

	for i := 0; i < steps; i++ {
		if err := goose.DownContext(ctx, e.db, e.migrationsDir); err != nil {
			return fmt.Errorf("revert step %d: %w", i+1, err)
		}

		var maxVersion int64
		row := e.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM migration_hash_chain`)
		if err := row.Scan(&maxVersion); err != nil {
			return fmt.Errorf("get max chain version: %w", err)
		}

		if maxVersion > 0 {
			_, err := e.db.ExecContext(ctx, `DELETE FROM migration_hash_chain WHERE version = $1`, maxVersion)
			if err != nil {
				return fmt.Errorf("remove chain entry %d: %w", maxVersion, err)
			}
			fmt.Printf("Reverted migration %d\n", maxVersion)
		}
	}

	return nil
}

type StatusReport struct {
	CurrentVersion int64
	CurrentBranch  string
	HeadCommit     string
	AppliedCount   int
	PendingCount   int
	IsDirty        bool
	Divergence     *hashchain.Divergence
}

func (e *Engine) Status(ctx context.Context) (*StatusReport, error) {
	if err := e.Init(ctx); err != nil {
		return nil, err
	}

	report := &StatusReport{}

	goose.SetDialect("postgres")
	ver, err := goose.GetDBVersionContext(ctx, e.db)
	if err != nil {
		ver = 0
	}
	report.CurrentVersion = ver

	report.CurrentBranch, _ = e.git.CurrentBranch()
	report.HeadCommit, _ = e.git.HeadCommitHash()

	dbChain, err := e.loadDBChain(ctx)
	if err != nil {
		return nil, fmt.Errorf("load chain: %w", err)
	}
	report.AppliedCount = len(dbChain)

	localFiles, err := e.readLocalMigrationFiles()
	if err != nil {
		return nil, fmt.Errorf("read local migrations: %w", err)
	}
	report.PendingCount = len(filterPending(localFiles, ver))

	mainFiles, err := e.git.MigrationFilesAtRef(e.mainBranch, e.migrationsDir)
	if err != nil {
		return report, nil
	}

	expectedChain := hashchain.BuildExpectedChain(mainFiles)
	div := hashchain.FindDivergence(dbChain, expectedChain)
	if div != nil {
		report.IsDirty = true
		report.Divergence = div
	}

	return report, nil
}

func (e *Engine) Verify(ctx context.Context) (bool, int64, error) {
	if err := e.Init(ctx); err != nil {
		return false, 0, err
	}

	chain, err := e.loadDBChain(ctx)
	if err != nil {
		return false, 0, fmt.Errorf("load chain: %w", err)
	}

	if len(chain) == 0 {
		return true, 0, nil
	}

	valid, failedAt := hashchain.VerifyChain(chain)
	return valid, failedAt, nil
}

func (e *Engine) Create(name string) (string, error) {
	localFiles, err := e.readLocalMigrationFiles()
	if err != nil {
		return "", fmt.Errorf("read migrations: %w", err)
	}

	var nextVersion int64 = 1
	if len(localFiles) > 0 {
		nextVersion = localFiles[len(localFiles)-1].Version + 1
	}

	filename := fmt.Sprintf("%03d_%s.sql", nextVersion, sanitizeName(name))
	fullPath := filepath.Join(e.migrationsDir, filename)

	commitHash, _ := e.git.HeadCommitHash()

	content := fmt.Sprintf(`-- +goose Up
-- Migration: %s
-- Git commit: %s
-- Created: %s


-- +goose Down

`, name, commitHash, time.Now().Format(time.RFC3339))

	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		return "", fmt.Errorf("write migration file: %w", err)
	}

	return fullPath, nil
}

func (e *Engine) loadDBChain(ctx context.Context) ([]hashchain.Entry, error) {
	rows, err := e.db.QueryContext(ctx,
		`SELECT version, git_commit, parent_hash, entry_hash, checksum, applied_branch, applied_at
		 FROM migration_hash_chain ORDER BY version ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chain []hashchain.Entry
	for rows.Next() {
		var e hashchain.Entry
		if err := rows.Scan(&e.Version, &e.GitCommitHash, &e.ParentHash, &e.EntryHash, &e.Checksum, &e.AppliedBranch, &e.AppliedAt); err != nil {
			return nil, err
		}
		chain = append(chain, e)
	}
	return chain, rows.Err()
}

func (e *Engine) readLocalMigrationFiles() ([]hashchain.MigrationFile, error) {
	entries, err := os.ReadDir(e.migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}

	var files []hashchain.MigrationFile
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		version, err := parseVersion(entry.Name())
		if err != nil {
			continue
		}
		content, err := os.ReadFile(filepath.Join(e.migrationsDir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		files = append(files, hashchain.MigrationFile{
			Version:  version,
			Filename: entry.Name(),
			Content:  content,
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Version < files[j].Version
	})

	return files, nil
}

func (e *Engine) migrationFilePath(version int64) string {
	entries, _ := os.ReadDir(e.migrationsDir)
	prefix := fmt.Sprintf("%03d_", version)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			return filepath.Join(e.migrationsDir, entry.Name())
		}
	}
	return filepath.Join(e.migrationsDir, fmt.Sprintf("%03d_unknown.sql", version))
}

func filterPending(files []hashchain.MigrationFile, currentVersion int64) []hashchain.MigrationFile {
	var pending []hashchain.MigrationFile
	for _, f := range files {
		if f.Version > currentVersion {
			pending = append(pending, f)
		}
	}
	return pending
}

func parseVersion(filename string) (int64, error) {
	parts := strings.SplitN(filename, "_", 2)
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid filename: %s", filename)
	}
	return strconv.ParseInt(parts[0], 10, 64)
}

func sanitizeName(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	return name
}
