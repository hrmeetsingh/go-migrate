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

	"github.com/harmeetsingh/go-migrate/internal/gitstate"
	"github.com/harmeetsingh/go-migrate/internal/hashchain"
)

type Engine struct {
	db            *sql.DB
	git           *gitstate.GitState
	migrationsDir string
	mainBranch    string
}

type Config struct {
	DB            *sql.DB
	RepoPath      string
	MigrationsDir string
	MainBranch    string
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

	currentDBVersion, err := goose.GetDBVersionContext(ctx, e.db)
	if err != nil {
		currentDBVersion = 0
	}

	localFiles, err := e.readLocalMigrationFiles()
	if err != nil {
		return fmt.Errorf("read local migrations: %w", err)
	}

	dbChain, err := e.loadDBChain(ctx)
	if err != nil {
		return fmt.Errorf("load chain: %w", err)
	}

	expectedChain := hashchain.BuildExpectedChain(localFiles)
	if div := hashchain.FindDivergence(dbChain, expectedChain); div != nil {
		fmt.Println("\n!! DIRTY STATE DETECTED !!")
		fmt.Printf("Diverged at:     version %d\n", div.DivergentAtVersion)
		fmt.Printf("Revert count:    %d migrations\n", div.RevertCount)
		fmt.Printf("Versions to revert: %v\n", div.DBVersions)
		if div.AppliedFromBranch != "" {
			fmt.Printf("Applied from:    %s\n", div.AppliedFromBranch)
			fmt.Printf(">> Switch to branch '%s' and run 'migrate down %d' to revert, then switch back and re-run 'migrate up'\n",
				div.AppliedFromBranch, div.RevertCount)
		} else {
			fmt.Printf(">> Run 'migrate down %d' to revert, then re-run 'migrate up'\n", div.RevertCount)
		}
		return fmt.Errorf("cannot apply migrations: hash chain has diverged at version %d", div.DivergentAtVersion)
	}

	pending := filterPending(localFiles, currentDBVersion)
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
			Version: version,
			Content: content,
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
