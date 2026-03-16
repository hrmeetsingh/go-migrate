package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"

	"github.com/harmeetsingh/go-migrate/internal/engine"
)

var (
	dbURL         string
	migrationsDir string
	mainBranch    string
)

func main() {
	root := &cobra.Command{
		Use:   "migrate",
		Short: "Git-aware database migration tool",
		Long:  "A database migration tool that chains migrations by git commit hash and detects dirty/diverged state.",
	}

	root.PersistentFlags().StringVar(&dbURL, "db", "", "PostgreSQL connection URL (or set DATABASE_URL)")
	root.PersistentFlags().StringVar(&migrationsDir, "dir", "migrations", "Path to migration files")
	root.PersistentFlags().StringVar(&mainBranch, "main-branch", "main", "Name of the main/trunk branch")

	root.AddCommand(upCmd(), downCmd(), statusCmd(), createCmd(), verifyCmd())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func newEngine() (*engine.Engine, error) {
	connURL := dbURL
	if connURL == "" {
		connURL = os.Getenv("DATABASE_URL")
	}
	if connURL == "" {
		return nil, fmt.Errorf("database URL required: use --db flag or set DATABASE_URL")
	}

	db, err := sql.Open("pgx", connURL)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("get working directory: %w", err)
	}

	return engine.New(engine.Config{
		DB:            db,
		RepoPath:      cwd,
		MigrationsDir: migrationsDir,
		MainBranch:    mainBranch,
	})
}

func upCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "up",
		Short: "Apply all pending migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			eng, err := newEngine()
			if err != nil {
				return err
			}
			return eng.Up(context.Background())
		},
	}
}

func downCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "down [N]",
		Short: "Revert the last N migrations (default 1)",
		RunE: func(cmd *cobra.Command, args []string) error {
			steps := 1
			if len(args) > 0 {
				n, err := strconv.Atoi(args[0])
				if err != nil {
					return fmt.Errorf("invalid step count: %w", err)
				}
				steps = n
			}
			eng, err := newEngine()
			if err != nil {
				return err
			}
			return eng.Down(context.Background(), steps)
		},
	}
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show migration status and detect dirty state",
		RunE: func(cmd *cobra.Command, args []string) error {
			eng, err := newEngine()
			if err != nil {
				return err
			}
			report, err := eng.Status(context.Background())
			if err != nil {
				return err
			}

			fmt.Printf("Branch:          %s\n", report.CurrentBranch)
			fmt.Printf("HEAD:            %s\n", report.HeadCommit)
			fmt.Printf("DB version:      %d\n", report.CurrentVersion)
			fmt.Printf("Applied:         %d\n", report.AppliedCount)
			fmt.Printf("Pending:         %d\n", report.PendingCount)

			if report.IsDirty {
				fmt.Println()
				fmt.Println("!! DIRTY STATE DETECTED !!")
				fmt.Printf("Diverged at:     version %d\n", report.Divergence.DivergentAtVersion)
				fmt.Printf("Revert count:    %d migrations\n", report.Divergence.RevertCount)
				fmt.Printf("Versions to revert: %v\n", report.Divergence.DBVersions)
				if len(report.Divergence.ExpectedVersions) > 0 {
					fmt.Printf("Then apply from %s: %v\n", mainBranch, report.Divergence.ExpectedVersions)
				}
			} else {
				fmt.Println("\nState:           clean")
			}

			return nil
		},
	}
}

func createCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create [name]",
		Short: "Create a new migration file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			eng, err := newEngine()
			if err != nil {
				return err
			}
			path, err := eng.Create(args[0])
			if err != nil {
				return err
			}
			fmt.Printf("Created: %s\n", path)
			return nil
		},
	}
}

func verifyCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "verify",
		Short: "Verify the integrity of the migration hash chain",
		RunE: func(cmd *cobra.Command, args []string) error {
			eng, err := newEngine()
			if err != nil {
				return err
			}
			valid, failedAt, err := eng.Verify(context.Background())
			if err != nil {
				return err
			}
			if valid {
				fmt.Println("Hash chain is valid. No tampering detected.")
			} else {
				fmt.Printf("Hash chain BROKEN at version %d!\n", failedAt)
				fmt.Println("A migration file may have been modified after it was applied.")
				os.Exit(1)
			}
			return nil
		},
	}
}
