package gitstate

import (
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/hrmeetsingh/go-migrate/internal/hashchain"
)

type GitState struct {
	repo *git.Repository
}

func Open(repoPath string) (*GitState, error) {
	r, err := git.PlainOpenWithOptions(repoPath, &git.PlainOpenOptions{
		DetectDotGit: true,
	})
	if err != nil {
		return nil, fmt.Errorf("open git repo at %s: %w", repoPath, err)
	}
	return &GitState{repo: r}, nil
}

func (g *GitState) HeadCommitHash() (string, error) {
	ref, err := g.repo.Head()
	if err != nil {
		return "", fmt.Errorf("get HEAD: %w", err)
	}
	return ref.Hash().String(), nil
}

func (g *GitState) CurrentBranch() (string, error) {
	ref, err := g.repo.Head()
	if err != nil {
		return "", fmt.Errorf("get HEAD: %w", err)
	}
	return ref.Name().Short(), nil
}

// MigrationFilesAtRef reads migration SQL files from the given directory
// as they exist at the specified git ref (branch name, tag, or commit hash).
func (g *GitState) MigrationFilesAtRef(refName, migrationsDir string) ([]hashchain.MigrationFile, error) {
	hash, err := g.resolveRef(refName)
	if err != nil {
		return nil, err
	}

	commit, err := g.repo.CommitObject(hash)
	if err != nil {
		return nil, fmt.Errorf("get commit %s: %w", hash, err)
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("get tree: %w", err)
	}

	// Navigate to migrations subdirectory
	if migrationsDir != "" && migrationsDir != "." {
		tree, err = tree.Tree(migrationsDir)
		if err != nil {
			return nil, fmt.Errorf("migrations dir %q not found at ref %s: %w", migrationsDir, refName, err)
		}
	}

	var files []hashchain.MigrationFile
	for _, entry := range tree.Entries {
		if !strings.HasSuffix(entry.Name, ".sql") {
			continue
		}

		version, err := parseVersion(entry.Name)
		if err != nil {
			continue
		}

		f, err := tree.File(entry.Name)
		if err != nil {
			return nil, fmt.Errorf("read file %s: %w", entry.Name, err)
		}

		reader, err := f.Reader()
		if err != nil {
			return nil, fmt.Errorf("open reader for %s: %w", entry.Name, err)
		}
		content, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			return nil, fmt.Errorf("read content of %s: %w", entry.Name, err)
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

// FindMergeBase returns the commit hash where the current branch diverged
// from the given target branch.
func (g *GitState) FindMergeBase(targetBranch string) (string, error) {
	headRef, err := g.repo.Head()
	if err != nil {
		return "", fmt.Errorf("get HEAD: %w", err)
	}

	targetHash, err := g.resolveRef(targetBranch)
	if err != nil {
		return "", err
	}

	headCommit, err := g.repo.CommitObject(headRef.Hash())
	if err != nil {
		return "", fmt.Errorf("get HEAD commit: %w", err)
	}

	targetCommit, err := g.repo.CommitObject(targetHash)
	if err != nil {
		return "", fmt.Errorf("get target commit: %w", err)
	}

	bases, err := headCommit.MergeBase(targetCommit)
	if err != nil {
		return "", fmt.Errorf("find merge base: %w", err)
	}

	if len(bases) == 0 {
		return "", fmt.Errorf("no common ancestor between HEAD and %s", targetBranch)
	}

	return bases[0].Hash.String(), nil
}

func (g *GitState) resolveRef(name string) (plumbing.Hash, error) {
	// Try as a branch ref first
	ref, err := g.repo.Reference(plumbing.NewBranchReferenceName(name), true)
	if err == nil {
		return ref.Hash(), nil
	}

	// Try as a remote branch
	ref, err = g.repo.Reference(plumbing.NewRemoteReferenceName("origin", name), true)
	if err == nil {
		return ref.Hash(), nil
	}

	// Try as a tag
	ref, err = g.repo.Reference(plumbing.NewTagReferenceName(name), true)
	if err == nil {
		tagObj, err := g.repo.TagObject(ref.Hash())
		if err == nil {
			commit, err := tagObj.Commit()
			if err == nil {
				return commit.Hash, nil
			}
		}
		return ref.Hash(), nil
	}

	// Try as a raw hash
	if len(name) == 40 {
		h := plumbing.NewHash(name)
		_, err := g.repo.CommitObject(h)
		if err == nil {
			return h, nil
		}
	}

	return plumbing.ZeroHash, fmt.Errorf("cannot resolve ref %q", name)
}

func parseVersion(filename string) (int64, error) {
	base := filepath.Base(filename)
	parts := strings.SplitN(base, "_", 2)
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid migration filename: %s", filename)
	}
	return strconv.ParseInt(parts[0], 10, 64)
}

// CommitForFile returns the hash of the latest commit that modified the
// given file path relative to the repo root.
func (g *GitState) CommitForFile(filePath string) (string, error) {
	logOpts := &git.LogOptions{
		FileName: &filePath,
	}
	iter, err := g.repo.Log(logOpts)
	if err != nil {
		return "", fmt.Errorf("git log for %s: %w", filePath, err)
	}
	defer iter.Close()

	var latest *object.Commit
	err = iter.ForEach(func(c *object.Commit) error {
		if latest == nil {
			latest = c
		}
		return fmt.Errorf("stop")
	})
	if latest == nil {
		if err != nil {
			return "", fmt.Errorf("no commits found for %s: %w", filePath, err)
		}
		return "", fmt.Errorf("no commits found for %s", filePath)
	}
	return latest.Hash.String(), nil
}
