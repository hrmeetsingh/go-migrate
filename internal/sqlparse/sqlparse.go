package sqlparse

import (
	"regexp"
	"strings"

	"github.com/hrmeetsingh/go-migrate/internal/hashchain"
)

type OpKind string

const (
	OpCreateTable    OpKind = "CREATE TABLE"
	OpAlterTable     OpKind = "ALTER TABLE"
	OpDropTable      OpKind = "DROP TABLE"
	OpCreateIndex    OpKind = "CREATE INDEX"
	OpDropIndex      OpKind = "DROP INDEX"
	OpCreateSequence OpKind = "CREATE SEQUENCE"
	OpDropSequence   OpKind = "DROP SEQUENCE"
	OpInsert         OpKind = "INSERT"
	OpUpdate         OpKind = "UPDATE"
	OpDelete         OpKind = "DELETE"
	OpAddConstraint  OpKind = "ADD CONSTRAINT"
	OpDropConstraint OpKind = "DROP CONSTRAINT"
)

type SQLOp struct {
	Kind       OpKind
	EntityName string
}

type ConflictKind string

const (
	SoftConflict ConflictKind = "soft"
	HardConflict ConflictKind = "hard"
)

var (
	reCreateTable    = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)`)
	reAlterTable     = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?(\w+)`)
	reDropTable      = regexp.MustCompile(`(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\w+)`)
	reCreateIndex    = regexp.MustCompile(`(?i)CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+ON\s+(\w+)`)
	reDropIndex      = regexp.MustCompile(`(?i)DROP\s+INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+EXISTS\s+)?(\w+)`)
	reCreateSequence = regexp.MustCompile(`(?i)CREATE\s+SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)`)
	reDropSequence   = regexp.MustCompile(`(?i)DROP\s+SEQUENCE\s+(?:IF\s+EXISTS\s+)?(\w+)`)
	reInsert         = regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\w+)`)
	reUpdate         = regexp.MustCompile(`(?i)UPDATE\s+(\w+)`)
	reDelete         = regexp.MustCompile(`(?i)DELETE\s+FROM\s+(\w+)`)
)

// extractGooseUpSection returns only the SQL between "-- +goose Up" and
// "-- +goose Down" (or EOF). This ensures we only analyse the forward
// migration, not the rollback.
func extractGooseUpSection(sql []byte) string {
	text := string(sql)

	upIdx := strings.Index(strings.ToLower(text), "-- +goose up")
	if upIdx < 0 {
		return text
	}
	text = text[upIdx:]

	downIdx := strings.Index(strings.ToLower(text), "-- +goose down")
	if downIdx > 0 {
		text = text[:downIdx]
	}

	return text
}

// ParseOperations extracts DDL/DML operations from the goose Up section
// of a migration file.
func ParseOperations(sql []byte) []SQLOp {
	text := extractGooseUpSection(sql)
	var ops []SQLOp

	for _, m := range reCreateTable.FindAllStringSubmatch(text, -1) {
		ops = append(ops, SQLOp{Kind: OpCreateTable, EntityName: strings.ToLower(m[1])})
	}
	for _, m := range reAlterTable.FindAllStringSubmatch(text, -1) {
		ops = append(ops, SQLOp{Kind: OpAlterTable, EntityName: strings.ToLower(m[1])})
	}
	for _, m := range reDropTable.FindAllStringSubmatch(text, -1) {
		ops = append(ops, SQLOp{Kind: OpDropTable, EntityName: strings.ToLower(m[1])})
	}
	for _, m := range reCreateIndex.FindAllStringSubmatch(text, -1) {
		ops = append(ops, SQLOp{Kind: OpCreateIndex, EntityName: strings.ToLower(m[2])})
	}
	for _, m := range reDropIndex.FindAllStringSubmatch(text, -1) {
		ops = append(ops, SQLOp{Kind: OpDropIndex, EntityName: strings.ToLower(m[1])})
	}
	for _, m := range reCreateSequence.FindAllStringSubmatch(text, -1) {
		ops = append(ops, SQLOp{Kind: OpCreateSequence, EntityName: strings.ToLower(m[1])})
	}
	for _, m := range reDropSequence.FindAllStringSubmatch(text, -1) {
		ops = append(ops, SQLOp{Kind: OpDropSequence, EntityName: strings.ToLower(m[1])})
	}
	for _, m := range reInsert.FindAllStringSubmatch(text, -1) {
		ops = append(ops, SQLOp{Kind: OpInsert, EntityName: strings.ToLower(m[1])})
	}
	for _, m := range reUpdate.FindAllStringSubmatch(text, -1) {
		ops = append(ops, SQLOp{Kind: OpUpdate, EntityName: strings.ToLower(m[1])})
	}
	for _, m := range reDelete.FindAllStringSubmatch(text, -1) {
		ops = append(ops, SQLOp{Kind: OpDelete, EntityName: strings.ToLower(m[1])})
	}

	return ops
}

// ExtractDefinedEntities collects every table name that is created across
// the supplied migration files.
func ExtractDefinedEntities(files []hashchain.MigrationFile) map[string]bool {
	entities := make(map[string]bool)
	for _, f := range files {
		for _, op := range ParseOperations(f.Content) {
			if op.Kind == OpCreateTable {
				entities[op.EntityName] = true
			}
		}
	}
	return entities
}

// ClassifyConflict determines whether a set of SQL operations from a
// branch migration represents a soft conflict (can be resequenced) or a
// hard conflict (modifies entities defined in main).
//
// Soft: all operations create new entities not in mainEntities, or create
// indexes/sequences.
// Hard: any operation alters, drops, or does DML on a mainEntities member,
// or creates a table that already exists in main.
func ClassifyConflict(ops []SQLOp, mainEntities map[string]bool) ConflictKind {
	for _, op := range ops {
		switch op.Kind {
		case OpCreateTable:
			if mainEntities[op.EntityName] {
				return HardConflict
			}
		case OpAlterTable, OpDropTable:
			if mainEntities[op.EntityName] {
				return HardConflict
			}
		case OpInsert, OpUpdate, OpDelete:
			if mainEntities[op.EntityName] {
				return HardConflict
			}
		case OpDropIndex, OpDropSequence, OpDropConstraint:
			return HardConflict
		case OpCreateIndex, OpCreateSequence, OpAddConstraint:
			// Additive operations — always soft
		}
	}
	return SoftConflict
}
