package sqlparse

import (
	"testing"

	"github.com/hrmeetsingh/go-migrate/internal/hashchain"
)

func TestParseOperations_CreateTable(t *testing.T) {
	sql := []byte(`-- +goose Up
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- +goose Down
DROP TABLE IF EXISTS users;
`)
	ops := ParseOperations(sql)
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d: %+v", len(ops), ops)
	}
	if ops[0].Kind != OpCreateTable || ops[0].EntityName != "users" {
		t.Errorf("expected CREATE TABLE users, got %+v", ops[0])
	}
}

func TestParseOperations_IgnoresDownSection(t *testing.T) {
	sql := []byte(`-- +goose Up
CREATE TABLE orders (
    id SERIAL PRIMARY KEY
);

-- +goose Down
DROP TABLE IF EXISTS orders;
ALTER TABLE users DROP COLUMN email;
`)
	ops := ParseOperations(sql)
	if len(ops) != 1 {
		t.Fatalf("expected 1 op (only Up section), got %d: %+v", len(ops), ops)
	}
	if ops[0].Kind != OpCreateTable {
		t.Errorf("expected CREATE TABLE, got %s", ops[0].Kind)
	}
}

func TestParseOperations_AlterTable(t *testing.T) {
	sql := []byte(`-- +goose Up
ALTER TABLE users ADD COLUMN email VARCHAR(255);

-- +goose Down
ALTER TABLE users DROP COLUMN email;
`)
	ops := ParseOperations(sql)
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d: %+v", len(ops), ops)
	}
	if ops[0].Kind != OpAlterTable || ops[0].EntityName != "users" {
		t.Errorf("expected ALTER TABLE users, got %+v", ops[0])
	}
}

func TestParseOperations_CreateIndex(t *testing.T) {
	sql := []byte(`-- +goose Up
CREATE TABLE payments (id SERIAL PRIMARY KEY, status VARCHAR(50));
CREATE INDEX idx_payments_status ON payments (status);
CREATE UNIQUE INDEX idx_payments_order ON payments (order_id);

-- +goose Down
DROP TABLE IF EXISTS payments;
`)
	ops := ParseOperations(sql)

	createTableCount := 0
	createIndexCount := 0
	for _, op := range ops {
		switch op.Kind {
		case OpCreateTable:
			createTableCount++
			if op.EntityName != "payments" {
				t.Errorf("expected entity payments, got %s", op.EntityName)
			}
		case OpCreateIndex:
			createIndexCount++
			if op.EntityName != "payments" {
				t.Errorf("expected index on payments, got %s", op.EntityName)
			}
		}
	}
	if createTableCount != 1 {
		t.Errorf("expected 1 CREATE TABLE, got %d", createTableCount)
	}
	if createIndexCount != 2 {
		t.Errorf("expected 2 CREATE INDEX, got %d", createIndexCount)
	}
}

func TestParseOperations_IfNotExists(t *testing.T) {
	sql := []byte(`-- +goose Up
CREATE TABLE IF NOT EXISTS notifications (id SERIAL PRIMARY KEY);

-- +goose Down
DROP TABLE IF EXISTS notifications;
`)
	ops := ParseOperations(sql)
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d", len(ops))
	}
	if ops[0].Kind != OpCreateTable || ops[0].EntityName != "notifications" {
		t.Errorf("expected CREATE TABLE notifications, got %+v", ops[0])
	}
}

func TestParseOperations_Mixed(t *testing.T) {
	sql := []byte(`-- +goose Up
CREATE TABLE cart (id SERIAL PRIMARY KEY, user_id INTEGER);
CREATE UNIQUE INDEX idx_cart_user ON cart (user_id);
INSERT INTO settings (key, value) VALUES ('cart_enabled', 'true');
UPDATE config SET version = 2;
DELETE FROM temp_data;

-- +goose Down
DROP TABLE IF EXISTS cart;
`)
	ops := ParseOperations(sql)

	kindCounts := make(map[OpKind]int)
	for _, op := range ops {
		kindCounts[op.Kind]++
	}

	if kindCounts[OpCreateTable] != 1 {
		t.Errorf("expected 1 CREATE TABLE, got %d", kindCounts[OpCreateTable])
	}
	if kindCounts[OpCreateIndex] != 1 {
		t.Errorf("expected 1 CREATE INDEX, got %d", kindCounts[OpCreateIndex])
	}
	if kindCounts[OpInsert] != 1 {
		t.Errorf("expected 1 INSERT, got %d", kindCounts[OpInsert])
	}
	if kindCounts[OpUpdate] != 1 {
		t.Errorf("expected 1 UPDATE, got %d", kindCounts[OpUpdate])
	}
	if kindCounts[OpDelete] != 1 {
		t.Errorf("expected 1 DELETE, got %d", kindCounts[OpDelete])
	}
}

func TestParseOperations_NoGooseMarkers(t *testing.T) {
	sql := []byte(`CREATE TABLE simple (id SERIAL PRIMARY KEY);`)
	ops := ParseOperations(sql)
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d", len(ops))
	}
	if ops[0].Kind != OpCreateTable || ops[0].EntityName != "simple" {
		t.Errorf("expected CREATE TABLE simple, got %+v", ops[0])
	}
}

func TestParseOperations_DropTable(t *testing.T) {
	sql := []byte(`-- +goose Up
DROP TABLE IF EXISTS legacy_data;

-- +goose Down
`)
	ops := ParseOperations(sql)
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d", len(ops))
	}
	if ops[0].Kind != OpDropTable || ops[0].EntityName != "legacy_data" {
		t.Errorf("expected DROP TABLE legacy_data, got %+v", ops[0])
	}
}

func TestParseOperations_CreateSequence(t *testing.T) {
	sql := []byte(`-- +goose Up
CREATE SEQUENCE IF NOT EXISTS order_seq;

-- +goose Down
DROP SEQUENCE IF EXISTS order_seq;
`)
	ops := ParseOperations(sql)
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d", len(ops))
	}
	if ops[0].Kind != OpCreateSequence || ops[0].EntityName != "order_seq" {
		t.Errorf("expected CREATE SEQUENCE order_seq, got %+v", ops[0])
	}
}

func TestExtractDefinedEntities(t *testing.T) {
	files := []hashchain.MigrationFile{
		{Version: 1, Content: []byte(`-- +goose Up
CREATE TABLE users (id SERIAL PRIMARY KEY);
-- +goose Down
DROP TABLE users;`)},
		{Version: 2, Content: []byte(`-- +goose Up
CREATE TABLE orders (id SERIAL PRIMARY KEY);
CREATE INDEX idx_orders_user ON orders (user_id);
-- +goose Down
DROP TABLE orders;`)},
		{Version: 3, Content: []byte(`-- +goose Up
ALTER TABLE users ADD COLUMN email VARCHAR(255);
-- +goose Down
ALTER TABLE users DROP COLUMN email;`)},
	}

	entities := ExtractDefinedEntities(files)

	if !entities["users"] {
		t.Error("expected 'users' in entities")
	}
	if !entities["orders"] {
		t.Error("expected 'orders' in entities")
	}
	if len(entities) != 2 {
		t.Errorf("expected 2 entities, got %d: %v", len(entities), entities)
	}
}

func TestClassifyConflict_SoftNewTable(t *testing.T) {
	ops := []SQLOp{
		{Kind: OpCreateTable, EntityName: "notifications"},
		{Kind: OpCreateIndex, EntityName: "notifications"},
	}
	mainEntities := map[string]bool{"users": true, "orders": true}

	if got := ClassifyConflict(ops, mainEntities); got != SoftConflict {
		t.Errorf("expected soft, got %s", got)
	}
}

func TestClassifyConflict_SoftCreateIndex(t *testing.T) {
	ops := []SQLOp{
		{Kind: OpCreateIndex, EntityName: "users"},
	}
	mainEntities := map[string]bool{"users": true}

	if got := ClassifyConflict(ops, mainEntities); got != SoftConflict {
		t.Errorf("creating index on main table should be soft, got %s", got)
	}
}

func TestClassifyConflict_HardAlterMainTable(t *testing.T) {
	ops := []SQLOp{
		{Kind: OpAlterTable, EntityName: "users"},
	}
	mainEntities := map[string]bool{"users": true, "orders": true}

	if got := ClassifyConflict(ops, mainEntities); got != HardConflict {
		t.Errorf("expected hard, got %s", got)
	}
}

func TestClassifyConflict_HardCreateExistingTable(t *testing.T) {
	ops := []SQLOp{
		{Kind: OpCreateTable, EntityName: "users"},
	}
	mainEntities := map[string]bool{"users": true}

	if got := ClassifyConflict(ops, mainEntities); got != HardConflict {
		t.Errorf("creating already-defined table should be hard, got %s", got)
	}
}

func TestClassifyConflict_HardDropMainTable(t *testing.T) {
	ops := []SQLOp{
		{Kind: OpDropTable, EntityName: "orders"},
	}
	mainEntities := map[string]bool{"users": true, "orders": true}

	if got := ClassifyConflict(ops, mainEntities); got != HardConflict {
		t.Errorf("dropping main table should be hard, got %s", got)
	}
}

func TestClassifyConflict_HardDMLOnMainTable(t *testing.T) {
	ops := []SQLOp{
		{Kind: OpInsert, EntityName: "users"},
	}
	mainEntities := map[string]bool{"users": true}

	if got := ClassifyConflict(ops, mainEntities); got != HardConflict {
		t.Errorf("insert into main table should be hard, got %s", got)
	}
}

func TestClassifyConflict_SoftNewSequence(t *testing.T) {
	ops := []SQLOp{
		{Kind: OpCreateSequence, EntityName: "invoice_seq"},
	}
	mainEntities := map[string]bool{"users": true}

	if got := ClassifyConflict(ops, mainEntities); got != SoftConflict {
		t.Errorf("new sequence should be soft, got %s", got)
	}
}

func TestClassifyConflict_HardDropIndex(t *testing.T) {
	ops := []SQLOp{
		{Kind: OpDropIndex, EntityName: "idx_users_email"},
	}
	mainEntities := map[string]bool{"users": true}

	if got := ClassifyConflict(ops, mainEntities); got != HardConflict {
		t.Errorf("dropping an index should be hard, got %s", got)
	}
}

func TestClassifyConflict_MixedSoftAndHard(t *testing.T) {
	ops := []SQLOp{
		{Kind: OpCreateTable, EntityName: "notifications"},
		{Kind: OpAlterTable, EntityName: "users"},
	}
	mainEntities := map[string]bool{"users": true}

	if got := ClassifyConflict(ops, mainEntities); got != HardConflict {
		t.Errorf("mixed ops with any hard should be hard, got %s", got)
	}
}

func TestClassifyConflict_EmptyOps(t *testing.T) {
	mainEntities := map[string]bool{"users": true}
	if got := ClassifyConflict(nil, mainEntities); got != SoftConflict {
		t.Errorf("no ops should be soft, got %s", got)
	}
}
