-- +goose Up
-- Migration: create_offers
-- Git commit: 69109ccd66749b9e454118ea56bc945690f73594
-- Created: 2026-03-18T21:30:07-06:00

CREATE TABLE offers (
    id          SERIAL PRIMARY KEY,
    title       VARCHAR(255)   NOT NULL,
    description TEXT,
    discount    DECIMAL(5,2)   NOT NULL,
    min_order   DECIMAL(10,2)  NOT NULL DEFAULT 0,
    code        VARCHAR(50)    NOT NULL UNIQUE,
    active      BOOLEAN        NOT NULL DEFAULT TRUE,
    starts_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_offers_code   ON offers (code);
CREATE INDEX idx_offers_active ON offers (active);

-- +goose Down
DROP TABLE IF EXISTS offers;
