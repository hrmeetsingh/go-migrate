-- +goose Up
-- Migration: create_cart
-- Git commit: 69109ccd66749b9e454118ea56bc945690f73594
-- Created: 2026-03-19T06:47:30-06:00

CREATE TABLE cart (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_cart_user_id ON cart (user_id);

CREATE TABLE cart_items (
    id          SERIAL PRIMARY KEY,
    cart_id     INTEGER        NOT NULL REFERENCES cart(id) ON DELETE CASCADE,
    product     VARCHAR(255)   NOT NULL,
    quantity    INTEGER        NOT NULL DEFAULT 1 CHECK (quantity > 0),
    unit_price  DECIMAL(10,2)  NOT NULL,
    added_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_cart_items_cart_id ON cart_items (cart_id);

-- +goose Down
DROP TABLE IF EXISTS cart_items;
DROP TABLE IF EXISTS cart;
