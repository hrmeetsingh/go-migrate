-- +goose Up
CREATE TABLE orders (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    total      DECIMAL(10,2) NOT NULL DEFAULT 0,
    status     VARCHAR(50)  NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_user_id ON orders (user_id);
CREATE INDEX idx_orders_status  ON orders (status);

-- +goose Down
DROP TABLE IF EXISTS orders;
