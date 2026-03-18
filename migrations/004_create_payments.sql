-- +goose Up
CREATE TABLE payments (
    id         SERIAL PRIMARY KEY,
    order_id   INTEGER        NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    amount     DECIMAL(10,2)  NOT NULL,
    method     VARCHAR(50)    NOT NULL,
    status     VARCHAR(50)    NOT NULL DEFAULT 'pending',
    paid_at    TIMESTAMPTZ,
    created_at TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payments_order_id ON payments (order_id);
CREATE INDEX idx_payments_status   ON payments (status);

-- +goose Down
DROP TABLE IF EXISTS payments;
