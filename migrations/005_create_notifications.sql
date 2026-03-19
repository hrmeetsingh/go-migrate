-- +goose Up
-- Migration: create_notifications
-- Git commit: 69109ccd66749b9e454118ea56bc945690f73594
-- Created: 2026-03-18T21:35:04-06:00

CREATE TABLE notifications (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel     VARCHAR(50)  NOT NULL DEFAULT 'in_app',
    title       VARCHAR(255) NOT NULL,
    body        TEXT,
    read        BOOLEAN      NOT NULL DEFAULT FALSE,
    sent_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    read_at     TIMESTAMPTZ,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_notifications_user_id ON notifications (user_id);
CREATE INDEX idx_notifications_read    ON notifications (user_id, read);

-- +goose Down
DROP TABLE IF EXISTS notifications;
