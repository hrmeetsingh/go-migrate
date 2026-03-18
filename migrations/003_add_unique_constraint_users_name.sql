-- +goose Up
ALTER TABLE users ADD CONSTRAINT uq_users_name UNIQUE (name);

-- +goose Down
ALTER TABLE users DROP CONSTRAINT IF EXISTS uq_users_name;
