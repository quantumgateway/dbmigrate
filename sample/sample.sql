-- version: 1.0.1
-- description: Create sample users table

CREATE TABLE IF NOT EXISTS users (
    id UUID DEFAULT generateUUIDv4(),
    username String,
    email String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY created_at;
