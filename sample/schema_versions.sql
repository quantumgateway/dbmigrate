-- version: 1.0.0
-- description: Create schema_versions table for migration tracking

CREATE TABLE IF NOT EXISTS schema_versions (
    version String,
    description String,
    filename String,
    checksum String,
    applied_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY applied_at;
