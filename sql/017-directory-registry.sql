-- Add directory registry tables (providers + provider-directory mapping) and mark directory origins

CREATE TABLE IF NOT EXISTS directory_providers (
    id SERIAL PRIMARY KEY,
    url VARCHAR NOT NULL UNIQUE,
    enabled BOOLEAN DEFAULT TRUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    last_refresh_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS directory_provider_directories (
    provider_id INTEGER NOT NULL,
    directory_id VARCHAR NOT NULL,
    first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    last_seen_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    removed_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (provider_id, directory_id),
    CONSTRAINT fk_provider FOREIGN KEY (provider_id) REFERENCES directory_providers(id) ON DELETE CASCADE,
    CONSTRAINT fk_directory FOREIGN KEY (directory_id) REFERENCES directory_info(id) ON DELETE CASCADE
);

ALTER TABLE directory_info
    ADD COLUMN IF NOT EXISTS origin VARCHAR NOT NULL DEFAULT 'provider';

CREATE INDEX IF NOT EXISTS idx_directory_info_endpoint_address ON directory_info(endpoint_address);
CREATE INDEX IF NOT EXISTS idx_provider_directories_provider ON directory_provider_directories(provider_id);
CREATE INDEX IF NOT EXISTS idx_provider_directories_directory ON directory_provider_directories(directory_id);
