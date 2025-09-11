-- Unify directory_info, directory_cache, and ignored_directory into a single table

-- Create new table with unified schema
CREATE TABLE IF NOT EXISTS directory_info_new (
    id VARCHAR PRIMARY KEY NOT NULL UNIQUE,
    endpoint_address VARCHAR NOT NULL,
    failed_sync_count INTEGER DEFAULT 0 NOT NULL,
    failed_attempts INTEGER DEFAULT 0 NOT NULL,
    last_success_sync TIMESTAMP WITH TIME ZONE,
    is_ignored BOOLEAN DEFAULT FALSE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    deleted_at TIMESTAMP WITH TIME ZONE -- describes if in the future, when the data will be deleted.
);