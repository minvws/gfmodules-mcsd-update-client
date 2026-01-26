CREATE TABLE supplier_ignored_directories (
    directory_id VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
GRANT SELECT, UPDATE, DELETE, INSERT ON supplier_ignored_directories TO mcsd_consumer;