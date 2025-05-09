GRANT SELECT, UPDATE, DELETE, INSERT ON supplier_cache TO mcsd_consumer;
CREATE TABLE supplier_directory_ignore (
    directory_id VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
GRANT SELECT, UPDATE, DELETE, INSERT ON supplier_directory_ignore TO mcsd_consumer;