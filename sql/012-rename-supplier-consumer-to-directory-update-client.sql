-- Migration to rename all supplier/consumer references to directory/update_client
-- This migration updates all database schema elements to use the new naming conventions from https://profiles.ihe.net/ITI/mCSD/4.0.0-comment/issues.html#section

CREATE ROLE mcsd_update_client;
ALTER ROLE mcsd_update_client WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION NOBYPASSRLS;

CREATE ROLE mcsd_update_client_dba;  
ALTER ROLE mcsd_update_client_dba WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION NOBYPASSRLS;

ALTER TABLE supplier_cache RENAME TO directory_cache;

ALTER TABLE supplier_info RENAME TO directory_info;

ALTER TABLE supplier_ignored_directories RENAME TO ignored_directories;


ALTER TABLE resource_maps RENAME COLUMN supplier_id TO directory_id;
ALTER TABLE resource_maps RENAME COLUMN supplier_resource_id TO directory_resource_id;
ALTER TABLE resource_maps RENAME COLUMN consumer_resource_id TO update_client_resource_id;

ALTER TABLE directory_cache RENAME COLUMN supplier_id TO directory_id;

ALTER TABLE directory_info RENAME COLUMN supplier_id TO directory_id;


ALTER TABLE deploy_releases OWNER TO mcsd_update_client_dba;
ALTER TABLE resource_maps OWNER TO mcsd_update_client_dba;
ALTER TABLE directory_cache OWNER TO mcsd_update_client_dba;
ALTER TABLE directory_info OWNER TO mcsd_update_client_dba;
ALTER TABLE ignored_directories OWNER TO mcsd_update_client_dba;


GRANT SELECT ON deploy_releases TO mcsd_update_client;
GRANT SELECT, UPDATE, DELETE, INSERT ON resource_maps TO mcsd_update_client;
GRANT SELECT, UPDATE, DELETE, INSERT ON directory_cache TO mcsd_update_client;
GRANT SELECT, UPDATE, DELETE, INSERT ON directory_info TO mcsd_update_client;
GRANT SELECT, UPDATE, DELETE, INSERT ON ignored_directories TO mcsd_update_client;

-- Revoke all permissions from old roles before dropping them
REVOKE ALL PRIVILEGES ON deploy_releases FROM mcsd_consumer;
REVOKE ALL PRIVILEGES ON resource_maps FROM mcsd_consumer;
REVOKE ALL PRIVILEGES ON directory_cache FROM mcsd_consumer;
REVOKE ALL PRIVILEGES ON ignored_directories FROM mcsd_consumer;

-- Drop old roles (now that their privileges have been revoked)
DROP ROLE IF EXISTS mcsd_consumer;
DROP ROLE IF EXISTS mcsd_consumer_dba;

