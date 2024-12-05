--- Create web user addressing
CREATE ROLE mcsd_consumer;
ALTER ROLE mcsd_consumer WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION NOBYPASSRLS ;

--- Create DBA role
CREATE ROLE mcsd_consumer_dba;
ALTER ROLE mcsd_consumer_dba WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION NOBYPASSRLS ;

CREATE TABLE deploy_releases
(
        version varchar(255),
        deployed_at timestamp default now()
);

ALTER TABLE deploy_releases OWNER TO mcsd_consumer_dba;

GRANT SELECT ON deploy_releases TO mcsd_consumer;
