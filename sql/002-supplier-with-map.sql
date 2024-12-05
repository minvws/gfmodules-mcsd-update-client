CREATE TABLE supplier_endpoints
(
  id                CHAR(8)         NOT NULL UNIQUE,
  care_provider_name        VARCHAR(150)    NOT NULL,
  update_supplier_endpoint  VARCHAR         NOT NULL,
  created_at                TIMESTAMP           DEFAULT NOW(),
  modified_at               TIMESTAMP           DEFAULT NOW(),

  PRIMARY KEY (ura_number)
);


CREATE TABLE resource_map (
  id                     uuid         NOT NULL DEFAULT gen_random_uuid(),
  supplier_id           VARCHAR NOT NULL,
  resource_type         VARCHAR NOT NULL,
  supplier_resource_id  VARCHAR NOT NULL UNIQUE,
  supplier_resource_version INT NOT NULL,
  consumer_resource_id  VARCHAR NOT NULL UNIQUE,
  consumer_resource_version INT NOT NULL,
  last_update TIMESTAMP,
  created_at             TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
  modified_at            TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')
);
