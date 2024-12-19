CREATE TABLE suppliers
(
  id          CHAR(8)      NOT NULL UNIQUE,
  name        VARCHAR(150) NOT NULL,
  endpoint    VARCHAR      NOT NULL,
  created_at  TIMESTAMP DEFAULT NOW(),
  modified_at TIMESTAMP DEFAULT NOW(),

  PRIMARY KEY (id)
);


CREATE TABLE resource_maps
(
  id                        uuid    NOT NULL         DEFAULT gen_random_uuid(),
  supplier_id               VARCHAR(8) NOT NULL,
  resource_type             VARCHAR NOT NULL,
  supplier_resource_id      VARCHAR NOT NULL,
  supplier_resource_version INT     NOT NULL,
  consumer_resource_id      VARCHAR NOT NULL UNIQUE,
  consumer_resource_version INT     NOT NULL,
  last_update               TIMESTAMP,
  created_at                TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
  modified_at               TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
  UNIQUE (supplier_id, supplier_resource_id)
);
