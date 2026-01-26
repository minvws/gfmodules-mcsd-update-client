-- Drop column id from the supplier_info table and set supplier_id as the primary key
ALTER TABLE supplier_info
  ADD PRIMARY KEY (supplier_id),
  DROP COLUMN IF EXISTS id;

-- Create the supplier_cache table
CREATE TABLE supplier_cache
(
  supplier_id VARCHAR     NOT NULL UNIQUE,
  endpoint    VARCHAR     NOT NULL,
  is_deleted  BOOLEAN     NOT NULL DEFAULT FALSE,

  created_at  TIMESTAMP   DEFAULT NOW(),
  modified_at TIMESTAMP   DEFAULT NOW(),

  PRIMARY KEY (supplier_id)
);

