CREATE TABLE supplier_info
(
  id                uuid       NOT NULL      DEFAULT gen_random_uuid(),
  supplier_id       VARCHAR    NOT NULL,
  failed_sync_count INT        NOT NULL      DEFAULT 0,
  failed_attempts   INT        NOT NULL      DEFAULT 0,
  last_success_sync TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')
);
