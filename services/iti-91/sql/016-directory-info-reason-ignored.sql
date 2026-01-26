-- Adds a human-readable reason/comment for why a directory is ignored/skipped.

ALTER TABLE directory_info ADD COLUMN IF NOT EXISTS reason_ignored TEXT;
