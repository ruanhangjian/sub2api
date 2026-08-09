-- Add a group-scoped scheduling switch without removing the membership.
ALTER TABLE account_groups
    ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS idx_account_groups_group_enabled
    ON account_groups(group_id, enabled);
