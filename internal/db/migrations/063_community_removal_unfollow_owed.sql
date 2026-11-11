-- +goose Up
-- A community removal owes Lemmy an Undo{Follow} when the community was
-- followed (pending or accepted) at the removal. unfollow_owed_at records it
-- in the removal's transaction and stays set until an Undo{Follow} is
-- delivered, so a failed send or a crash before the send is retried by the
-- follow retrier. NULL is nothing owed.
ALTER TABLE community_removal_suppressions ADD COLUMN unfollow_owed_at TIMESTAMPTZ;

CREATE INDEX community_removal_suppressions_unfollow_owed
    ON community_removal_suppressions (id) WHERE unfollow_owed_at IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS community_removal_suppressions_unfollow_owed;
ALTER TABLE community_removal_suppressions DROP COLUMN IF EXISTS unfollow_owed_at;
