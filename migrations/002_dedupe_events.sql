-- Enforce that each event_id is stored at most once.
--
-- internal/store.InsertEvent relies on this constraint via
-- "INSERT ... ON CONFLICT (event_id) DO NOTHING" to make webhook
-- deduplication atomic. Before this, event_id only had a plain (non-unique)
-- index, so two concurrent redeliveries of the same event could both pass a
-- separate "does it exist" check before either had written, and both would
-- proceed to insert a row and double-increment account_stats.
DROP INDEX IF EXISTS idx_events_event_id;
ALTER TABLE events ADD CONSTRAINT events_event_id_key UNIQUE (event_id);
