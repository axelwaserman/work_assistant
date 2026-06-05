-- Slack user cache. Refreshed weekly per docs/04-ingestion-pipelines.md §4.1.

CREATE TABLE slack_users (
  user_id      TEXT PRIMARY KEY,
  email        TEXT,
  display_name TEXT NOT NULL,
  fetched_at   INTEGER NOT NULL
);

CREATE INDEX idx_slack_users_fetched_at ON slack_users(fetched_at);
