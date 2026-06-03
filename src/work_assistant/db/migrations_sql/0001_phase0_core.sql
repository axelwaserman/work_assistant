-- Phase 0 core schema: events, FTS5 mirror, ingest cursors, worker advisory locks.
-- Later phases add embeddings, advisor_memory, proposals, todoist_dedup,
-- review_queue, notes_fetch_queue.

CREATE TABLE events (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  source          TEXT NOT NULL,
  source_id       TEXT NOT NULL,
  source_link     TEXT,
  content_hash    TEXT NOT NULL,
  occurred_at     INTEGER NOT NULL,
  ingested_at     INTEGER NOT NULL,
  actor           TEXT,
  thread_key      TEXT,
  kind            TEXT NOT NULL,
  title           TEXT,
  body            TEXT,
  metadata_json   TEXT,
  UNIQUE(source, source_id)
);

CREATE INDEX idx_events_occurred ON events(occurred_at DESC);
CREATE INDEX idx_events_thread   ON events(thread_key);
CREATE INDEX idx_events_actor    ON events(actor);
CREATE INDEX idx_events_source_kind ON events(source, kind);

CREATE VIRTUAL TABLE events_fts USING fts5(
  title, body,
  content=events,
  content_rowid=id,
  tokenize='porter unicode61'
);

CREATE TRIGGER events_ai AFTER INSERT ON events BEGIN
  INSERT INTO events_fts(rowid, title, body) VALUES (new.id, new.title, new.body);
END;

CREATE TRIGGER events_ad AFTER DELETE ON events BEGIN
  INSERT INTO events_fts(events_fts, rowid, title, body) VALUES('delete', old.id, old.title, old.body);
END;

CREATE TRIGGER events_au AFTER UPDATE ON events BEGIN
  INSERT INTO events_fts(events_fts, rowid, title, body) VALUES('delete', old.id, old.title, old.body);
  INSERT INTO events_fts(rowid, title, body) VALUES (new.id, new.title, new.body);
END;

CREATE TABLE ingest_cursors (
  source       TEXT PRIMARY KEY,
  cursor       TEXT NOT NULL,
  updated_at   INTEGER NOT NULL,
  last_status  TEXT
);

CREATE TABLE worker_locks (
  name         TEXT PRIMARY KEY,
  pid          INTEGER NOT NULL,
  acquired_at  INTEGER NOT NULL
);
