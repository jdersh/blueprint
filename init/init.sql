DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'action') THEN
    CREATE TYPE action AS ENUM ('add', 'delete', 'rename');
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS operation
(
  event varchar,
  action action,
  name varchar,
  action_metadata jsonb,
  version int,
  ordering int,
  ts timestamp without time zone default NOW(),
  user_name varchar,
  PRIMARY KEY (event, version, ordering)
);
