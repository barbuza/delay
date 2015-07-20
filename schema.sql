CREATE EXTENSION IF NOT EXISTS jsquery;
CREATE OR REPLACE LANGUAGE PLPYTHONU;

CREATE OR REPLACE FUNCTION merge_json(left jsonb, right jsonb)
RETURNS jsonb AS $$
import json
l, r = json.loads(left), json.loads(right)
l.update(r)
j = json.dumps(l)
return j
$$ LANGUAGE PLPYTHONU IMMUTABLE;

DROP TABLE IF EXISTS j;

CREATE TABLE j (
  id serial primary key,
  data jsonb not null
);

CREATE INDEX j_index ON j USING gin (data jsonb_path_value_ops);
--
insert into j (id, data) values (1, '{"child":2, "parent": 4}');
insert into j (id, data) values (2, '{"child":3, "parent": 1}');
insert into j (id, data) values (3, '{"child":4, "parent": 2}');
insert into j (id, data) values (4, '{"child":1, "parent": 3}');
