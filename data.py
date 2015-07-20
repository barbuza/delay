import asyncio

from psycopg2.extras import Json
import aiopg


class Entity:
    __slots__ = ('id', 'data')

    def __init__(self, id, data):
        self.id = id
        self.data = data

    def refs(self, follow):
        refs = set()
        for key in follow:
            val = self.data.get(key, None)
            if isinstance(val, int):
                refs.add(val)
        return refs


class Store:
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.pool = None

    @asyncio.coroutine
    def connect(self, dsn):
        self.pool = yield from aiopg.create_pool(dsn, loop=self.loop, enable_json=True, enable_hstore=False)

    @asyncio.coroutine
    def update_schema(self):
        with (yield from self.pool.cursor()) as cursor:
            yield from cursor.execute('BEGIN')
            yield from cursor.execute("SELECT relname FROM pg_class WHERE relname = 'j'")
            data = yield from cursor.fetchall()
            if not data:
                yield from cursor.execute('CREATE TABLE j (id SERIAL PRIMARY KEY, data JSONB)')
                yield from cursor.execute('CREATE INDEX j_index ON j USING GIN (data jsonb_path_value_ops)')
                yield from cursor.execute('INSERT INTO j (data) VALUES (%s), (%s), (%s), (%s)', (
                    Json({'id': 1, 'parent': 4, 'child': 2}),
                    Json({'id': 2, 'parent': 1, 'child': 3}),
                    Json({'id': 3, 'parent': 2, 'child': 4}),
                    Json({'id': 4, 'parent': 3, 'child': 1}),
                ))
            yield from cursor.execute('COMMIT')

    @asyncio.coroutine
    def persist(self, id, data):
        with (yield from self.pool.cursor()) as cursor:
            yield from cursor.execute('BEGIN')

            if id:
                data['id'] = id
                yield from cursor.execute('UPDATE j SET data = %s WHERE id = %s RETURNING id', (Json(data), id))
            else:
                yield from cursor.execute('INSERT INTO j (data) VALUES (%s) RETURNING id', (Json(data),))
                id, = yield from cursor.fetchone()
                data['id'] = id
                yield from cursor.execute('UPDATE j SET data = %s WHERE id = %s RETURNING id', (Json(data), id))

            row = yield from cursor.fetchone()
            yield from cursor.execute('COMMIT')

            if row is None:
                return None

            return Entity(id, data)

    @asyncio.coroutine
    def fetch_list(self, query, follow, depth):
        if isinstance(query, list) and not query:
            return {}

        if not depth:
            print('no depth')
            return {}

        entities = {}

        ids = query
        if isinstance(ids, list):
            ids = set(ids)

        with (yield from self.pool.cursor()) as cursor:
            while True:
                if isinstance(ids, set):
                    yield from cursor.execute('SELECT id, data FROM j WHERE id = ANY(%s)', (list(ids),))
                elif isinstance(ids, str):
                    yield from cursor.execute('SELECT id, data FROM j WHERE data @@ %s::jsquery', (ids, ))
                else:
                    raise RuntimeError('unknown query type {}'.format(type(ids)))

                rows = yield from cursor.fetchall()

                # collect entities and new references
                ids = set()
                for row in rows:
                    entity = Entity(*row)
                    entities[entity.id] = entity
                    ids = ids.union(entity.refs(follow))

                # remove references to known entities
                ids = ids.difference(set(entities.keys()))

                depth -= 1
                if not ids or not depth:
                    break

        return entities
