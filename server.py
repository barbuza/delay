#!./venv/bin/python

import json
import asyncio
import psycopg2
import click

from aiohttp import web
from data import Store, Entity


class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Entity):
            return dict(o.data, **{'id': o.id})
        return super(JsonEncoder, self).default(o)


@asyncio.coroutine
def fetch_handler(store, request):
    body = yield from request.content.read()

    try:
        payload = json.loads(body.decode('utf-8'))

        depth = payload.get('depth', 1)
        if not isinstance(depth, int):
            raise ValueError("depth isn't int")

        follow = payload.get('follow', [])
        if not isinstance(follow, list):
            raise ValueError("follow isn't a list")

        query = payload.get('query', [])
        if not isinstance(query, list) and not isinstance(query, str):
            raise ValueError("query isn't an array or string")

    except ValueError as err:
        return web.Response(status=400, body=str(err).encode('utf-8'))

    try:
        data = yield from store.fetch_list(query, follow, depth)
    except psycopg2.ProgrammingError as err:
        return web.Response(body=err.diag.message_primary.encode('utf-8'), status=400)

    return web.Response(body=json.dumps(data, cls=JsonEncoder).encode('utf-8'),
                        content_type='application/json')


@asyncio.coroutine
def persist_handler(store, request):
    body = yield from request.content.read()

    try:
        payload = json.loads(body.decode('utf-8'))
        id = payload.pop('id', None)
        if id is not None and not isinstance(id, int):
            raise ValueError()
    except ValueError:
        return web.Response(status=400)

    data = yield from store.persist(id, payload)
    return web.Response(body=json.dumps(data, cls=JsonEncoder).encode('utf-8'),
                        content_type='application/json')


@asyncio.coroutine
def init(loop, store, port):
    app = web.Application(loop=loop)
    app.router.add_route('POST', '/fetch', lambda request: fetch_handler(store, request))
    app.router.add_route('POST', '/persist', lambda request: persist_handler(store, request))
    srv = yield from loop.create_server(app.make_handler(), '127.0.0.1', port)
    return srv


@click.command()
@click.option('--db', default='delay', help='postgres db')
@click.option('--user', default='delay', help='postgres user')
@click.option('--port', default=8080, help='http port')
def main(db, user, port):
    loop = asyncio.get_event_loop()
    store = Store(loop=loop)
    loop.run_until_complete(store.connect('dbname={} user={}'.format(db, user)))
    loop.run_until_complete(store.update_schema())
    loop.run_until_complete(init(loop, store, port))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
