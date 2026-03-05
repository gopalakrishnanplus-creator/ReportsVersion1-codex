from contextlib import contextmanager
from django.db import connection


@contextmanager
def cursor():
    with connection.cursor() as cur:
        yield cur


def execute(sql: str, params=None) -> None:
    with cursor() as cur:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)


def fetchall(sql: str, params=None):
    with cursor() as cur:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
