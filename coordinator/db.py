from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg.rows import dict_row


class Database:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self._dsn, row_factory=dict_row) as conn:
            yield conn

