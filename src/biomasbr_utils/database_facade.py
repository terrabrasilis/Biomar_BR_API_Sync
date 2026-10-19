"""Database utilities."""

from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urlparse

from psycopg2 import connect
from psycopg2.extensions import connection
from pydantic import BaseModel

from biomasbr_utils.logger import TasksLogger


logger = TasksLogger("DATABASE")
logger.setLoggerLevel(None)


def get_connection_components(db_url: str):
    parsed_url = urlparse(db_url)
    user = parsed_url.username
    password = parsed_url.password
    host = parsed_url.hostname
    port = parsed_url.port
    db_name = parsed_url.path[1:]
    return user, password, host, str(port), db_name


class DatabaseFacade(BaseModel):
    """Database facade."""

    user: str
    password: str
    host: str
    port: str
    db_name: str
    _conn: Optional[connection] = None

    @classmethod
    def from_url(cls, db_url: str) -> "DatabaseFacade":
        user, password, host, port, db_name = get_connection_components(db_url)
        return cls(
            user=user,
            password=password,
            host=host,
            port=port,
            db_name=db_name,
        )

    @property
    def db_url(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    @property
    def conn(self):
        if self._conn is None:
            self._conn = connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                dbname=self.db_name,
            )
            self._conn.set_isolation_level(1)
            assert self._conn.status == 1
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def execute(self, sql: str, logger: TasksLogger = None):
        if logger:
            logger.debug(" ".join(sql.split()))
        cursor = self.conn.cursor()
        cursor.execute(sql)
        rowcount = cursor.rowcount
        return rowcount

    def fetchall(self, sql: str, logger: TasksLogger = None):
        if logger:
            logger.debug(sql.strip())
        cursor = self.conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        return data

    def fetchone(self, sql: str, logger: TasksLogger = None):
        if logger:
            logger.debug(sql.strip())
        cursor = self.conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchone()
        cursor.close()
        return data

    def insert(self, query: str, data: Any, logger: TasksLogger = None):
        if logger:
            logger.debug(query.strip())
        cursor = self.conn.cursor()
        cursor.executemany(query, data)
        cursor.close()

    def create_schema(self, name: str, comment: str = "", force_recreate: bool = False):
        sql = ""
        if force_recreate:
            sql += f"DROP SCHEMA IF EXISTS {name} CASCADE;"
        sql += f"CREATE SCHEMA IF NOT EXISTS {name} AUTHORIZATION {self.user};"
        if comment:
            sql += f"COMMENT ON SCHEMA {name} IS {comment};"
        sql += f"GRANT ALL ON SCHEMA {name} TO {self.user};"
        self.execute(sql)

    def create_table(self, schema: str, name: str, columns: list, force_recreate: bool = False):
        table = f'{schema}."{name}"'
        sql = ""
        if force_recreate:
            sql += f"DROP TABLE IF EXISTS {table};"
        sql += f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                {", ".join(columns)}
            )
        """
        self.execute(sql)

    def truncate(self, table: str, cascade: bool = False):
        _cascade = "CASCADE" if cascade else ""
        sql = f"TRUNCATE {table} {_cascade};"
        self.execute(sql=sql)

    def drop_table(self, table: str, cascade: bool = False):
        _cascade = "CASCADE" if cascade else ""
        self.execute(f"DROP TABLE IF EXISTS {table} {_cascade};")

    def table_exist(self, table: str, schema: str):
        query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
            );
        """
        return self.fetchone(query=query)
