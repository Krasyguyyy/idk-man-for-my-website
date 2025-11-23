import pymysql
from pymysql import err as errors
from pymysql import cursors as pymysql_cursors


class MySQLCursorWrapper:
    """Wrapper to mimic mysql.connector cursor using PyMySQL."""
    def __init__(self, pym_cursor):
        self._cur = pym_cursor

    def execute(self, query, params=None):
        return self._cur.execute(query, params)

    def executemany(self, query, params):
        return self._cur.executemany(query, params)

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def fetchmany(self, size=None):
        return self._cur.fetchmany(size)

    def close(self):
        return self._cur.close()

    @property
    def rowcount(self):
        return self._cur.rowcount

    @property
    def lastrowid(self):
        return self._cur.lastrowid


class MySQLConnectionWrapper:
    """Wrapper for PyMySQL Connection to look like mysql.connector."""
    def __init__(self, **kwargs):
        self._conn = pymysql.connect(
            host=kwargs.get("host", "localhost"),
            user=kwargs.get("user"),
            password=kwargs.get("password"),
            database=kwargs.get("database"),
            port=kwargs.get("port", 3306),
            autocommit=kwargs.get("autocommit", False),
            charset=kwargs.get("charset", "utf8mb4"),
            cursorclass=pymysql_cursors.Cursor
        )

    def cursor(self, buffered=False, dictionary=False):
        """Support mysql.connector's cursor options."""
        if dictionary:
            cur = self._conn.cursor(pymysql_cursors.DictCursor)
        else:
            cur = self._conn.cursor()

        return MySQLCursorWrapper(cur)

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        return self._conn.close()


def connect(**kwargs):
    """Replacement for mysql.connector.connect."""
    return MySQLConnectionWrapper(**kwargs)


# Expose mysql.connector-compatible error names
Error = errors.MySQLError
InterfaceError = errors.InterfaceError
DatabaseError = errors.DatabaseError
OperationalError = errors.OperationalError
ProgrammingError = errors.ProgrammingError
IntegrityError = errors.IntegrityError
DataError = errors.DataError
NotSupportedError = errors.NotSupportedError
