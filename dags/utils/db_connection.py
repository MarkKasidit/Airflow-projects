from abc import abstractmethod
import psycopg2
import psycopg2.extras
# from logger import Logger
import logging as logger
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import os
from dotenv import load_dotenv

load_dotenv()

config = {}

config["DB_HOST"] = os.environ.get("DB_HOST")
config["DB_NAME"] = os.environ.get("DB_DATABASE")
config["DB_USERNAME"] = os.environ.get("DB_USERNAME")
config["DB_PASSWORD"] = os.environ.get("DB_PASSWORD")
config["DB_PORT"] = os.environ.get("DB_PORT")

class SqlRunner:
    _logger = None
    _conn = None
    _engine = None

    # def __init__(self, logger: Logger):
    def __init__(self):
        """ a SqlRunner is used to hold a connection to the database and can
            execute queries on that connection

        :param logger: a logger object used to capture diagnostic messages

        :return: nothing
        """
        self._logger = logger
        self._get_connection()

    @abstractmethod
    def _get_connection(self):
        """
        Retrieves the connection string and returns a new connection object.
        """

    @abstractmethod
    def _get_sql_engine(self):
        """
        Retrieves the connection string and returns a new SQLAlchemy engine.
        """

    def runquery(self, query, batch=False):
        """ 
        This method will run a query on the current connection owned by
        this object

            Parameters
            ----------
            query: str - The string holding the SQL query to execute

            batch: boolean - Whether a cursor will be returned to fetch in batches
            or the entire result set will be returned

        """
        self._get_connection()
        cursor = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        try:
            result = cursor.execute(query)
        except (psycopg2.OperationalError, psycopg2.InternalError) as e:
            self._logger.exception("Database exception while execute the sql: {0}, error={1}".format(query, e))
            raise
        except psycopg2.Error as e:
            result = None
            error_msg = e.diag.severity + ': ' + e.diag.source_function + ': '\
                + e.diag.message_primary
            self._logger.error(error_msg)
            self._logger.info("sql command: {0}".format(query))
            raise
        
        if batch:
            return cursor
        else:
            try:
                rows = cursor.fetchall()
            except:
                rows = None
            self._conn.commit()
            return rows

    def get_header(self, table_name: str):
        self._get_connection()
        cursor = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = "SELECT * FROM {} LIMIT 1".format(table_name)
        cursor.execute(query)
        colnames = [desc[0] for desc in cursor.description]
        self._conn.commit()
        return colnames

    def close(self):
        """ the close method is used to explicitly close the connection owned
            by this object
        """
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class PostgresConnector(SqlRunner):
    """
        Connects and interacts with a Postgres database.
    """

    def _get_connection(self):
        if self._conn is None:
            conn_string = """host='{0}' dbname='{1}' user='{2}'
            password='{3}' port='{4}'""".format(
                config["DB_HOST"], config["DB_NAME"], config["DB_USERNAME"],
                config["DB_PASSWORD"], config["DB_PORT"])
            self._logger.info(
                "Database connection: \
                host='{0}' dbname='{1}' user='{2}' port='{3}'".format(
                config["DB_HOST"],
                config["DB_NAME"],
                config["DB_USERNAME"],
                config["DB_PORT"]
                )
            )
            try:
                self._conn = psycopg2.connect(conn_string)
            except psycopg2.OperationalError as e:
                self._logger.error(
                    "SqlRunner failed to connect to cluster\n{0}\n{1}".format(
                        conn_string, e
                    )
                )
                raise


    def _get_sql_engine(self):
        if self._engine is None:
            conn_url = f'''postgresql://{config["DB_USERNAME"]}:{config["DB_PASSWORD"]}@{config["DB_HOST"]}:{config["DB_PORT"]}/{config["DB_NAME"]}'''
            self._logger.info(
                "Database connection: \
                host='{0}' dbname='{1}' user='{2}' port='{3}'".format(
                config["DB_HOST"],
                config["DB_NAME"],
                config["DB_USERNAME"],
                config["DB_PORT"]
                )
            )
            try:
                self._engine = create_engine(conn_url)
            except OperationalError as e:
                self._logger.error(
                    "sql engine failed to create\n{0}\n{1}".format(
                        conn_url, e
                    )
                )
                raise
