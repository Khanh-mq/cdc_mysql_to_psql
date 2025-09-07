import re
from urllib.parse import urlparse
from psycopg2 import pool
from contextlib import contextmanager
import logging
from typing import Generator


from src.utils.error_handling import retry, PipelineError , handle_database_errors 


logger  = logging.getLogger(__name__)

class ConnectionPoolManager:
    def __init__(self, postgres_config):
        self.postgres_config = postgres_config
        self.connection_pool = None
        self.pool =  None 
        self._initialize_pool()

    @retry(max_retries=3 , delay=1)
    def _initialize_pool(self):
        """postgres connection pool with retry logic

        Raises:
            PipelineError: _description_
        """
        dsn  = self.postgres_config.url.replace("jdbc:", "")
        parsed = urlparse(dsn)

        host = parsed.hostname
        port = parsed.port
        dbname = parsed.path.lstrip("/")
        self.pool = pool.SimpleConnectionPool(
            1,  # minconn
            10, # maxconn
            dsn=dsn,
            user=self.postgres_config.user,
            password=self.postgres_config.password,
            host=host,
            port=port,
            database=dbname,
            connect_timeout=5
        )
        logger.info("Postgres connection pool initialized.")
    

    @contextmanager
    def get_connection(self) -> Generator:
        """get connection from pool with conext manager"""
        conn =  None 
        try:
            if not self.pool:
                raise PipelineError("Connection pool is not initialized.")
            conn = self.pool.getconn()
            yield conn
        except Exception as e:
            logger.error(f"Error getting connection from pool: {str(e)}", exc_info=True)
            raise PipelineError("Error getting connection from pool", e) from e
        finally:
            if conn and self.pool:
                self.pool.putconn(conn)
    def close_all_connections(self):
        """close all connections in the pool"""
        if self.pool:
            self.pool.closeall()
            logger.info("All connections in the pool have been closed.")

