import logging
import time
from functools import wraps
from typing import Callable, Type, Any , Optional
from psycopg2 import OperationalError, InterfaceError, DatabaseError    
from pyspark.sql import SparkSession



logger =  logging.getLogger(__name__)

class PipelineError(Exception):
    """Base class for all custom exceptions in the pipeline."""
    def __init__(self, massage: str , original_error: Optional[Exception] = None) -> None:
        self.massage = massage
        self.original_error = original_error
        super().__init__(self.massage)

def retry(
        max_retries: int = 3,
        delay: int = 5,
        backoff: int = 2,
        exceptions: tuple =  (Exception,)
):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            retries = 0 
            current_delay = delay
            while retries<=  max_retries:
                try:
                    return func(*args, **kwargs)
                except OperationalError as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries." , exc_info=True)
                        raise PipelineError(f"Function {func.__name__} failed after {max_retries} retries.", e) from e
                    logger.warning(
                        f"Function {func.__name__} failed (attempt {retries}/{max_retries}). "
                        f"Retrying in {current_delay}s. Error: {str(e)}"
                    )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff  # Exponential backoff
            
            raise PipelineError(f"Function {func.__name__} failed after {max_retries} retries")
        
        return wrapper
    return decorator


def handle_database_errors(func: Callable) -> Callable:
    """decorator de xu ly loi database

    Args:
        func (Callable): _description_

    Returns:
        Callable: _description_
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except OperationalError as e :
            logger.error (f'database connection error ;{str(e)}')
            raise PipelineError("Database connection error", e) from e
        except DatabaseError as e:
            logger.error(f'Database  error: {str(e)}')
            raise PipelineError("Database error", e) from e
        except Exception as e:
            logger.error(f'Unexpected error: {str(e)}', exc_info=True)
            raise PipelineError("Unexpected error", e) from e
    return wrapper


def handle_spark_errors(func: Callable) -> Callable:
    """decorator de xu ly loi spark

    Args:
        func (Callable): _description_

    Returns:
        Callable: _description_
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f'Spark error: {str(e)}', exc_info=True)
            if "Connection refused" in str(e):
                raise PipelineError("Spark connection refused", e) from e
            elif "OutOfMemory" in str(e):
                raise PipelineError("Spark out of memory", e) from e
            else:
                raise PipelineError("Spark operation failed ", e) from e
    return wrapper