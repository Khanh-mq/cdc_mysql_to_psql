import enum
import logging
import json
from typing import Any, Dict, Optional
from datetime import datetime
import inspect


def setup_logger(level: int = logging.INFO, log_file: Optional[str] = None) -> None:
    """setup logger cho ứng dụng"""
    log_fromatter =  logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s'
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    #  xoa toan bo handler cu
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_fromatter)
    root_logger.addHandler(console_handler)

    #  su ly neu co log_file thif them file handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(log_fromatter)
        root_logger.addHandler(file_handler)
    

    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.INFO)

def log_function_call(func):
    """decorator tu log function call voi tham so truyen vao

    Args:
        func (_type_): _description_
    """
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)

        #  get function name and parameters
        func_name = func.__name__
        arg_names =  inspect.getfullargspec(func).args
        arg_values =  args[1:] if 'self' in arg_names or 'cls' in arg_names else args


        params = {}
        for i , arg_name in enumerate(arg_names):
            if i < len (arg_values):
                #  gioi han chieu dai cua tham so truyen vao
                params[arg_name] = str(arg_values[i])[:100]
        params.update({k:str(v)[:100] for k,v in kwargs.items()})

        logger.debug(f"Calling function {func_name} with parameters: {json.dumps(params)}")


        try:
            result =  func(*args, **kwargs)
            logger.debug(f"Function {func_name} returned: {str(result)[:100]}")
            return result
        except Exception as e:
            logger.error(f"Exception in function {func_name}: {str(e)}", exc_info=True)
            raise e
    return wrapper

def log_batch_metrics(batch_id: int , metrics: Dict[str , Any]) -> None:
    """log metrics cho moi batch

    Args:
        batch_id (int): id cua batch
        metrics (Dict[str, Any]): dictionary chua cac metrics
    """
    logger =  logging.getLogger(__name__)
    logger.info(
        f'batch_id: {batch_id} metrics :'
        f'processed = {metrics.get('processed', 0)}, ',
        f'inserted = {metrics.get("inserted", 0)}, ',
        f'updated = {metrics.get("updated", 0)}, ',
        f'deleted = {metrics.get("deleted", 0)}, ',
        f'errors = {metrics.get("errors", 0)}, ',
        f'duration = {metrics.get("duration_ms", 0):.2f}ms'
    )