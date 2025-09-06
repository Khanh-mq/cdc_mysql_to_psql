import time 
import logging
from functools import wraps
from typing import Callable, Any , Dict
from datetime import datetime

from src.utils.logging_utils import log_batch_metrics


logger =  logging.getLogger(__name__)


class PipelineMetrics:
    """class to track pipeline metrics """

    def __init__(self):
        self.metrics = {
            'batches_processed': 0,
            'total_records_processed': 0,
            'total_inserts': 0,
            'total_updates': 0,
            'total_deletes': 0,
            'total_errors': 0,
            'start_time': datetime.now(),
        }
    def update_metrics(self, batch_metrics: Dict[str ,Any]) -> None:
        """update toàn bộ metrics với batch metrics

        Args:
            batch_metrics (dict[str ,Any]): dữ liêu metrics của batch
        """


        self.metrics['batches_processed'] += 1
        self.metrics['total_records_processed'] += batch_metrics.get('processed', 0)
        self.metrics['total_inserts'] += batch_metrics.get('inserted', 0)
        self.metrics['total_updates'] += batch_metrics.get('updated', 0)
        self.metrics['total_deletes'] += batch_metrics.get('deleted', 0)
        self.metrics['total_errors'] += batch_metrics.get('errors', 0)



    def get_metrics(self) -> Dict[str ,Any]:
        """lấy cái metrics hiện tại

        Returns:
            dict[str ,Any]: _description_
        """
        current_time = datetime.now()
        elapsed_time = (current_time - self.metrics['start_time']).total_seconds()


        return{
            **self.metrics,
            'elapsed_time_seconds': elapsed_time,
            'records_per_second': self.metrics['total_records_processed'] / elapsed_time if elapsed_time > 0 else 0 

        }
    def log_summary(self) -> None:
        """log metrics summary"""
        metrics  =  self.get_metrics()
        logger.info(
            f'Pipeline Summary: '
            f'batches={metrics["batches_processed"]}, '
            f'records={metrics["total_records_processed"]}, '
            f'inserts={metrics["total_inserts"]}, '
            f'updates={metrics["total_updates"]}, '
            f'deletes={metrics["total_deletes"]}, '
            f'errors={metrics["total_errors"]}, '
            f'duration={metrics["elapsed_time_seconds"]:.2f}s, '
            f'rate={metrics["records_per_second"]:.2f} rec/s'
        )
def track_processing_time(func: Callable) -> Callable:
    """decorator để theo dõi thời gian xử lý của một hàm

    Args:
        func (Callable): _description_

    Returns:
        Callable: _description_
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            duration_ms = (time.time() - start_time) * 1000
            logger.debug(f"Function {func.__name__} executed in {duration_ms:.2f}ms")
            return result
        except Exception as e:
            duration_ms =  time.time() - start_time * 1000
            logger.error(f"Function {func.__name__} failed after {duration_ms:.2f}ms with error: {str(e)}", exc_info=True)
            raise e
    return wrapper


def record_batch_metrics(func: Callable) -> Callable:
    """decorator để ghi lại các số liệu của một batch xử lý

    Args:
        func (Callable): _description_

    Returns:
        Callable: _description_
    """
    @wraps(func)
    def wrapper(self , batch_df , batch_id):
        start_time = time.time()
        metrics = {
            'processed': 0,
            'inserted': 0,
            'updated': 0,
            'deleted': 0,
            'errors': 0,
        }
        try:
            result =  func(self , batch_df , batch_id , metrics)
            metrics['duration_ms'] = int((time.time() - start_time) * 1000)
            #  cập nhât metrics cho pipeline
            if hasattr(self , 'pipeline_metrics'):
                self.pipeline_metrics.update_metrics(metrics)


            log_batch_metrics(batch_id , metrics)
            return result
        except Exception as e:
            metrics['errors'] += 1
            metrics['duration_ms'] = int((time.time() - start_time) * 1000)
            logger.error(f'Batch {batch_id} failed after {metrics["duration_ms"]:.2f}ms with error: {str(e)}', exc_info=True)
            raise e
    return wrapper

            
