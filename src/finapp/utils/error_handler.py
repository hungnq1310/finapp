"""
Error Handler Module for Financial News Analysis

This module provides comprehensive error handling functionality
following Python best practices with proper logging and graceful degradation.
"""

import logging
import traceback
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, Union
from datetime import datetime

# Type variables for decorator
F = TypeVar('F', bound=Callable[..., Any])

# Configure logger
logger = logging.getLogger(__name__)

class DataCollectionError(Exception):
    """Custom exception for data collection errors"""
    
    def __init__(self, message: str, source: Optional[str] = None, error_code: Optional[str] = None):
        self.message = message
        self.source = source
        self.error_code = error_code
        self.timestamp = datetime.utcnow()
        super().__init__(self.message)

class RSSParsingError(DataCollectionError):
    """Exception for RSS parsing specific errors"""
    
    def __init__(self, message: str, feed_url: Optional[str] = None):
        super().__init__(message, source=feed_url, error_code="RSS_PARSE_ERROR")
        self.feed_url = feed_url

class DatabaseError(DataCollectionError):
    """Exception for database operation errors"""
    
    def __init__(self, message: str, operation: Optional[str] = None):
        super().__init__(message, error_code="DB_ERROR")
        self.operation = operation

class SchedulingError(DataCollectionError):
    """Exception for scheduling errors"""
    
    def __init__(self, message: str, job_id: Optional[str] = None):
        super().__init__(message, error_code="SCHEDULE_ERROR")
        self.job_id = job_id

def error_handler(
    default_return: Any = None,
    log_errors: bool = True,
    raise_on_error: bool = False,
    error_message: Optional[str] = None
):
    """
    Decorator for consistent error handling across the application.
    
    Args:
        default_return: Value to return when an error occurs (default: None)
        log_errors: Whether to log errors (default: True)
        raise_on_error: Whether to raise exceptions instead of swallowing them (default: False)
        error_message: Custom error message for logging
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except DataCollectionError as e:
                if log_errors:
                    logger.error(f"DataCollectionError in {func.__name__}: {e.message}")
                    if e.source:
                        logger.error(f"Error source: {e.source}")
                    if e.error_code:
                        logger.error(f"Error code: {e.error_code}")
                    logger.debug(traceback.format_exc())
                
                if raise_on_error:
                    raise
                
                return default_return
                
            except Exception as e:
                if log_errors:
                    error_msg = error_message or f"Unexpected error in {func.__name__}: {e}"
                    logger.error(error_msg)
                    logger.error(traceback.format_exc())
                
                if raise_on_error:
                    raise DataCollectionError(
                        message=str(e),
                        error_code="UNEXPECTED_ERROR"
                    )
                
                return default_return
        
        return wrapper  # type: ignore
    return decorator

def safe_execute(
    func: Callable,
    *args,
    default_return: Any = None,
    log_errors: bool = True,
    **kwargs
) -> Any:
    """
    Safely execute a function with error handling.
    
    Args:
        func: Function to execute
        *args: Function arguments
        default_return: Value to return on error
        log_errors: Whether to log errors
        **kwargs: Function keyword arguments
    
    Returns:
        Function result or default_return on error
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if log_errors:
            logger.error(f"Error executing {func.__name__}: {e}")
            logger.debug(traceback.format_exc())
        return default_return

def log_and_reraise(error_message: str, error_class: type = DataCollectionError):
    """
    Log an error and re-raise it with additional context.
    
    Args:
        error_message: Message to log and include in exception
        error_class: Exception class to raise
    
    Raises:
        error_class: The specified exception with the error message
    """
    logger.error(error_message)
    logger.debug(traceback.format_exc())
    raise error_class(error_message)

# Context manager for database operations
class DatabaseOperation:
    """Context manager for safe database operations"""
    
    def __init__(self, operation_name: str, db_session=None):
        self.operation_name = operation_name
        self.db_session = db_session
        self.logger = logging.getLogger(__name__)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            if self.db_session:
                try:
                    self.db_session.rollback()
                except Exception:
                    self.logger.error("Failed to rollback database session")
            
            if issubclass(exc_type, DataCollectionError):
                # Re-raise our custom exceptions
                return False
            
            # Wrap other exceptions in DatabaseError
            raise DatabaseError(
                message=f"Database operation '{self.operation_name}' failed: {exc_val}",
                operation=self.operation_name
            ) from exc_val
        
        if self.db_session:
            try:
                self.db_session.commit()
            except Exception as e:
                self.logger.error(f"Failed to commit database session: {e}")
                raise DatabaseError(
                    message=f"Failed to commit '{self.operation_name}': {e}",
                    operation=self.operation_name
                )
        
        return True

# Validation decorators
def validate_input(validation_func: Callable[[Any], bool], error_message: str):
    """
    Decorator to validate function inputs.
    
    Args:
        validation_func: Function that returns True if input is valid
        error_message: Message to use if validation fails
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not validation_func(*args, **kwargs):
                raise DataCollectionError(
                    message=error_message,
                    error_code="VALIDATION_ERROR"
                )
            return func(*args, **kwargs)
        return wrapper  # type: ignore
    return decorator

# Retry decorator for transient failures
def retry_on_failure(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    Decorator to retry functions on failure with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff_factor: Multiplier for exponential backoff
        exceptions: Tuple of exception types to catch and retry on
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            import time
            
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries: {e}")
                        raise DataCollectionError(
                            message=f"Operation failed after {max_retries} retries: {e}",
                            error_code="RETRY_EXHAUSTED"
                        )
                    
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying in {current_delay}s...")
                    time.sleep(current_delay)
                    current_delay *= backoff_factor
            
            # This should never be reached
            raise DataCollectionError("Unexpected error in retry decorator")
        
        return wrapper  # type: ignore
    return decorator

__all__ = [
    "DataCollectionError",
    "RSSParsingError", 
    "DatabaseError",
    "SchedulingError",
    "error_handler",
    "safe_execute",
    "log_and_reraise",
    "DatabaseOperation",
    "validate_input",
    "retry_on_failure",
]