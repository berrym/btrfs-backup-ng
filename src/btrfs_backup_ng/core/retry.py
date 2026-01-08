"""Retry framework with exponential backoff for btrfs-backup-ng.

This module provides a robust retry mechanism with exponential backoff,
jitter, and intelligent error classification to determine retryability.

Usage:
    from btrfs_backup_ng.core.retry import RetryPolicy, with_retry

    # Using decorator
    @with_retry(RetryPolicy(max_attempts=3))
    def transfer_snapshot():
        ...

    # Using context manager
    policy = RetryPolicy(max_attempts=5, initial_delay=2.0)
    async with RetryContext(policy) as ctx:
        while ctx.should_retry():
            try:
                result = do_operation()
                break
            except Exception as e:
                ctx.record_failure(e)

    # Manual retry loop
    for attempt in policy.attempts():
        try:
            result = do_operation()
            break
        except TransientError:
            attempt.wait()
"""

import asyncio
import functools
import logging
import random
import time
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Generator,
    Optional,
    TypeVar,
    Union,
)

from .errors import BackupError, TransientError, classify_error

logger = logging.getLogger(__name__)

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


@dataclass
class RetryPolicy:
    """Configuration for retry behavior.

    Attributes:
        max_attempts: Maximum number of attempts (including initial)
        initial_delay: Starting delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff calculation
        jitter: Random jitter factor (0.0 to 1.0) to prevent thundering herd
        retryable_exceptions: Tuple of exception types to retry on
        on_retry: Optional callback called before each retry
    """

    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 300.0
    exponential_base: float = 2.0
    jitter: float = 0.1
    retryable_exceptions: tuple[type[Exception], ...] = field(
        default_factory=lambda: (TransientError, ConnectionError, TimeoutError)
    )
    on_retry: Optional[Callable[[int, Exception, float], None]] = None

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for a given attempt number (0-indexed).

        Uses exponential backoff with optional jitter:
            delay = min(initial_delay * (base ^ attempt), max_delay)
            delay = delay * (1 + random(-jitter, +jitter))

        Args:
            attempt: The attempt number (0 for first retry)

        Returns:
            Delay in seconds
        """
        # Exponential backoff
        delay = self.initial_delay * (self.exponential_base**attempt)

        # Cap at max_delay
        delay = min(delay, self.max_delay)

        # Add jitter
        if self.jitter > 0:
            jitter_range = delay * self.jitter
            delay = delay + random.uniform(-jitter_range, jitter_range)

        return max(0, delay)

    def is_retryable(self, error: Exception) -> bool:
        """Check if an exception should trigger a retry.

        Args:
            error: The exception to check

        Returns:
            True if the error is retryable
        """
        # Check if it's a BackupError with retryable flag
        if isinstance(error, BackupError):
            return error.is_retryable

        # Check against configured exception types
        return isinstance(error, self.retryable_exceptions)

    def attempts(self) -> Generator["RetryAttempt", None, None]:
        """Generator yielding RetryAttempt objects for each attempt.

        Usage:
            for attempt in policy.attempts():
                try:
                    result = do_something()
                    break
                except TransientError as e:
                    if not attempt.should_retry(e):
                        raise
                    attempt.wait()
        """
        for attempt_num in range(self.max_attempts):
            yield RetryAttempt(
                attempt_number=attempt_num,
                max_attempts=self.max_attempts,
                policy=self,
            )


@dataclass
class RetryAttempt:
    """Represents a single retry attempt."""

    attempt_number: int
    max_attempts: int
    policy: RetryPolicy
    last_error: Optional[Exception] = None

    @property
    def is_first(self) -> bool:
        """Return True if this is the first attempt."""
        return self.attempt_number == 0

    @property
    def is_last(self) -> bool:
        """Return True if this is the last allowed attempt."""
        return self.attempt_number >= self.max_attempts - 1

    @property
    def remaining_attempts(self) -> int:
        """Return the number of remaining attempts after this one."""
        return max(0, self.max_attempts - self.attempt_number - 1)

    def should_retry(self, error: Exception) -> bool:
        """Check if we should retry after this error.

        Args:
            error: The exception that occurred

        Returns:
            True if we should retry
        """
        self.last_error = error
        if self.is_last:
            return False
        return self.policy.is_retryable(error)

    def wait(self) -> float:
        """Wait for the calculated backoff delay.

        Returns:
            The delay that was waited
        """
        if self.is_last:
            return 0

        delay = self.policy.calculate_delay(self.attempt_number)

        if self.policy.on_retry and self.last_error:
            self.policy.on_retry(
                self.attempt_number + 1,
                self.last_error,
                delay,
            )

        logger.debug(
            "Retry attempt %d/%d after %.2fs delay",
            self.attempt_number + 2,
            self.max_attempts,
            delay,
        )
        time.sleep(delay)
        return delay

    async def wait_async(self) -> float:
        """Async version of wait().

        Returns:
            The delay that was waited
        """
        if self.is_last:
            return 0

        delay = self.policy.calculate_delay(self.attempt_number)

        if self.policy.on_retry and self.last_error:
            self.policy.on_retry(
                self.attempt_number + 1,
                self.last_error,
                delay,
            )

        logger.debug(
            "Retry attempt %d/%d after %.2fs delay (async)",
            self.attempt_number + 2,
            self.max_attempts,
            delay,
        )
        await asyncio.sleep(delay)
        return delay


@dataclass
class RetryResult:
    """Result of a retry operation."""

    success: bool
    result: Any = None
    error: Optional[Exception] = None
    attempts: int = 0
    total_delay: float = 0.0
    errors: list[Exception] = field(default_factory=list)

    def unwrap(self) -> Any:
        """Return the result or raise the last error."""
        if self.success:
            return self.result
        if self.error:
            raise self.error
        raise RuntimeError("Retry failed with no error recorded")


def with_retry(
    policy: Optional[RetryPolicy] = None,
    *,
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 300.0,
    exponential_base: float = 2.0,
    jitter: float = 0.1,
    classify_errors: bool = True,
) -> Callable[[F], F]:
    """Decorator to add retry logic to a function.

    Can be used with a RetryPolicy or with keyword arguments:

        @with_retry(RetryPolicy(max_attempts=5))
        def my_function():
            ...

        @with_retry(max_attempts=3, initial_delay=2.0)
        def my_other_function():
            ...

    Args:
        policy: Optional RetryPolicy to use
        max_attempts: Maximum attempts if no policy provided
        initial_delay: Initial delay if no policy provided
        max_delay: Maximum delay if no policy provided
        exponential_base: Backoff base if no policy provided
        jitter: Jitter factor if no policy provided
        classify_errors: Whether to classify unknown errors

    Returns:
        Decorated function with retry logic
    """
    if policy is None:
        policy = RetryPolicy(
            max_attempts=max_attempts,
            initial_delay=initial_delay,
            max_delay=max_delay,
            exponential_base=exponential_base,
            jitter=jitter,
        )

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_error: Optional[Exception] = None
            total_delay = 0.0
            errors: list[Exception] = []

            for attempt in policy.attempts():
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # Classify error if requested
                    if classify_errors and not isinstance(e, BackupError):
                        e = classify_error(e)

                    errors.append(e)
                    last_error = e

                    if not attempt.should_retry(e):
                        logger.warning(
                            "Non-retryable error on attempt %d/%d: %s",
                            attempt.attempt_number + 1,
                            attempt.max_attempts,
                            e,
                        )
                        raise

                    logger.info(
                        "Retryable error on attempt %d/%d: %s",
                        attempt.attempt_number + 1,
                        attempt.max_attempts,
                        e,
                    )
                    total_delay += attempt.wait()

            # All attempts exhausted
            if last_error:
                logger.error(
                    "All %d attempts failed. Total delay: %.2fs. Last error: %s",
                    policy.max_attempts,
                    total_delay,
                    last_error,
                )
                raise last_error

            # Should never reach here
            raise RuntimeError("Retry loop exited without result or error")

        return wrapper  # type: ignore

    return decorator


def with_retry_async(
    policy: Optional[RetryPolicy] = None,
    **kwargs: Any,
) -> Callable[[F], F]:
    """Async version of with_retry decorator.

    Usage:
        @with_retry_async(max_attempts=3)
        async def my_async_function():
            ...
    """
    if policy is None:
        policy = RetryPolicy(**kwargs) if kwargs else RetryPolicy()

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kw: Any) -> Any:
            last_error: Optional[Exception] = None

            for attempt in policy.attempts():
                try:
                    return await func(*args, **kw)
                except Exception as e:
                    if not isinstance(e, BackupError):
                        e = classify_error(e)

                    last_error = e

                    if not attempt.should_retry(e):
                        raise

                    await attempt.wait_async()

            if last_error:
                raise last_error
            raise RuntimeError("Retry loop exited without result or error")

        return wrapper  # type: ignore

    return decorator


def retry_call(
    func: Callable[..., T],
    args: tuple = (),
    kwargs: Optional[dict] = None,
    policy: Optional[RetryPolicy] = None,
    **policy_kwargs: Any,
) -> RetryResult:
    """Execute a function with retry logic, returning detailed results.

    Unlike with_retry decorator, this function returns a RetryResult
    with details about all attempts, rather than raising on failure.

    Args:
        func: Function to call
        args: Positional arguments for func
        kwargs: Keyword arguments for func
        policy: Optional RetryPolicy
        **policy_kwargs: Arguments to create RetryPolicy if not provided

    Returns:
        RetryResult with success status and details
    """
    if kwargs is None:
        kwargs = {}
    if policy is None:
        policy = RetryPolicy(**policy_kwargs) if policy_kwargs else RetryPolicy()

    errors: list[Exception] = []
    total_delay = 0.0

    for attempt in policy.attempts():
        try:
            result = func(*args, **kwargs)
            return RetryResult(
                success=True,
                result=result,
                attempts=attempt.attempt_number + 1,
                total_delay=total_delay,
                errors=errors,
            )
        except Exception as e:
            if not isinstance(e, BackupError):
                e = classify_error(e)

            errors.append(e)

            if not attempt.should_retry(e):
                return RetryResult(
                    success=False,
                    error=e,
                    attempts=attempt.attempt_number + 1,
                    total_delay=total_delay,
                    errors=errors,
                )

            total_delay += attempt.wait()

    # All attempts exhausted
    return RetryResult(
        success=False,
        error=errors[-1] if errors else None,
        attempts=policy.max_attempts,
        total_delay=total_delay,
        errors=errors,
    )


class RetryContext:
    """Context manager for manual retry control.

    Usage:
        with RetryContext(policy) as ctx:
            while not ctx.exhausted:
                try:
                    result = do_something()
                    ctx.succeed(result)
                    break
                except Exception as e:
                    if not ctx.record_failure(e):
                        raise
                    ctx.wait()

        if ctx.result.success:
            print(ctx.result.result)
    """

    def __init__(self, policy: Optional[Union[RetryPolicy, dict]] = None):
        if policy is None:
            policy = RetryPolicy()
        elif isinstance(policy, dict):
            policy = RetryPolicy(**policy)
        self.policy = policy
        self._attempt = 0
        self._errors: list[Exception] = []
        self._total_delay = 0.0
        self._result: Optional[Any] = None
        self._success = False

    def __enter__(self) -> "RetryContext":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        return False

    @property
    def exhausted(self) -> bool:
        """Return True if all attempts have been used."""
        return self._attempt >= self.policy.max_attempts

    @property
    def attempt_number(self) -> int:
        """Current attempt number (0-indexed)."""
        return self._attempt

    @property
    def result(self) -> RetryResult:
        """Get the current result state."""
        return RetryResult(
            success=self._success,
            result=self._result,
            error=self._errors[-1] if self._errors else None,
            attempts=self._attempt,
            total_delay=self._total_delay,
            errors=self._errors,
        )

    def succeed(self, result: Any = None) -> None:
        """Mark the operation as successful."""
        self._success = True
        self._result = result

    def record_failure(self, error: Exception) -> bool:
        """Record a failure and check if we should retry.

        Args:
            error: The exception that occurred

        Returns:
            True if we should retry, False if we should give up
        """
        if not isinstance(error, BackupError):
            error = classify_error(error)

        self._errors.append(error)
        self._attempt += 1

        if self.exhausted:
            return False

        return self.policy.is_retryable(error)

    def wait(self) -> float:
        """Wait for the backoff delay.

        Returns:
            The delay in seconds
        """
        if self.exhausted:
            return 0

        delay = self.policy.calculate_delay(self._attempt - 1)
        self._total_delay += delay

        logger.debug(
            "RetryContext: waiting %.2fs before attempt %d/%d",
            delay,
            self._attempt + 1,
            self.policy.max_attempts,
        )
        time.sleep(delay)
        return delay


# Default policies for common use cases
DEFAULT_TRANSFER_POLICY = RetryPolicy(
    max_attempts=3,
    initial_delay=5.0,
    max_delay=300.0,
    exponential_base=2.0,
    jitter=0.1,
)

DEFAULT_NETWORK_POLICY = RetryPolicy(
    max_attempts=5,
    initial_delay=1.0,
    max_delay=60.0,
    exponential_base=2.0,
    jitter=0.2,
)

DEFAULT_QUICK_POLICY = RetryPolicy(
    max_attempts=3,
    initial_delay=0.5,
    max_delay=5.0,
    exponential_base=2.0,
    jitter=0.1,
)
