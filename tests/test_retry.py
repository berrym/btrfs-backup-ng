"""Tests for the retry framework module."""

from unittest.mock import patch

import pytest

from btrfs_backup_ng.core.errors import (
    PermanentPermissionError,
    TransientNetworkError,
)
from btrfs_backup_ng.core.retry import (
    DEFAULT_NETWORK_POLICY,
    DEFAULT_QUICK_POLICY,
    DEFAULT_TRANSFER_POLICY,
    RetryAttempt,
    RetryContext,
    RetryPolicy,
    retry_call,
    with_retry,
)


class TestRetryPolicy:
    """Tests for RetryPolicy configuration."""

    def test_default_values(self):
        """Test default policy values."""
        policy = RetryPolicy()
        assert policy.max_attempts == 3
        assert policy.initial_delay == 1.0
        assert policy.max_delay == 300.0
        assert policy.exponential_base == 2.0
        assert policy.jitter == 0.1

    def test_custom_values(self):
        """Test custom policy values."""
        policy = RetryPolicy(
            max_attempts=5,
            initial_delay=2.0,
            max_delay=60.0,
            exponential_base=3.0,
            jitter=0.2,
        )
        assert policy.max_attempts == 5
        assert policy.initial_delay == 2.0
        assert policy.max_delay == 60.0
        assert policy.exponential_base == 3.0
        assert policy.jitter == 0.2

    def test_calculate_delay_exponential(self):
        """Test exponential backoff calculation."""
        policy = RetryPolicy(
            initial_delay=1.0,
            exponential_base=2.0,
            max_delay=100.0,
            jitter=0.0,  # Disable jitter for predictable testing
        )
        assert policy.calculate_delay(0) == 1.0
        assert policy.calculate_delay(1) == 2.0
        assert policy.calculate_delay(2) == 4.0
        assert policy.calculate_delay(3) == 8.0

    def test_calculate_delay_capped(self):
        """Test that delay is capped at max_delay."""
        policy = RetryPolicy(
            initial_delay=10.0,
            exponential_base=2.0,
            max_delay=50.0,
            jitter=0.0,
        )
        # 10 * 2^3 = 80, but should be capped at 50
        assert policy.calculate_delay(3) == 50.0

    def test_calculate_delay_with_jitter(self):
        """Test that jitter adds randomness."""
        policy = RetryPolicy(
            initial_delay=10.0,
            exponential_base=2.0,
            max_delay=100.0,
            jitter=0.2,  # 20% jitter
        )
        delays = [policy.calculate_delay(1) for _ in range(100)]
        # Base delay is 20, with 20% jitter = 16 to 24
        assert all(16 <= d <= 24 for d in delays)
        # Should have some variance
        assert len(set(delays)) > 1

    def test_is_retryable_backup_error(self):
        """Test retryability check for BackupError subclasses."""
        policy = RetryPolicy()

        transient = TransientNetworkError("Network error")
        assert policy.is_retryable(transient)

        permanent = PermanentPermissionError("Permission denied")
        assert not policy.is_retryable(permanent)

    def test_is_retryable_builtin_exceptions(self):
        """Test retryability check for built-in exceptions."""
        policy = RetryPolicy()

        assert policy.is_retryable(ConnectionError("Connection failed"))
        assert policy.is_retryable(TimeoutError("Timed out"))
        assert not policy.is_retryable(ValueError("Invalid value"))

    def test_attempts_generator(self):
        """Test the attempts generator."""
        policy = RetryPolicy(max_attempts=3)
        attempts = list(policy.attempts())

        assert len(attempts) == 3
        assert attempts[0].attempt_number == 0
        assert attempts[0].is_first
        assert not attempts[0].is_last
        assert attempts[2].attempt_number == 2
        assert attempts[2].is_last


class TestRetryAttempt:
    """Tests for RetryAttempt class."""

    def test_is_first_and_last(self):
        """Test first and last attempt detection."""
        policy = RetryPolicy(max_attempts=3)

        attempt0 = RetryAttempt(attempt_number=0, max_attempts=3, policy=policy)
        assert attempt0.is_first
        assert not attempt0.is_last
        assert attempt0.remaining_attempts == 2

        attempt2 = RetryAttempt(attempt_number=2, max_attempts=3, policy=policy)
        assert not attempt2.is_first
        assert attempt2.is_last
        assert attempt2.remaining_attempts == 0

    def test_should_retry_transient_error(self):
        """Test should_retry with transient error."""
        policy = RetryPolicy(max_attempts=3)
        attempt = RetryAttempt(attempt_number=0, max_attempts=3, policy=policy)

        error = TransientNetworkError("Network error")
        assert attempt.should_retry(error)
        assert attempt.last_error is error

    def test_should_retry_permanent_error(self):
        """Test should_retry with permanent error."""
        policy = RetryPolicy(max_attempts=3)
        attempt = RetryAttempt(attempt_number=0, max_attempts=3, policy=policy)

        error = PermanentPermissionError("Permission denied")
        assert not attempt.should_retry(error)

    def test_should_retry_last_attempt(self):
        """Test should_retry on last attempt."""
        policy = RetryPolicy(max_attempts=3)
        attempt = RetryAttempt(attempt_number=2, max_attempts=3, policy=policy)

        error = TransientNetworkError("Network error")
        assert not attempt.should_retry(error)  # Last attempt, can't retry

    @patch("time.sleep")
    def test_wait(self, mock_sleep):
        """Test wait with mocked sleep."""
        policy = RetryPolicy(initial_delay=1.0, jitter=0.0)
        attempt = RetryAttempt(attempt_number=0, max_attempts=3, policy=policy)
        attempt.last_error = TransientNetworkError("Error")

        delay = attempt.wait()
        assert delay == 1.0
        mock_sleep.assert_called_once_with(1.0)

    def test_wait_on_last_attempt(self):
        """Test that wait returns 0 on last attempt."""
        policy = RetryPolicy(max_attempts=3)
        attempt = RetryAttempt(attempt_number=2, max_attempts=3, policy=policy)

        delay = attempt.wait()
        assert delay == 0


class TestWithRetryDecorator:
    """Tests for the with_retry decorator."""

    def test_success_on_first_attempt(self):
        """Test function that succeeds immediately."""
        call_count = 0

        @with_retry(max_attempts=3)
        def successful_function():
            nonlocal call_count
            call_count += 1
            return "success"

        result = successful_function()
        assert result == "success"
        assert call_count == 1

    @patch("time.sleep")
    def test_success_after_retries(self, mock_sleep):
        """Test function that succeeds after retries."""
        call_count = 0

        @with_retry(max_attempts=3, initial_delay=1.0, jitter=0.0)
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TransientNetworkError("Network error")
            return "success"

        result = flaky_function()
        assert result == "success"
        assert call_count == 3
        assert mock_sleep.call_count == 2

    @patch("time.sleep")
    def test_failure_after_max_attempts(self, mock_sleep):
        """Test function that fails all attempts."""
        call_count = 0

        @with_retry(max_attempts=3)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise TransientNetworkError("Network error")

        with pytest.raises(TransientNetworkError):
            always_fails()

        assert call_count == 3

    def test_permanent_error_no_retry(self):
        """Test that permanent errors are not retried."""
        call_count = 0

        @with_retry(max_attempts=3)
        def permanent_failure():
            nonlocal call_count
            call_count += 1
            raise PermanentPermissionError("Permission denied")

        with pytest.raises(PermanentPermissionError):
            permanent_failure()

        assert call_count == 1  # No retries

    def test_with_policy_object(self):
        """Test decorator with RetryPolicy object."""
        policy = RetryPolicy(max_attempts=5)
        call_count = 0

        @with_retry(policy)
        def function():
            nonlocal call_count
            call_count += 1
            if call_count < 5:
                raise TransientNetworkError("Error")
            return "success"

        with patch("time.sleep"):
            result = function()

        assert result == "success"
        assert call_count == 5


class TestRetryCall:
    """Tests for retry_call function."""

    def test_success_returns_result(self):
        """Test successful call returns RetryResult."""

        def successful():
            return 42

        result = retry_call(successful)
        assert result.success
        assert result.result == 42
        assert result.attempts == 1
        assert result.error is None
        assert len(result.errors) == 0

    @patch("time.sleep")
    def test_failure_returns_result(self, mock_sleep):
        """Test failed call returns RetryResult with errors."""

        def always_fails():
            raise TransientNetworkError("Error")

        result = retry_call(always_fails, max_attempts=3)
        assert not result.success
        assert result.result is None
        assert result.attempts == 3
        assert isinstance(result.error, TransientNetworkError)
        assert len(result.errors) == 3

    def test_unwrap_success(self):
        """Test unwrap on successful result."""

        def successful():
            return "value"

        result = retry_call(successful)
        assert result.unwrap() == "value"

    def test_unwrap_failure(self):
        """Test unwrap on failed result raises error."""

        def fails():
            raise TransientNetworkError("Error")

        with patch("time.sleep"):
            result = retry_call(fails, max_attempts=1)

        with pytest.raises(TransientNetworkError):
            result.unwrap()

    def test_with_args_and_kwargs(self):
        """Test retry_call with function arguments."""

        def add(a, b, c=0):
            return a + b + c

        result = retry_call(add, args=(1, 2), kwargs={"c": 3})
        assert result.success
        assert result.result == 6


class TestRetryContext:
    """Tests for RetryContext context manager."""

    def test_success_on_first_attempt(self):
        """Test successful operation on first attempt."""
        with RetryContext(RetryPolicy(max_attempts=3)) as ctx:
            ctx.succeed("result")

        assert ctx.result.success
        assert ctx.result.result == "result"
        assert ctx.attempt_number == 0

    @patch("time.sleep")
    def test_success_after_retries(self, mock_sleep):
        """Test success after retries."""
        attempts = 0

        with RetryContext(RetryPolicy(max_attempts=3)) as ctx:
            while not ctx.exhausted:
                attempts += 1
                if attempts < 3:
                    if not ctx.record_failure(TransientNetworkError("Error")):
                        break
                    ctx.wait()
                else:
                    ctx.succeed("result")
                    break

        assert ctx.result.success
        assert ctx.result.result == "result"
        assert ctx.attempt_number == 2

    @patch("time.sleep")
    def test_exhausted_after_max_attempts(self, mock_sleep):
        """Test exhaustion after max attempts."""
        with RetryContext(RetryPolicy(max_attempts=2)) as ctx:
            while not ctx.exhausted:
                if not ctx.record_failure(TransientNetworkError("Error")):
                    break
                ctx.wait()

        assert ctx.exhausted
        assert not ctx.result.success
        assert ctx.result.attempts == 2

    def test_permanent_error_stops_retry(self):
        """Test that permanent error stops retries."""
        with RetryContext(RetryPolicy(max_attempts=3)) as ctx:
            should_retry = ctx.record_failure(
                PermanentPermissionError("Permission denied")
            )
            assert not should_retry

        assert not ctx.result.success
        assert ctx.result.attempts == 1

    def test_with_dict_config(self):
        """Test context manager with dict configuration."""
        with RetryContext({"max_attempts": 5, "initial_delay": 2.0}) as ctx:
            assert ctx.policy.max_attempts == 5
            assert ctx.policy.initial_delay == 2.0


class TestDefaultPolicies:
    """Tests for pre-defined policies."""

    def test_transfer_policy(self):
        """Test DEFAULT_TRANSFER_POLICY values."""
        assert DEFAULT_TRANSFER_POLICY.max_attempts == 3
        assert DEFAULT_TRANSFER_POLICY.initial_delay == 5.0
        assert DEFAULT_TRANSFER_POLICY.max_delay == 300.0

    def test_network_policy(self):
        """Test DEFAULT_NETWORK_POLICY values."""
        assert DEFAULT_NETWORK_POLICY.max_attempts == 5
        assert DEFAULT_NETWORK_POLICY.initial_delay == 1.0
        assert DEFAULT_NETWORK_POLICY.max_delay == 60.0

    def test_quick_policy(self):
        """Test DEFAULT_QUICK_POLICY values."""
        assert DEFAULT_QUICK_POLICY.max_attempts == 3
        assert DEFAULT_QUICK_POLICY.initial_delay == 0.5
        assert DEFAULT_QUICK_POLICY.max_delay == 5.0
