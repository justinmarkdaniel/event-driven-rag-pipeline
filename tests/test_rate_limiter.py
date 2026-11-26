import pytest

from rag_ingest.domain import RateLimitedError
from rag_ingest.rate_limiter import TokenBucket


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def bucket(clock: FakeClock, rpm: float = 60, tpm: float = 600) -> TokenBucket:
    return TokenBucket(rpm, tpm, clock=clock, sleep=clock.sleep)


def test_acquires_immediately_while_budget_remains(clock: FakeClock) -> None:
    limiter = bucket(clock)
    for _ in range(5):
        limiter.acquire(100, max_wait_seconds=0)
    assert clock.slept == []


def test_sleeps_exactly_until_tokens_refill(clock: FakeClock) -> None:
    limiter = bucket(clock, tpm=600)  # refills 10 tokens/second
    limiter.acquire(600, max_wait_seconds=0)

    limiter.acquire(50, max_wait_seconds=10)

    assert clock.slept == [pytest.approx(5.0)]


def test_request_limit_applies_independently_of_tokens(clock: FakeClock) -> None:
    limiter = bucket(clock, rpm=2, tpm=10_000)  # one request per 30 s
    limiter.acquire(1, 0)
    limiter.acquire(1, 0)

    limiter.acquire(1, max_wait_seconds=60)

    assert clock.slept == [pytest.approx(30.0)]


def test_raises_instead_of_waiting_too_long(clock: FakeClock) -> None:
    limiter = bucket(clock, tpm=600)
    limiter.acquire(600, 0)
    with pytest.raises(RateLimitedError):
        limiter.acquire(600, max_wait_seconds=1)
    assert clock.slept == []


def test_refill_is_capped_at_bucket_size(clock: FakeClock) -> None:
    limiter = bucket(clock, tpm=600)
    clock.now = 3600
    limiter.acquire(600, 0)
    with pytest.raises(RateLimitedError):
        limiter.acquire(1, max_wait_seconds=0)


def test_request_larger_than_bucket_is_rejected(clock: FakeClock) -> None:
    with pytest.raises(ValueError):
        bucket(clock, tpm=100).acquire(101, max_wait_seconds=1000)
