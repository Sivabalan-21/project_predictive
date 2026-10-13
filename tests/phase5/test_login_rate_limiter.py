from services.login_rate_limiter import LoginRateLimiter


def test_login_limit_is_bounded_per_client_and_recovers_after_window():
    now = [0.0]
    limiter = LoginRateLimiter(2, 10, clock=lambda: now[0])
    assert limiter.allow("127.0.0.1") and limiter.allow("127.0.0.1") and not limiter.allow("127.0.0.1")
    assert limiter.allow("127.0.0.2")
    now[0] = 10.1
    assert limiter.allow("127.0.0.1")
