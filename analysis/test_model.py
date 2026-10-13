import math
import model


def test_pk_reduces_to_mm1_waiting_time():
    lam, mu = 50.0, 100.0  # exponential service: E[S^2] = 2/mu^2
    assert math.isclose(model.mg1_waiting_time(lam, 1 / mu, 2 / mu**2), lam / (mu * (mu - lam)))


def test_mm1_p99():
    assert math.isclose(model.mm1_sojourn_quantile(0.99, 50, 100), math.log(100) / 50)


def test_streams_more_reliable_than_pubsub():
    args = dict(n=20, room_rate=100, nu=1 / 3600, mean_offline=2, session_s=270)
    assert model.reliability_streams(window=10_000, **args) > model.reliability_pubsub(**args)


def test_minimal_window_reaches_target():
    args = dict(n=20, room_rate=100, nu=1 / 60, mean_offline=2, session_s=270)
    L = model.minimal_window(0.99, **args)
    assert model.reliability_streams(window=L, **args) >= 0.99
    assert model.reliability_streams(window=max(L - 1, 0), **args) < 0.99
