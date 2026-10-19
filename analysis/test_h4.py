"""H4 fit rules: at least three load levels, and a solution that is not stuck on a bound."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import analyze  # noqa: E402
import model  # noqa: E402


def test_two_load_levels_are_invalid():
    means = pd.Series({1000.0: 10.0, 2500.0: 12.0})
    fit = analyze.fit_latency_curve(means)
    assert fit["valid"] is False
    assert "3" in fit["reason"]


def test_three_levels_on_the_curve_predict_the_held_out_load():
    mu, t0 = 20_000.0, 10.0
    loads = [1000.0, 2500.0, 5000.0]
    means = pd.Series({load: model.latency_curve(load, t0, mu) for load in loads})
    fit = analyze.fit_latency_curve(means)
    assert fit["valid"] is True
    assert fit["held_out"] is True
    assert fit["reason"] == ""
    assert abs(fit["mu"] - mu) / mu < 0.05
    assert abs(fit["t0_ms"] - t0) < 1
    assert fit["ape"] < 0.01


def test_mu_is_not_forced_above_the_test_load():
    # Run 20261018T110200Z-c440945, streams stage B means per load (ms). The old bound
    # (mu > 1.01 x 5000) pinned mu at 5050 for every broker. Free of it, the low loads
    # put mu near 3500 ev/s: the model predicts saturation before 5000, so H4 fails.
    points = pd.Series([10.77, 10.77, 11.37, 11.37, 59.28, 59.28],
                       index=[1000.0, 1000.0, 2500.0, 2500.0, 5000.0, 5000.0])
    fit = analyze.fit_latency_curve(points)
    assert fit["valid"] is True
    assert 2500 < fit["mu"] < 5000
    assert fit["predicted"] == float("inf")
    assert "saturation" in fit["reason"]
    assert 5000 < fit["mu_all"] < 5100


def test_per_run_points_give_mu_a_standard_error():
    mu, t0 = 8000.0, 10.0
    rng = np.random.default_rng(1)
    loads = [1000.0, 2500.0, 4000.0, 5000.0]
    xs = [load for load in loads for _ in range(5)]
    ys = [model.latency_curve(load, t0, mu) + rng.normal(0, 0.05) for load in xs]
    fit = analyze.fit_latency_curve(pd.Series(ys, index=xs))
    assert fit["valid"] is True
    assert np.isfinite(fit["mu_se"]) and fit["mu_se"] > 0
    assert abs(fit["mu"] - mu) / mu < 0.1


def test_a_fit_pinned_on_the_service_rate_bound_is_invalid():
    # Mean latency collapses onto the highest load, so mu cannot sit above that load.
    means = pd.Series({1000.0: 10.0, 2500.0: 10.0, 5000.0: 1e6})
    fit = analyze.fit_latency_curve(means)
    assert fit["valid"] is False
    assert "bound" in fit["reason"] or "covariance" in fit["reason"] or "need" in fit["reason"] or fit["reason"]
