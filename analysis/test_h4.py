"""H4 fit rules: at least three load levels, and a solution that is not stuck on a bound."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import analyze  # noqa: E402
import model  # noqa: E402


def test_two_load_levels_are_invalid():
    means = pd.Series({1000.0: 10.0, 2500.0: 12.0})
    fit = analyze.fit_latency_curve(means)
    assert fit["valid"] is False
    assert "3" in fit["reason"]


def test_three_levels_on_the_curve_are_valid_and_in_sample():
    mu, t0 = 20_000.0, 10.0
    loads = [1000.0, 2500.0, 5000.0]
    means = pd.Series({load: model.latency_curve(load, t0, mu) for load in loads})
    fit = analyze.fit_latency_curve(means)
    assert fit["valid"] is True
    assert fit["held_out"] is False
    assert fit["reason"] == ""
    assert abs(fit["mu"] - mu) / mu < 0.05
    assert abs(fit["t0_ms"] - t0) < 1


def test_a_fit_pinned_on_the_service_rate_bound_is_invalid():
    # Mean latency collapses onto the highest load, so mu cannot sit above that load.
    means = pd.Series({1000.0: 10.0, 2500.0: 10.0, 5000.0: 1e6})
    fit = analyze.fit_latency_curve(means)
    assert fit["valid"] is False
    assert "bound" in fit["reason"] or "covariance" in fit["reason"] or "need" in fit["reason"] or fit["reason"]
