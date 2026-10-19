#!/usr/bin/env python3
"""Statistical analysis of the run matrix. Usage: python analysis/analyze.py results experiments/configs/matrix.csv

Unit of observation is the RUN (events within a run are autocorrelated). Shards of a run are merged.
Outputs CSV tables into results/_analysis/.
"""
from __future__ import annotations
import json, sys
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import optimize, stats

import model

RNG = np.random.default_rng(20261101)
B = 10_000


def load_runs(results: Path, matrix: Path) -> tuple[pd.DataFrame, dict[str, np.ndarray], pd.DataFrame]:
    plan = pd.read_csv(matrix)
    runs, samples, rooms = [], {}, []
    for row in plan.itertuples():
        shards = sorted((results / row.run_id).glob("shard-*/summary.json"))
        if not shards:
            continue
        frames = [pd.read_csv(s.parent / "latency_samples.csv") for s in shards]
        # service_ms is send->receive. total_ms includes open-loop generator lag.
        lat = np.concatenate([(f["service_ms"] if "service_ms" in f.columns else f["total_ms"]).to_numpy() for f in frames])
        rm = pd.concat([pd.read_csv(s.parent / "rooms.csv") for s in shards])
        samples[row.run_id] = lat
        rooms.append(rm.assign(run_id=row.run_id))
        summaries = [json.loads(s.read_text()) for s in shards]
        counters = [s["counters"] for s in summaries]
        saturated = any(bool(s.get("load", {}).get("saturated")) for s in summaries)
        # HDR percentiles see every delivery. The reservoir is a sample and used to be
        # only the start of an L5000 window, so the run-level numbers follow the HDR.
        def hdr(key: str) -> float:
            return max(float((s.get("latency") or {}).get(key) or 0) for s in summaries)
        ratios = [float(s["delivery"]["ratio"]) for s in summaries if isinstance(s.get("delivery"), dict) and "ratio" in s["delivery"]]
        delivery_ratio = min(ratios) if ratios else np.nan
        censored = bool(ratios) and delivery_ratio < 0.99
        sent_n = skipped_n = attempted = offered = 0.0
        for summary, counter in zip(summaries, counters):
            sent_n += float(counter.get("sent", 0))
            skipped_n += float(counter.get("skippedWhileOffline", 0))
            cfg = summary.get("config") or {}
            window = max(0.001, float(cfg.get("WARMUP_S", 0)) + float(cfg.get("DURATION_S", 0)))
            attempted += (float(counter.get("sent", 0)) + float(counter.get("skippedWhileOffline", 0))) / window
            offered += float((summary.get("load") or {}).get("offeredPerSec") or 0)
        offline = skipped_n / (sent_n + skipped_n) if (sent_n + skipped_n) else 0.0
        attempted_ratio = attempted / offered if offered else 1.0
        # Generator kept up and the broker did not. Kept in saturation.csv, dropped from H1/H4.
        saturated_valid = bool(
            ratios and delivery_ratio < 0.99 and attempted_ratio >= 0.99 and offline <= 0.05 and row.broker != "pubsub"
        )
        runs.append(dict({k: v for k, v in row._asdict().items() if k != "Index"}, n_samples=len(lat),
                         p50=hdr("p50Ms"), p95=hdr("p95Ms"), p99=hdr("p99Ms"), mean=hdr("meanMs"),
                         rooms_total=len(rm),
                         rooms_diverged=int(rm["diverged"].sum()),
                         clients_lost=sum(int(s.get("clientsLost", 0)) for s in summaries),
                         clients_not_converged=sum(int(s.get("clientsNotConverged", 0)) for s in summaries),
                         sent=sum(c["sent"] for c in counters),
                         attempted_ratio=attempted_ratio, offline_fraction=offline,
                         saturated=saturated, delivery_ratio=delivery_ratio, censored=censored,
                         saturated_valid=saturated_valid))
    return pd.DataFrame(runs), samples, pd.concat(rooms) if rooms else pd.DataFrame()


def _bootstrap_median(values: np.ndarray) -> tuple[float, float, float]:
    """95% CI of the median, resampling runs. One run has no width."""
    point = float(np.median(values))
    if len(values) < 2:
        return point, point, point
    stats_ = []
    for _ in range(min(B, 2000)):
        pick = values[RNG.integers(0, len(values), len(values))]
        stats_.append(float(np.median(pick)))
    lo, hi = np.percentile(stats_, [2.5, 97.5])
    return point, float(lo), float(hi)


def cluster_bootstrap_quantile(run_ids: list[str], samples: dict[str, np.ndarray], q: float) -> tuple[float, float, float]:
    """Two-stage bootstrap: resample runs, then pool their samples; 95% percentile CI of the q-quantile."""
    arrays = [samples[r] for r in run_ids]
    point = np.percentile(np.concatenate(arrays), q)
    stats_ = []
    for _ in range(min(B, 2000)):
        pick = RNG.integers(0, len(arrays), len(arrays))
        pooled = np.concatenate([RNG.choice(arrays[i], size=len(arrays[i])) for i in pick])
        stats_.append(np.percentile(pooled, q))
    lo, hi = np.percentile(stats_, [2.5, 97.5])
    return point, lo, hi


def vargha_delaney_a12(x: np.ndarray, y: np.ndarray) -> float:
    """P(X > Y) + 0.5 P(X = Y)."""
    gt = sum((xi > y).sum() + 0.5 * (xi == y).sum() for xi in x)
    return gt / (len(x) * len(y))


def holm(pvalues: list[float]) -> list[float]:
    order = np.argsort(pvalues)
    adjusted, running = [0.0] * len(pvalues), 0.0
    for rank, idx in enumerate(order):
        running = max(running, (len(pvalues) - rank) * pvalues[idx])
        adjusted[idx] = min(1.0, running)
    return adjusted


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    p = k / n
    centre = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return centre - half, centre + half


def tost_two_proportions(k1: int, n1: int, k2: int, n2: int, margin: float) -> float:
    """Two one-sided z-tests for equivalence |p1 - p2| < margin; returns the larger p-value."""
    p1, p2 = k1 / n1, k2 / n2
    se = np.sqrt(max(p1 * (1 - p1) / n1 + p2 * (1 - p2) / n2, 1e-12))
    d = p1 - p2
    return max(1 - stats.norm.cdf((d + margin) / se), stats.norm.cdf((d - margin) / se))


def fit_latency_curve(means: pd.Series) -> dict:
    """H4: E[T](L) = T0 + 1000/(mu - L). Needs >= 3 load levels and an interior fit.

    With four or more levels the highest load is held out. With exactly three, the fit is
    in-sample: holding one out would leave two points, which is what pinned mu on the bound.
    """
    means = means.sort_index()
    if len(means) < 3:
        return {"valid": False, "reason": f"need >= 3 load levels, have {len(means)}", "levels": int(len(means))}
    held_out = len(means) >= 4
    train = means.iloc[:-1] if held_out else means
    test_load = float(means.index[-1])
    test_value = float(means.iloc[-1])
    lower_mu = float(train.index.max()) * 1.01
    upper_mu = 1e7
    upper_t0 = 1e4
    try:
        (t0, mu), pcov = optimize.curve_fit(
            lambda L, t0, mu: [model.latency_curve(x, t0, mu) for x in L],
            train.index.to_numpy(float), train.to_numpy(),
            p0=[float(train.iloc[0]), max(lower_mu * 2, float(train.index.max()) + 1000)],
            bounds=([0, lower_mu], [upper_t0, upper_mu]),
        )
    except (RuntimeError, ValueError) as error:
        return {"valid": False, "reason": str(error), "levels": int(len(means)), "held_out": held_out}
    t0, mu = float(t0), float(mu)
    if pcov is None or not np.all(np.isfinite(pcov)):
        return {"valid": False, "reason": "covariance is not finite", "t0_ms": t0, "mu": mu,
                "levels": int(len(means)), "held_out": held_out}
    on_bound = (
        mu <= lower_mu * (1 + 1e-6) or mu >= upper_mu * (1 - 1e-9)
        or t0 <= 1e-9 or t0 >= upper_t0 * (1 - 1e-9)
    )
    pred = float(model.latency_curve(test_load, t0, mu))
    result = {
        "valid": not on_bound,
        "reason": "fit hit a parameter bound" if on_bound else "",
        "t0_ms": t0, "mu": mu, "load": test_load, "predicted": pred, "measured": test_value,
        "ape": abs(pred - test_value) / test_value if test_value else np.nan,
        "levels": int(len(means)), "held_out": held_out,
    }
    return result


def _latency_runs(runs: pd.DataFrame) -> pd.DataFrame:
    """Drop censored and SATURATED_VALID cells. Those are not latency samples."""
    keep = runs
    if "censored" in keep.columns:
        keep = keep[keep["censored"] != True]  # noqa: E712 — pandas NA stays
    if "saturated_valid" in keep.columns:
        keep = keep[keep["saturated_valid"] != True]  # noqa: E712
    return keep


def analyse(results: Path, matrix: Path) -> None:
    out = results / "_analysis"
    out.mkdir(exist_ok=True)
    runs, samples, rooms = load_runs(results, matrix)
    if runs.empty:
        sys.exit("no finished runs found")
    runs.to_csv(out / "runs.csv", index=False)
    if "saturated_valid" in runs.columns:
        runs[runs["saturated_valid"] == True].to_csv(out / "saturation.csv", index=False)  # noqa: E712

    # H1: HDR p99 per broker x load. Censored and SATURATED_VALID cells are left out.
    # The CI resamples runs, which is the unit of observation. H1 holds if the upper CI < 50 ms.
    b = _latency_runs(runs)
    b = b[b.stage == "B"]
    h1 = []
    for (broker, load), g in b.groupby(["broker", "load"]):
        point, lo, hi = _bootstrap_median(g.p99.to_numpy(float))
        h1.append(dict(broker=broker, load=load, runs=len(g), p99=point, ci_low=lo, ci_high=hi, h1_holds=hi < 50))
    pd.DataFrame(h1).to_csv(out / "h1_p99.csv", index=False)

    # Broker comparison of run-level p99: Kruskal-Wallis per load, pairwise Mann-Whitney + Holm + A12.
    comparisons = []
    for load, g in b.groupby("load"):
        groups = {k: v.p99.to_numpy() for k, v in g.groupby("broker")}
        if len(groups) > 1:
            kw = stats.kruskal(*groups.values())
            pairs = list(combinations(groups, 2))
            ps = [stats.mannwhitneyu(groups[a], groups[c], alternative="two-sided").pvalue for a, c in pairs]
            for (a, c), p, ph in zip(pairs, ps, holm(ps)):
                comparisons.append(dict(load=load, kruskal_p=kw.pvalue, a=a, b=c, mw_p=p, mw_p_holm=ph,
                                        a12=vargha_delaney_a12(groups[a], groups[c])))
    pd.DataFrame(comparisons).to_csv(out / "broker_comparisons.csv", index=False)

    # H4: same censored-cell filter as H1. A two-point fit is reported invalid.
    h4 = []
    for broker, g in b.groupby("broker"):
        means = g.groupby("load")["mean"].mean().sort_index()
        h4.append(dict(broker=broker, **fit_latency_curve(means)))
    pd.DataFrame(h4).to_csv(out / "h4_model_fit.csv", index=False)

    # H2/H3/H5: reliability per broker x nu x kill.
    c = runs[runs.stage == "C"]
    rel = []
    for (nu, kill), g in c.groupby(["disconnects_per_min", "gateway_kill"]):
        cell = {k: (int(v.rooms_total.sum() - v.rooms_diverged.sum()), int(v.rooms_total.sum())) for k, v in g.groupby("broker")}
        for broker, (ok, n) in cell.items():
            lo, hi = wilson(ok, n)
            row = dict(nu_per_min=nu, gateway_kill=kill, broker=broker, rooms=n, R=ok / n, ci_low=lo, ci_high=hi)
            if broker == "pubsub" and kill == 0:
                session = float(g.iloc[0].get("duration_s", 270))
                row["R_model"] = model.reliability_pubsub(int(g.iloc[0].clients), g.iloc[0].clients * g.iloc[0].rate,
                                                          nu / 60, 2.0, session)
            rel.append(row)
        if "streams" in cell and "pubsub" in cell:
            (k1, n1), (k2, n2) = cell["streams"], cell["pubsub"]
            rel.append(dict(nu_per_min=nu, gateway_kill=kill, broker="H2 streams>pubsub (Fisher, one-sided)",
                            p=stats.fisher_exact([[k1, n1 - k1], [k2, n2 - k2]], alternative="greater")[1]))
        if "streams" in cell and "kafka-tuned" in cell:
            (k1, n1), (k2, n2) = cell["streams"], cell["kafka-tuned"]
            rel.append(dict(nu_per_min=nu, gateway_kill=kill, broker="H3 streams~kafka (TOST, margin 0.02)",
                            p=tost_two_proportions(k1, n1, k2, n2, 0.02)))
    pd.DataFrame(rel).to_csv(out / "reliability.csv", index=False)
    print(f"analysed {len(runs)} runs -> {out}")


if __name__ == "__main__":
    analyse(Path(sys.argv[1] if len(sys.argv) > 1 else "results"),
            Path(sys.argv[2] if len(sys.argv) > 2 else "experiments/configs/matrix.csv"))
