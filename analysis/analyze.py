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
        lat = np.concatenate([pd.read_csv(s.parent / "latency_samples.csv")["total_ms"].to_numpy() for s in shards])
        rm = pd.concat([pd.read_csv(s.parent / "rooms.csv") for s in shards])
        samples[row.run_id] = lat
        rooms.append(rm.assign(run_id=row.run_id))
        counters = [json.loads(s.read_text())["counters"] for s in shards]
        runs.append(dict({k: v for k, v in row._asdict().items() if k != "Index"}, n_samples=len(lat), p50=np.percentile(lat, 50), p95=np.percentile(lat, 95),
                         p99=np.percentile(lat, 99), mean=lat.mean(), rooms_total=len(rm),
                         rooms_diverged=int(rm["diverged"].sum()), sent=sum(c["sent"] for c in counters)))
    return pd.DataFrame(runs), samples, pd.concat(rooms) if rooms else pd.DataFrame()


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


def analyse(results: Path, matrix: Path) -> None:
    out = results / "_analysis"
    out.mkdir(exist_ok=True)
    runs, samples, rooms = load_runs(results, matrix)
    if runs.empty:
        sys.exit("no finished runs found")
    runs.to_csv(out / "runs.csv", index=False)

    # H1: p99 latency per broker x load with cluster-bootstrap CI; H1 holds if upper CI < 50 ms.
    b = runs[runs.stage == "B"]
    h1 = []
    for (broker, load), g in b.groupby(["broker", "load"]):
        point, lo, hi = cluster_bootstrap_quantile(list(g.run_id), samples, 99)
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

    # H4: fit E[T](L) = T0 + 1000/(mu - L) on the two lower loads, predict the highest load (out of sample).
    h4 = []
    for broker, g in b.groupby("broker"):
        means = g.groupby("load")["mean"].mean().sort_index()
        if len(means) < 3:
            continue
        train, test = means.iloc[:-1], means.iloc[-1:]
        try:
            (t0, mu), _ = optimize.curve_fit(lambda L, t0, mu: [model.latency_curve(x, t0, mu) for x in L],
                                             train.index.to_numpy(float), train.to_numpy(),
                                             p0=[train.iloc[0], test.index[0] * 2], bounds=([0, test.index[0] * 1.01], [1e4, 1e7]))
            pred = model.latency_curve(float(test.index[0]), t0, mu)
            h4.append(dict(broker=broker, t0_ms=t0, mu=mu, load=test.index[0], predicted=pred,
                           measured=test.iloc[0], ape=abs(pred - test.iloc[0]) / test.iloc[0]))
        except (RuntimeError, ValueError) as error:
            h4.append(dict(broker=broker, error=str(error)))
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
