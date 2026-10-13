"""Analytical model of the article (queueing latency + probabilistic reliability).

Notation follows research plan section 1: n clients per room, lambda_r = n * lambda_u room event rate,
nu disconnect rate per client (1/s), delta mean offline time (s), L replay window, T_s session length (s).
"""
from __future__ import annotations
import math


def mg1_waiting_time(arrival_rate: float, mean_service: float, second_moment_service: float) -> float:
    """Pollaczek-Khinchine mean waiting time W = lambda E[S^2] / (2 (1 - rho))."""
    rho = arrival_rate * mean_service
    if rho >= 1:
        return math.inf
    return arrival_rate * second_moment_service / (2 * (1 - rho))


def mm1_sojourn_mean(arrival_rate: float, service_rate: float) -> float:
    return math.inf if arrival_rate >= service_rate else 1.0 / (service_rate - arrival_rate)


def mm1_sojourn_quantile(q: float, arrival_rate: float, service_rate: float) -> float:
    """t_q = -ln(1-q) / (mu - lambda); sojourn time in M/M/1 is Exp(mu - lambda)."""
    return math.inf if arrival_rate >= service_rate else -math.log(1 - q) / (service_rate - arrival_rate)


def latency_curve(load: float, t0_ms: float, service_rate: float) -> float:
    """Single-bottleneck approximation used for H4: E[T](Lambda) = T0 + 1000 / (mu - Lambda) [ms]."""
    return t0_ms + 1000.0 * mm1_sojourn_mean(load, service_rate)


def p_event_during_outage(room_rate: float, mean_offline: float) -> float:
    """P(N_D >= 1) with N_D | D ~ Poisson(lambda_r D), D ~ Exp(mean delta): lambda_r delta / (1 + lambda_r delta)."""
    x = room_rate * mean_offline
    return x / (1 + x)


def reliability_pubsub(n: int, room_rate: float, nu: float, mean_offline: float, session_s: float,
                       p_loss: float = 0.0) -> float:
    """R_PS = exp(-n nu T_s q) * (1 - p)^(K (n - 1)), q = P(event during an outage), K = lambda_r T_s."""
    q = p_event_during_outage(room_rate, mean_offline)
    k = room_rate * session_s
    return math.exp(-n * nu * session_s * q) * (1 - p_loss) ** (k * (n - 1))


def reliability_streams(n: int, room_rate: float, nu: float, mean_offline: float, session_s: float,
                        window: int) -> float:
    """R_RS = exp(-n nu T_s q^(L+1)); divergence only if more than L events accrue during an outage."""
    q = p_event_during_outage(room_rate, mean_offline)
    return math.exp(-n * nu * session_s * q ** (window + 1))


def minimal_window(target_r: float, n: int, room_rate: float, nu: float, mean_offline: float,
                   session_s: float) -> int:
    """L* = ceil( ln(-ln R0 / (n nu T_s)) / ln q ) - 1."""
    q = p_event_during_outage(room_rate, mean_offline)
    m = n * nu * session_s
    if m == 0 or -math.log(target_r) >= m:
        return 0
    return max(0, math.ceil(math.log(-math.log(target_r) / m) / math.log(q)) - 1)
