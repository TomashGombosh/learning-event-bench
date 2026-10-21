"""Provisional scenario profiles for the CSA scheme × transport matrix.

Edit this file to change a profile. Aggregate offered rate is
L = rooms * clients * rate. Whiteboard L is 2500. Quiz and class-chat rooms
are chosen so that product is an integer; those two loads are provisional.
"""
from __future__ import annotations

# Distinct from make_matrix.SEED so the two shuffles are not the same sequence.
CSA_SEED = 20261021

SCHEMES = ("yjs", "sharedb")
# The six alternatives are SCHEMES × BROKERS. Kafka-tuned matches the Yjs tuned producer.
BROKERS = ("pubsub", "streams", "kafka-tuned")

# name, clients per room, events/s/user, rooms, aggregate L
PROFILES = {
    "whiteboard": {"clients": 20, "rate": 5.0, "rooms": 25, "load": 2500},
    "quiz": {"clients": 30, "rate": 0.2, "rooms": 10, "load": 60},
    "class-chat": {"clients": 50, "rate": 0.5, "rooms": 10, "load": 250},
}

# nu = 0; and nu = 1/min with a gateway kill.
FAULTS = (
    {"disconnects_per_min": 0, "gateway_kill": 0},
    {"disconnects_per_min": 1, "gateway_kill": 1},
)

REPS = 5
SMOKE_PROFILE = "whiteboard"
SMOKE_REPS = 1
