#!/bin/sh
# Optional network impairment, e.g. NETEM="delay 20ms 5ms distribution normal loss 1%".
# Requires cap_add: NET_ADMIN. Applied to egress of this container only.
set -e
if [ -n "$NETEM" ]; then
  # sch_netem must already be loaded on the host. A container cannot modprobe it,
  # and tc then exits with "Specified qdisc kind is unknown" before the workload starts.
  echo "applying netem on eth0: $NETEM"
  tc qdisc replace dev eth0 root netem $NETEM
  echo "netem applied: $NETEM"
fi
exec "$@"
