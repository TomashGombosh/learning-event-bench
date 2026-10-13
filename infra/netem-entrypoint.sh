#!/bin/sh
# Optional network impairment, e.g. NETEM="delay 20ms 5ms distribution normal loss 1%".
# Requires cap_add: NET_ADMIN. Applied to egress of this container only.
set -e
if [ -n "$NETEM" ]; then
  tc qdisc replace dev eth0 root netem $NETEM
  echo "netem applied: $NETEM"
fi
exec "$@"
