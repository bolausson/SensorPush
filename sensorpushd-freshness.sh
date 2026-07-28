#!/bin/bash
# Alert when sensorpushd stops storing data.
#
# Process supervision alone cannot catch this: the daemon can be "active"
# and still be storing nothing (upstream API down, all backends refusing
# writes).  It can equally be dead.  Both show up here as a stale
# last-success marker, which sensorpushd rewrites only after data has
# actually reached a backend.
#
# Invoked by sensorpushd-freshness.timer.  Exits non-zero when stale so
# the unit itself goes to failed state and is visible in systemctl.

set -uo pipefail

STATE_FILE="${SENSORPUSHD_STATE:-/var/lib/sensorpushd/last-success}"
# Alert once data is older than this.  Well above the 300s poll interval
# so an ordinary slow cycle or a short retry storm does not page anyone.
MAX_AGE_MIN="${SENSORPUSHD_MAX_AGE_MIN:-30}"
RECIPIENT="${SENSORPUSHD_ALERT_TO:-root}"

notify() {
    local subject="$1" body="$2"
    # The journal entry is the reliable channel and is written first: mail
    # delivery depends on a relay that may itself be down, and an
    # unreachable relay makes sendmail block indefinitely - which would
    # wedge this timer instead of alerting anyone.  Hence the hard timeout.
    logger -t sensorpushd-freshness -p daemon.err -- "$subject: $body"
    if command -v sendmail >/dev/null 2>&1; then
        printf 'To: %s\nSubject: %s\n\n%s\n' \
            "$RECIPIENT" "$subject" "$body" \
            | timeout 15 sendmail -t 2>/dev/null \
            || logger -t sensorpushd-freshness -p daemon.warning -- \
                 "mail delivery to $RECIPIENT failed or timed out"
    fi
}

active=$(systemctl is-active sensorpushd.service 2>/dev/null || true)

if [[ ! -f "$STATE_FILE" ]]; then
    # Nothing written yet.  Only a problem if the daemon has been up long
    # enough to have completed a cycle.
    if [[ "$active" == "active" ]]; then
        started=$(systemctl show -p ActiveEnterTimestampMonotonic --value \
                  sensorpushd.service 2>/dev/null || echo 0)
        up_sec=$(( ( $(cut -d' ' -f1 /proc/uptime | cut -d. -f1) ) - started/1000000 ))
        if (( up_sec > 900 )); then
            notify "sensorpushd: no data written since start" \
                   "$STATE_FILE missing after ${up_sec}s of uptime."
            exit 1
        fi
    fi
    exit 0
fi

last_iso=$(<"$STATE_FILE")
last_epoch=$(date -d "$last_iso" +%s 2>/dev/null || echo 0)
if (( last_epoch == 0 )); then
    notify "sensorpushd: unreadable last-success marker" \
           "Could not parse '$last_iso' from $STATE_FILE."
    exit 1
fi

age_min=$(( ( $(date +%s) - last_epoch ) / 60 ))

if (( age_min > MAX_AGE_MIN )); then
    notify "sensorpushd: no data for ${age_min} minutes" \
"Last successful write: $last_iso
Service state:         $active
Threshold:             ${MAX_AGE_MIN} min

Recent log:
$(journalctl -u sensorpushd -n 20 --no-pager 2>/dev/null)"
    exit 1
fi

exit 0
