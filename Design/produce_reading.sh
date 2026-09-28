#!/usr/bin/env bash
set -euo pipefail

reading_id="-$(( $(date +%s) ))"
reading_time="$(date +%s)"
message="{\"ID\":${reading_id},\"time\":${reading_time},\"profile_name\":\"design-demo\",\"temperature\":62,\"humidity\":72,\"pressure\":1.4}"

gcloud pubsub topics publish smartMeterReadings --message="$message" >/dev/null
printf 'Published ID: %s\nMessage: %s\n' "$reading_id" "$message"
