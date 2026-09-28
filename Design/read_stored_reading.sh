#!/usr/bin/env bash
set -euo pipefail
id="${1:?Usage: ./Design/read_stored_reading.sh READING_ID}"
[[ "$id" =~ ^-[0-9]+$ ]] || { echo "Invalid reading ID"; exit 1; }
read -rsp 'MySQL password: ' db_password
echo
MYSQL_PWD="$db_password" mysql -h 34.130.216.53 -P 3306 -u usr \
  -e "SELECT * FROM Readings.SmartMeter WHERE ID = $id;"
