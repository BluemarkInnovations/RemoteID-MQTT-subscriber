#!/bin/sh
# First-time setup of the Nx Witness test server (integrations/vms/docker): a new Nx system
# accepts admin / admin until it is set up. Sets the system name and the admin password.
# For the test server only.
#     integrations/vms/tools/nx_setup.sh [password, default DroneScout-Test-1] [url, default https://127.0.0.1:7001]
set -e
PASSWORD=${1:-DroneScout-Test-1}
URL=${2:-https://127.0.0.1:7001}
TOKEN=$(curl -sk -X POST "$URL/rest/v3/login/sessions" -H 'Content-Type: application/json' \
    -d '{"username":"admin","password":"admin"}' | python3 -c 'import json,sys; print(json.load(sys.stdin).get("token",""))')
if [ -z "$TOKEN" ]; then
    echo "admin / admin doesn't work: the system is already set up (or the server isn't up yet)"
    exit 1
fi
curl -skf -X POST "$URL/rest/v3/system/setup" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "{\"name\":\"dronescout-test\",\"local\":{\"password\":\"$PASSWORD\"}}" > /dev/null
echo "Nx Witness test system set up: $URL, user admin, password $PASSWORD"
