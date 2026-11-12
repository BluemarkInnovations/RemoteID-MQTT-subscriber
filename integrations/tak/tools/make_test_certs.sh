#!/bin/sh
# Test certificates for tak_gateway.py and tools/mock_tak_server.py, like a TAK Server setup:
# own CA, a server certificate for localhost and a client certificate "dronescout",
# also as .p12 (as TAK Server hands them out) to try the conversion in tak_config.py.
# For testing only. Usage: integrations/tak/tools/make_test_certs.sh [output folder, default ./test_certs]
set -e
OUT=${1:-./test_certs}
mkdir -p "$OUT"
cd "$OUT"

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=DroneScout test CA" \
    -keyout ca.key -out ca.pem 2>/dev/null

openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout server.key -out server.csr 2>/dev/null
printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n" > server.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 3650 \
    -extfile server.ext -out server.pem 2>/dev/null

openssl req -newkey rsa:2048 -nodes -subj "/CN=dronescout" -keyout dronescout.key -out dronescout.csr 2>/dev/null
printf "extendedKeyUsage=clientAuth\n" > client.ext
openssl x509 -req -in dronescout.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 3650 \
    -extfile client.ext -out dronescout.pem 2>/dev/null

openssl pkcs12 -export -in dronescout.pem -inkey dronescout.key -certfile ca.pem \
    -passout pass:atakatak -out dronescout.p12 2>/dev/null

rm -f server.csr dronescout.csr server.ext client.ext ca.srl
echo "test certificates in $OUT: ca.pem, server.pem/.key, dronescout.pem/.key/.p12 (password atakatak)"
