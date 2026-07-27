#!/bin/sh
# Regenerate the test-only TLS material used by QwpAckServer(tls=True):
# a throwaway CA and a CA-signed server certificate for 127.0.0.1 /
# localhost. rustls requires the pinned trust anchor to be a real CA
# (basicConstraints CA:true), so a bare self-signed leaf does not work.
set -eu
cd "$(dirname "$0")"

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout ca.key -out ca.crt -days 36500 -nodes \
    -subj '/CN=py-questdb-client-test-ca' \
    -addext 'basicConstraints=critical,CA:true' \
    -addext 'keyUsage=critical,keyCertSign'

openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout server.key -out server.csr -nodes \
    -subj '/CN=py-questdb-client-test'

cat > ext.cnf <<'EOF'
subjectAltName=IP:127.0.0.1,DNS:localhost
basicConstraints=CA:false
keyUsage=digitalSignature
extendedKeyUsage=serverAuth
EOF

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out server.crt -days 36500 -extfile ext.cnf

rm -f server.csr ext.cnf ca.srl ca.key
openssl verify -CAfile ca.crt server.crt
