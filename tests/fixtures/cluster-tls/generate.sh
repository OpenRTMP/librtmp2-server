#!/bin/sh
# Regenerate the cluster mTLS test fixtures (valid for 100 years).
set -eu
D="$1"
cd "$D"
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out ca.key
openssl req -x509 -new -key ca.key -sha256 -days 36500 -subj "/CN=lrtmp2 test cluster CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" -out ca.pem
for n in 1 2; do
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out node$n.key
  openssl req -new -key node$n.key -subj "/CN=lrtmp2-node-$n" -out node$n.csr
  cat > ext$n.cnf <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:lrtmp2-node-$n,IP:127.0.0.1
EOF
  openssl x509 -req -in node$n.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 36500 -sha256 \
    -extfile ext$n.cnf -out node$n.pem
  rm -f node$n.csr ext$n.cnf
done
rm -f ca.key ca.srl
