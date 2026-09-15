#!/usr/bin/env bash
# Regenerate the TLS test fixtures of tests/tls.rs.
#
# The script needs openssl and a JDK keytool. The certificates are valid for
# 100 years, so the fixtures do not expire during the life of the tests.
# Run it from this directory: ./generate.sh
set -euo pipefail
cd "$(dirname "$0")"
rm -f -- *.pem *.key *.p12 *.jks *.srl *.csr *.ext

days=36500

# Certificate authority that the tests trust.
openssl req -x509 -newkey rsa:2048 -nodes -days "$days" -subj "/CN=krabka test CA" \
  -keyout ca.key -out ca.pem
# A second authority that the tests do not trust.
openssl req -x509 -newkey rsa:2048 -nodes -days "$days" -subj "/CN=krabka other CA" \
  -keyout other-ca.key -out other-ca.pem

sign() { # name subject san extended-key-usage
  openssl req -newkey rsa:2048 -nodes -subj "$2" -keyout "$1.key" -out "$1.csr"
  printf 'subjectAltName=%s\nextendedKeyUsage=%s\n' "$3" "$4" > "$1.ext"
  openssl x509 -req -in "$1.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days "$days" \
    -extfile "$1.ext" -out "$1.pem"
}

# Broker certificate for localhost and 127.0.0.1.
sign server "/CN=localhost" "DNS:localhost,IP:127.0.0.1" serverAuth
# Broker certificate whose names do not match localhost.
sign server-wrong-name "/CN=broker.example" "DNS:broker.example" serverAuth
# Client certificate for mutual TLS.
sign client "/CN=krabka-client" "DNS:krabka-client" clientAuth

# PKCS#8 keys: openssl writes "PRIVATE KEY" already. Encrypt the client key
# with PBES2 (PBKDF2-HMAC-SHA256, AES-256-CBC), as Kafka's ssl.key.password
# expects.
openssl pkcs8 -topk8 -v2 aes-256-cbc -v2prf hmacWithSHA256 -passout pass:key-secret \
  -in client.key -out client-encrypted.key
# One PEM key store file with the encrypted key and the certificate chain, as
# Kafka's ssl.keystore.type=PEM with ssl.keystore.location reads it.
cat client-encrypted.key client.pem ca.pem > client-keystore.pem

# PKCS#12 key store written by keytool.
openssl pkcs12 -export -name client -passout pass:store-secret \
  -inkey client.key -in client.pem -certfile ca.pem -out client-openssl.p12
keytool -importkeystore -noprompt \
  -srckeystore client-openssl.p12 -srcstoretype PKCS12 -srcstorepass store-secret \
  -destkeystore client.p12 -deststoretype PKCS12 -deststorepass store-secret
# JKS key store written by keytool, with a key password that differs from the
# store password.
keytool -importkeystore -noprompt \
  -srckeystore client-openssl.p12 -srcstoretype PKCS12 -srcstorepass store-secret \
  -destkeystore client.jks -deststoretype JKS -deststorepass store-secret \
  -destkeypass key-secret -srcalias client -destalias client
rm -f client-openssl.p12

# Trust stores written by keytool.
keytool -importcert -noprompt -alias ca -file ca.pem \
  -keystore truststore.p12 -storetype PKCS12 -storepass trust-secret
keytool -importcert -noprompt -alias ca -file ca.pem \
  -keystore truststore.jks -storetype JKS -storepass trust-secret

rm -f -- *.srl *.csr *.ext ca.key other-ca.key
