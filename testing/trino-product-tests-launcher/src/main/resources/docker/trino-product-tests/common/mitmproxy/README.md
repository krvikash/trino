# Setup instructions 


Spin up a mitmproxy container

```
docker run mitmproxy/mitmproxy
```

Retrieve the files `mitmproxy-ca.pem` and `mitmproxy-ca-cert.pem` through the following commands 

```
docker cp mitmproxy:/home/mitmproxy/.mitmproxy/mitmproxy-ca.pem ./mitmproxy-ca.pem
docker cp mitmproxy:/home/mitmproxy/.mitmproxy/mitmproxy-ca-cert.pem ./mitmproxy-ca-cert.pem
docker cp mitmproxy:/home/mitmproxy/.mitmproxy/mitmproxy-dhparam.pem ./mitmproxy-dhparam.pem
```

Create a PKCS12 Bundle

```
openssl pkcs12 -export \
    -in mitmproxy-ca.pem \                                                            
    -inkey mitmproxy-ca.pem \
    -out mitmproxy.p12 \
    -name "mitmproxy" \
    -passout pass:changeit
```

Convert PKCS12 to JKS

```
keytool -importkeystore \
    -srckeystore mitmproxy.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass changeit \
    -destkeystore mitmproxy.jks \
    -deststoretype JKS \
    -deststorepass changeit \
    -noprompt
```

The newly obtained truststore can be used in the JVM with the following system properties
for using the mitmproxy certificate for SSL connections:

```
-Djavax.net.ssl.trustStore=/path/to/mitmproxy.jks
-Djavax.net.ssl.trustStorePassword=changeit
```
