#!/bin/sh

# Store password for Trino server Keystore
keytool -importpass -storetype pkcs12 -alias keystore_password \
-keystore /docker/trino-product-tests/conf/trino/etc/credential.jckes \
-storepass password<<EOF
123456
123456
EOF

# Store shared secret for Trino server
keytool -importpass -storetype pkcs12 -alias shared_secret \
-keystore /docker/trino-product-tests/conf/trino/etc/credential.jckes \
-storepass password<<EOF
shared_secret
shared_secret
EOF

# Store password.db file for Trino server
keytool -importpass -storetype pkcs12 -alias password_db_file \
-keystore /docker/trino-product-tests/conf/trino/etc/credential.jckes \
-storepass password<<EOF
etc/password.db
etc/password.db
EOF

# Store JDBC URL for Postgres
keytool -importpass -storetype pkcs12 -alias postgres_jdbc_url \
-keystore /docker/trino-product-tests/conf/trino/etc/credential.jckes \
-storepass password<<EOF
jdbc:postgresql://postgresql:15432/test
jdbc:postgresql://postgresql:15432/test
EOF

# Store username for Postgres
keytool -importpass -storetype pkcs12 -alias postgres_username \
-keystore /docker/trino-product-tests/conf/trino/etc/credential.jckes \
-storepass password<<EOF
test
test
EOF

# Store password for Postgres
keytool -importpass -storetype pkcs12 -alias postgres_password \
-keystore /docker/trino-product-tests/conf/trino/etc/credential.jckes \
-storepass password<<EOF
test
test
EOF
