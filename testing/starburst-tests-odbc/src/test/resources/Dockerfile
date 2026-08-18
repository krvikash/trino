# The Starburst ODBC driver is x86_64-only, so this image must be linux/amd64 even on arm64 hosts.
# The digest pins the amd64 manifest of almalinux:9.2. Neither the build API's platform parameter
# nor "FROM --platform" is enough: the legacy builder resolves FROM by tag against the local image
# store, so on an arm64 host that already cached the tag it silently reuses the arm64 image and the
# ODBC rpm fails with "intended for a different architecture". A digest is unambiguous.
# Re-pin with `docker manifest inspect <image>:<tag>` when bumping the tag.
FROM 843985043183.dkr.ecr.us-east-1.amazonaws.com/docker-hub/library/almalinux:9.2@sha256:e78278ef22f8227423532e9694e9ed5c362f3e81067c7986f05f38f2968bb438

RUN yum install -y epel-release
RUN yum --enablerepo crb install -y python3{,-pip,-devel} unixODBC{,-devel} gcc gcc-c++ make

RUN rpm -i https://starburstdata-downloads-v2.s3.us-east-2.amazonaws.com/odbc/v3/2026/3.2.5.284/linux/amd64/starburst-odbc-3.2.5-284.x86_64.rpm

RUN cat /opt/starburstdata/starburst-odbc/etc/odbcinst.ini.template >> /etc/odbcinst.ini

RUN pip3 install pytest==6.2.5 pyodbc==4.0.32 && mkdir -p /work

COPY tests /work/tests/

ENTRYPOINT ["sh", "-c", "cd /work && pytest tests"]
