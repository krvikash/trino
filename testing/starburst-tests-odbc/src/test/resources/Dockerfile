FROM 843985043183.dkr.ecr.us-east-1.amazonaws.com/docker-hub/library/almalinux:9.2

RUN yum install -y epel-release
RUN yum --enablerepo crb install -y python3{,-pip,-devel} unixODBC{,-devel} gcc gcc-c++ make

RUN rpm -i https://starburstdata-downloads.s3.us-east-2.amazonaws.com/odbc/2.0.15.1021/starburstODBC-64bit-2.0.15.1021.el6.x86_64.rpm

RUN cat /opt/starburst/starburstodbc/Setup/odbcinst.ini >> /etc/odbcinst.ini

RUN pip3 install pytest==6.2.5 pyodbc==4.0.32 && mkdir -p /work

COPY tests /work/tests/

ENTRYPOINT ["sh", "-c", "cd /work && pytest tests"]
