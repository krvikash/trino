ARG JDK_VERSION=19
FROM eclipse-temurin:${JDK_VERSION}-jdk

ARG MODULE_NAME=galaxy-buffer-data-server
ARG PROJECT_NAME=data-server
ARG PROJECT_VERSION
ARG CHOWN="${PROJECT_NAME}:${PROJECT_NAME}"

# Install python
RUN \
    set -xeu && \
    apt-get update -q && \
    apt-get install -q -y python2 && \
    update-alternatives --install /usr/bin/python python /usr/bin/python2 1 && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd ${PROJECT_NAME} --gid 1000 && \
    useradd ${PROJECT_NAME} --uid 1000 --gid 1000

# Add project files
ADD target/${MODULE_NAME}-${PROJECT_VERSION}.tar.gz /tmp/${PROJECT_NAME}
RUN \
    mkdir -p /opt/app && \
    chown -R "${CHOWN}" /opt/app && \
    mv /tmp/${PROJECT_NAME}/${MODULE_NAME}-${PROJECT_VERSION}/* /opt/app && \
    chown ${CHOWN} -R /opt/app && \
    rm -rf /tmp/${PROJECT_NAME} && \
    echo "exec /opt/app/bin/launcher run --config=/opt/app/etc/config.properties" > /start.sh && \
    chown -R "${CHOWN}" /start.sh && \
    chmod +x /start.sh

COPY --chown=${CHOWN} etc /opt/app/etc
VOLUME /opt/app/etc

EXPOSE 8080
USER ${CHOWN}
CMD ["/start.sh"]
